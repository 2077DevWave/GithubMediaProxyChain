#!/usr/bin/env python3
"""
Headless crawler + media scraper → RAR multi‑volume archives (90 MB parts).
Requires `rar` command available (installed in workflow).
"""

import os, re, sys, subprocess, requests
from urllib.parse import urljoin, urlparse, urldefrag
from playwright.sync_api import sync_playwright
from tqdm import tqdm

# ----------------------------------------------------------------------
# ENV configuration
URLS_ENV = os.environ.get("TARGET_URLS", "")
TARGET_URLS = [u.strip() for u in URLS_ENV.split(",") if u.strip()]
CRAWL_DEPTH = int(os.environ.get("CRAWL_DEPTH", "0"))
URL_PATTERN = os.environ.get("URL_PATTERN", "").strip()
SAME_DOMAIN = os.environ.get("SAME_DOMAIN", "true").lower() == "true"

MAX_PAGES = int(os.environ.get("MAX_PAGES", "0") or "0")
MAX_MEDIA_FILES = int(os.environ.get("MAX_MEDIA_FILES", "0") or "0")
MAX_DOWNLOAD_BYTES = int(float(os.environ.get("MAX_DOWNLOAD_MB", "0") or "0") * 1024 * 1024)

OUTPUT_DIR = "media"          # temp folder for files
NAV_TIMEOUT = 60000
SCROLL_STEPS = 3
SCROLL_DELAY = 1500

RAR_VOLUME_SIZE_MB = 90       # volume size in MB

# Colors
G = "\033[92m"
Y = "\033[93m"
R = "\033[91m"
C = "\033[96m"
M = "\033[95m"
Z = "\033[0m"

def human(n):
    if n==0: return "0 B"
    for u in ["B","KB","MB","GB"]:
        if n<1024: return f"{n:.1f} {u}"
        n/=1024

def step(msg):    print(f"{C}▶ {msg}{Z}"); sys.stdout.flush()
def ok(msg):      print(f"{G}✓ {msg}{Z}"); sys.stdout.flush()
def warn(msg):    print(f"{Y}⚠ {msg}{Z}"); sys.stdout.flush()
def err(msg):     print(f"{R}✗ {msg}{Z}"); sys.stdout.flush()
def info(msg):    print(f"{M}  → {msg}{Z}"); sys.stdout.flush()

# ----------------------------------------------------------------------
def extract_media(page):
    raw = page.evaluate("""()=>{
        const u=[]; 
        document.querySelectorAll('video').forEach(v=>{if(v.src)u.push(v.src);v.querySelectorAll('source').forEach(s=>{if(s.src)u.push(s.src)})});
        document.querySelectorAll('img').forEach(i=>{const s=i.currentSrc||i.src;if(s)u.push(s)});
        document.querySelectorAll('picture source').forEach(s=>{if(s.srcset){const f=s.srcset.split(',')[0].trim().split(' ')[0];if(f)u.push(f)}if(s.src)u.push(s.src)});
        return [...new Set(u)];
    }""")
    base = page.url
    return [urljoin(base, x) for x in raw if not x.startswith("data:")]

def extract_links(page, seed_domain):
    raw = page.evaluate("""()=>{
        const l=[];document.querySelectorAll('a[href]').forEach(a=>{
            const h=a.getAttribute('href');if(h&&!h.startsWith('javascript:')&&!h.startsWith('mailto:')&&!h.startsWith('#'))l.push(h)
        });return [...new Set(l)];
    }""")
    base = page.url
    absolute = []
    for href in raw:
        full, _ = urldefrag(urljoin(base, href))
        if not full.startswith(("http://","https://")): continue
        if SAME_DOMAIN and urlparse(full).netloc != seed_domain: continue
        absolute.append(full)
    return absolute

def download(session, url, folder):
    try:
        r = session.get(url, timeout=30, stream=True)
        r.raise_for_status()
        cd = r.headers.get("content-disposition","")
        if "filename=" in cd: fname = re.findall("filename=(.+)",cd)[0].strip('" ')
        else:
            parsed = urlparse(url); fname = os.path.basename(parsed.path) or "index"
        safe = re.sub(r'[\\/*?:"<>|]',"_",fname)
        os.makedirs(folder, exist_ok=True)
        path = os.path.join(folder, safe)
        with open(path,"wb") as f:
            for c in r.iter_content(8192): f.write(c)
        return (path, os.path.getsize(path))
    except:
        return (None,0)

def create_rar_split(media_dir, index):
    """
    Compress all files inside media_dir into a RAR multi‑volume archive with
    90 MB parts. Returns list of generated .rar file names.
    """
    # Collect all files
    file_list = []
    for root, _, filenames in os.walk(media_dir):
        for f in filenames:
            file_list.append(os.path.join(root, f))
    if not file_list:
        return []

    # Build RAR command
    base_name = str(index)            # output root name (e.g., "1")
    # The RAR volumes will be: 1.part1.rar, 1.part2.rar, ... (with -v90m -scfg)
    # Use -ep to not store the full path, -m0 for store (fast, no compression)
    cmd = [
        "rar", "a",
        f"-v{RAR_VOLUME_SIZE_MB}m",   # volume size in MB
        "-m0",                        # no compression (fast)
        "-ep",                        # store bare filenames
        "-r",                         # recurse
        "-inul",                      # no verbose output (we'll capture)
        f"{base_name}",               # archive name (without extension)
    ] + file_list

    step(f"Creating RAR volumes (≤90 MB) for seed {index} ...")
    try:
        subprocess.run(cmd, check=True, cwd=os.getcwd())
    except subprocess.CalledProcessError as e:
        err(f"RAR command failed: {e}")
        return []

    # Find all generated .rar files matching the pattern
    rar_files = []
    for f in os.listdir("."):
        if f.startswith(f"{base_name}.") and f.endswith(".rar"):
            rar_files.append(f)
    rar_files.sort()
    for rf in rar_files:
        size = os.path.getsize(rf)
        ok(f"  {rf} ({human(size)})")
    return rar_files

def crawl_seed(seed_url, browser, index):
    seed_domain = urlparse(seed_url).netloc
    media_dir = os.path.join(OUTPUT_DIR, str(index))
    os.makedirs(media_dir, exist_ok=True)

    session = requests.Session()
    session.headers.update({
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
    })

    visited = set()
    queue = [(seed_url, 0)]
    total_dl = 0
    total_bytes = 0
    page_cnt = 0

    pattern = None
    if URL_PATTERN:
        try:
            pattern = re.compile(URL_PATTERN)
            info(f"Link pattern: {URL_PATTERN}")
        except re.error as e:
            err(f"Bad pattern: {e}")

    step(f"Crawling {seed_url} (depth={CRAWL_DEPTH}, same_domain={SAME_DOMAIN})")
    if MAX_PAGES: info(f"Max pages: {MAX_PAGES}")
    if MAX_MEDIA_FILES: info(f"Max media files: {MAX_MEDIA_FILES}")
    if MAX_DOWNLOAD_BYTES: info(f"Max download size: {human(MAX_DOWNLOAD_BYTES)}")

    limit_hit = False

    while queue and not limit_hit:
        url, depth = queue.pop(0)
        if url in visited: continue
        visited.add(url)
        page_cnt += 1

        print(f"\n{C}══ Page {page_cnt} (depth {depth}) ══{Z}")
        info(f"Visiting: {url}")

        context = None
        try:
            context = browser.new_context(
                viewport={"width":1920,"height":1080},
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
            )
            page = context.new_page()
            try:
                page.goto(url, wait_until="domcontentloaded", timeout=NAV_TIMEOUT)
            except Exception as e:
                warn(f"  load warning: {e}, fallback to 'load'")
                page.goto(url, wait_until="load", timeout=NAV_TIMEOUT)

            page.wait_for_timeout(3000)
            for _ in range(SCROLL_STEPS):
                page.evaluate("window.scrollBy(0, window.innerHeight)")
                page.wait_for_timeout(SCROLL_DELAY)
            page.wait_for_timeout(2000)

            # Media
            media_urls = extract_media(page)
            info(f"Found {len(media_urls)} media items")
            if media_urls:
                dl_count = 0
                dl_bytes = 0
                for mu in tqdm(media_urls, desc="  Media", unit="file", bar_format="{l_bar}{bar}| {n_fmt}/{total_fmt}"):
                    if MAX_MEDIA_FILES and total_dl >= MAX_MEDIA_FILES:
                        warn(f"Reached max media files ({MAX_MEDIA_FILES}).")
                        limit_hit = True
                        break
                    if MAX_DOWNLOAD_BYTES and total_bytes >= MAX_DOWNLOAD_BYTES:
                        warn(f"Reached max download size ({human(MAX_DOWNLOAD_BYTES)}).")
                        limit_hit = True
                        break

                    path, sz = download(session, mu, media_dir)
                    if path:
                        dl_count += 1; dl_bytes += sz
                        total_dl += 1; total_bytes += sz
                        info(f"  ✓ {os.path.basename(path)} ({human(sz)}) ← {mu}")
                    else:
                        err(f"  ✗ {mu}")
                if not limit_hit:
                    ok(f"  Downloaded {dl_count} files ({human(dl_bytes)})")
            else:
                warn("  No media on this page.")

            # Check limits after page
            if MAX_PAGES and page_cnt >= MAX_PAGES:
                warn(f"Reached max pages ({MAX_PAGES}).")
                limit_hit = True
                break
            if MAX_MEDIA_FILES and total_dl >= MAX_MEDIA_FILES:
                warn(f"Reached max media files ({MAX_MEDIA_FILES}).")
                limit_hit = True
                break
            if MAX_DOWNLOAD_BYTES and total_bytes >= MAX_DOWNLOAD_BYTES:
                warn(f"Reached max download size ({human(MAX_DOWNLOAD_BYTES)}).")
                limit_hit = True
                break

            # Links
            if depth < CRAWL_DEPTH and not limit_hit:
                links = extract_links(page, seed_domain)
                filtered = []
                for l in links:
                    if l in visited or any(l == q[0] for q in queue): continue
                    if pattern and not pattern.search(l): continue
                    filtered.append(l)
                info(f"Found {len(links)} links, {len(filtered)} new after filtering")
                for l in filtered:
                    queue.append((l, depth+1))
        except Exception as e:
            err(f"  Page error: {e}")
        finally:
            if context: context.close()

    if total_dl == 0:
        warn("No media downloaded, no RAR created.")
        return []

    # Create RAR volumes
    rar_files = create_rar_split(media_dir, index)
    return rar_files

def main():
    if not TARGET_URLS:
        err("No seed URLs.")
        return
    step(f"Processing {len(TARGET_URLS)} seed(s) with crawl depth {CRAWL_DEPTH}")
    print(f"Seeds: {', '.join(TARGET_URLS)}")

    if os.path.exists(OUTPUT_DIR):
        import shutil; shutil.rmtree(OUTPUT_DIR, ignore_errors=True)

    all_rars = []
    with sync_playwright() as p:
        step("Launching Chromium...")
        browser = p.chromium.launch(headless=True)
        ok("Browser ready")

        for i, seed in enumerate(tqdm(TARGET_URLS, desc="Seeds"), start=1):
            rar_list = crawl_seed(seed, browser, i)
            all_rars.extend(rar_list)

        browser.close()
        ok("Browser closed")

    import shutil; shutil.rmtree(OUTPUT_DIR, ignore_errors=True)

    print(f"\n{C}══ Summary ══{Z}")
    if all_rars:
        for f in all_rars:
            print(f"  {G}{f}{Z}  ({human(os.path.getsize(f))})")
        total = sum(os.path.getsize(f) for f in all_rars)
        print(f"  Total: {human(total)}")
    else:
        warn("No RAR files generated.")

if __name__=="__main__":
    main()
