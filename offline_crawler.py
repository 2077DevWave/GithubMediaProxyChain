#!/usr/bin/env python3
"""
offline_crawler.py - Download a full webpage (HTML, CSS, JS, images, fonts),
rewrite URLs for offline use, create a ZIP archive, and push the ZIP to a branch.
"""

import os
import re
import sys
import zipfile
import hashlib
import subprocess
import shutil
from urllib.parse import urljoin, urlparse
from pathlib import Path

import requests
from bs4 import BeautifulSoup

# ========== CONFIGURATION ==========
TARGET_URL = os.environ.get("TARGET_URL", "https://example.com")
OUTPUT_DIR = Path(os.environ.get("OUTPUT_DIR", "./offline_site"))
ZIP_FILENAME = os.environ.get("ZIP_FILENAME", "offline_site.zip")
BRANCH_NAME = os.environ.get("BRANCH_NAME", "offline-zip")
GITHUB_REPO = os.environ.get("GITHUB_REPOSITORY")
GITHUB_TOKEN = os.environ.get("GITHUB_TOKEN")

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
}

# ========== HELPER FUNCTIONS ==========
def get_local_path(url, base_output_dir):
    """Convert URL to local file path mirroring the URL structure."""
    parsed = urlparse(url)
    path = parsed.path.lstrip('/')
    if not path:
        path = "index.html"
    local_path = base_output_dir / path
    if parsed.path.endswith('/'):
        local_path = local_path / "index.html"
    return local_path

def download_file(url, output_dir):
    """Download a file and save it preserving URL structure."""
    local_path = get_local_path(url, output_dir)
    local_path.parent.mkdir(parents=True, exist_ok=True)
    try:
        resp = requests.get(url, headers=HEADERS, timeout=15)
        resp.raise_for_status()
    except Exception as e:
        print(f"⚠️ Failed to download {url}: {e}")
        return None

    if not local_path.suffix:
        content_type = resp.headers.get("content-type", "")
        ext_map = {
            "text/html": ".html",
            "text/css": ".css",
            "application/javascript": ".js",
            "image/jpeg": ".jpg",
            "image/png": ".png",
            "image/gif": ".gif",
            "image/webp": ".webp",
            "image/svg+xml": ".svg",
            "font/woff": ".woff",
            "font/woff2": ".woff2",
        }
        for ctype, ext in ext_map.items():
            if ctype in content_type:
                local_path = local_path.with_suffix(ext)
                break

    with open(local_path, "wb") as f:
        f.write(resp.content)
    print(f"✓ Downloaded: {url} -> {local_path}")
    return local_path

def rewrite_html_links(html_content, base_url, asset_map, output_dir):
    """Rewrite src/href/srcset in HTML to point to local files."""
    soup = BeautifulSoup(html_content, "html.parser")
    tag_attrs = {
        "link": ["href"],
        "script": ["src"],
        "img": ["src", "srcset"],
        "a": ["href"],
        "source": ["src", "srcset"],
        "video": ["src", "poster"],
        "audio": ["src"],
        "embed": ["src"],
        "iframe": ["src"],
    }
    for tag_name, attrs in tag_attrs.items():
        for tag in soup.find_all(tag_name):
            for attr in attrs:
                if tag.has_attr(attr):
                    url = tag[attr]
                    full_url = urljoin(base_url, url)
                    if full_url in asset_map:
                        tag[attr] = asset_map[full_url]
    return str(soup)

def rewrite_css_urls(css_content, css_url, asset_map):
    """Rewrite url(...) and @import inside CSS."""
    def replacer(match):
        url = match.group(1).strip("'\"")
        full_url = urljoin(css_url, url)
        if full_url in asset_map:
            target_rel = asset_map[full_url]
            css_dir = Path(css_url).parent
            rel_path = Path(target_rel).relative_to(css_dir, walk_up=True)
            return f"url('{rel_path}')"
        return match.group(0)

    new_content = re.sub(r"url\(([^)]+)\)", replacer, css_content)
    new_content = re.sub(
        r"@import\s+[\"']([^\"']+)[\"']",
        lambda m: f"@import '{asset_map.get(urljoin(css_url, m.group(1)), m.group(1))}'",
        new_content,
    )
    return new_content

def fetch_and_save_page(url, output_dir):
    """Main routine to download and save offline site."""
    print(f"🌐 Fetching main page: {url}")
    resp = requests.get(url, headers=HEADERS, timeout=20)
    resp.raise_for_status()
    html_content = resp.text
    base_url = url

    soup = BeautifulSoup(html_content, "html.parser")
    assets_to_download = set()

    for link in soup.find_all("link", rel="stylesheet"):
        if link.get("href"):
            assets_to_download.add(urljoin(base_url, link["href"]))
    for script in soup.find_all("script", src=True):
        assets_to_download.add(urljoin(base_url, script["src"]))
    for img in soup.find_all("img", src=True):
        assets_to_download.add(urljoin(base_url, img["src"]))
    for source in soup.find_all(["source", "video", "audio"]):
        for attr in ["src", "poster"]:
            if source.get(attr):
                assets_to_download.add(urljoin(base_url, source[attr]))

    asset_map = {}
    output_dir.mkdir(parents=True, exist_ok=True)

    for asset_url in assets_to_download:
        local_path = download_file(asset_url, output_dir)
        if local_path:
            rel_path = local_path.relative_to(output_dir)
            asset_map[asset_url] = str(rel_path)

    # Download assets referenced inside CSS
    for css_url, rel_path in list(asset_map.items()):
        if not rel_path.endswith(".css"):
            continue
        css_abs_path = output_dir / rel_path
        with open(css_abs_path, "r", encoding="utf-8") as f:
            css_content = f.read()
        url_pattern = re.compile(r"url\(['\"]?([^'\")]+)['\"]?\)")
        found_urls = url_pattern.findall(css_content)
        for found in found_urls:
            full_url = urljoin(css_url, found)
            # ---------- ADD THIS CHECK ----------
            if full_url.startswith("data:") or not full_url.startswith(("http://", "https://")):
                # skip data URIs and non-http URLs (like mailto:, javascript: etc.)
                continue
            # ------------------------------------
            if full_url not in asset_map:
                local_path = download_file(full_url, output_dir)
                if local_path:
                    rel_path2 = local_path.relative_to(output_dir)
                    asset_map[full_url] = str(rel_path2)

    # Rewrite CSS files
    for css_url, rel_path in asset_map.items():
        try:
            if not rel_path.endswith(".css"):
                continue
            css_abs_path = output_dir / rel_path
            with open(css_abs_path, "r", encoding="utf-8") as f:
                css_content = f.read()
            new_css = rewrite_css_urls(css_content, css_url, asset_map)
            with open(css_abs_path, "w", encoding="utf-8") as f:
                f.write(new_css)
        except Exception as e:
            pass

    # Rewrite HTML and save index.html
    try:
        new_html = rewrite_html_links(html_content, base_url, asset_map, output_dir)
        index_path = output_dir / "index.html"
        with open(index_path, "w", encoding="utf-8") as f:
            f.write(new_html)
    except Exception as e:
        pass

    print(f"✅ Offline site ready at {output_dir}")
    return output_dir

def create_zip_archive(source_dir, zip_path):
    """Create a ZIP archive of the offline site."""
    zip_path = Path(zip_path)
    if zip_path.exists():
        zip_path.unlink()
    print(f"📦 Creating ZIP archive: {zip_path}")
    with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zf:
        for file_path in source_dir.rglob("*"):
            if file_path.is_file():
                arcname = file_path.relative_to(source_dir.parent)
                zf.write(file_path, arcname)
    size_mb = zip_path.stat().st_size / (1024 * 1024)
    print(f"✅ ZIP created: {zip_path} ({size_mb:.2f} MB)")
    if size_mb > 100:
        print("⚠️  ZIP exceeds 100 MB – GitHub may reject the push. Consider splitting or using Git LFS.")
    return zip_path

def push_zip_to_branch(zip_path, branch_name):
    """Commit the ZIP file to a dedicated branch and push (fixed: no self-copy)."""
    if not GITHUB_REPO or not GITHUB_TOKEN:
        print("❌ GITHUB_REPO or GITHUB_TOKEN not set. Skipping branch push.")
        return False

    subprocess.run(["git", "config", "user.name", "github-actions[bot]"], check=False)
    subprocess.run(
        ["git", "config", "user.email", "github-actions[bot]@users.noreply.github.com"],
        check=False,
    )

    subprocess.run(["git", "fetch", "origin"], check=False)

    # Switch to the target branch (create if needed)
    result = subprocess.run(
        ["git", "rev-parse", "--verify", branch_name], capture_output=True
    )
    if result.returncode != 0:
        subprocess.run(["git", "checkout", "--orphan", branch_name], check=True)
        subprocess.run(["git", "rm", "-rf", "."], check=True)
    else:
        subprocess.run(["git", "checkout", branch_name], check=True)

    # Ensure the ZIP file exists in the working directory
    zip_filename = Path(zip_path).name
    if not Path(zip_filename).exists():
        # If missing (e.g., after git clean), copy from original location
        shutil.copy2(zip_path, zip_filename)
        print(f"📄 Copied {zip_path} to {zip_filename}")

    # Stage the ZIP file
    subprocess.run(["git", "add", zip_filename], check=True)

    # Commit only if there are changes
    status = subprocess.run(["git", "status", "--porcelain"], capture_output=True, text=True)
    if status.stdout.strip():
        commit_msg = f"Offline ZIP of {TARGET_URL} - {__import__('datetime').datetime.now()}"
        subprocess.run(["git", "commit", "-m", commit_msg], check=True)
        print("✅ Committed new ZIP file.")
    else:
        print("ℹ️ No changes to commit (ZIP already up to date).")

    # Push (force to overwrite previous ZIP)
    remote_url = f"https://x-access-token:{GITHUB_TOKEN}@github.com/{GITHUB_REPO}.git"
    subprocess.run(["git", "push", remote_url, branch_name, "--force"], check=True)
    print(f"🚀 Pushed ZIP to branch '{branch_name}'")
    return True

if __name__ == "__main__":
    if len(sys.argv) > 1:
        TARGET_URL = sys.argv[1]
    if len(sys.argv) > 2:
        OUTPUT_DIR = Path(sys.argv[2])

    print(f"Target URL: {TARGET_URL}")
    print(f"Output dir: {OUTPUT_DIR}")
    print(f"ZIP file:   {ZIP_FILENAME}")
    print(f"Branch:     {BRANCH_NAME}")

    output_path = fetch_and_save_page(TARGET_URL, OUTPUT_DIR)
    zip_file = create_zip_archive(output_path, ZIP_FILENAME)
    push_zip_to_branch(zip_file, BRANCH_NAME)

    print("🎉 Done! The ZIP file is now in the repository branch.")
