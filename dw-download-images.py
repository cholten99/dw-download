#!/usr/bin/env python3
"""
Download all unique embedded images referenced in Dreamwidth exports.

Features:
- Reads URLs from image_urls.txt
- One download per unique URL
- Hash-based filenames
- Skip-if-exists
- Polite random pauses
- Back-off on failure
- Failure log

This script ONLY downloads images.
It does NOT rewrite XML or touch WordPress.
"""

from __future__ import annotations

import hashlib
import logging
import random
import sys
import time
from pathlib import Path
from typing import Optional
from urllib.parse import urlparse

import requests

LOG = logging.getLogger("dw_images")


IMAGE_DIR = Path("dw_images")
FAIL_LOG = Path("image_failures.csv")


def configure_logging():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )


def polite_pause(min_s: int, max_s: int, reason: str):
    delay = random.randint(min_s, max_s)
    LOG.info("Pausing %ds (%s)", delay, reason)
    time.sleep(delay)


def url_to_path(url: str) -> Path:
    """
    Deterministically map a URL to a local file path.
    """
    h = hashlib.sha256(url.encode("utf-8")).hexdigest()
    subdir = h[:2]

    parsed = urlparse(url)
    suffix = Path(parsed.path).suffix
    if not suffix or len(suffix) > 5:
        suffix = ".bin"

    return IMAGE_DIR / subdir / f"{h}{suffix}"


def record_failure(url: str, reason: str):
    ts = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    with FAIL_LOG.open("a", encoding="utf-8") as f:
        f.write(f"{ts},{url},{reason}\n")


def download_image(url: str, dest: Path) -> bool:
    dest.parent.mkdir(parents=True, exist_ok=True)

    headers = {
        "User-Agent": "dw-image-archiver/1.0",
        "Accept": "*/*",
    }

    try:
        r = requests.get(url, headers=headers, timeout=30, stream=True)
    except Exception as e:
        record_failure(url, f"request_error:{e}")
        return False

    if r.status_code != 200:
        record_failure(url, f"http_{r.status_code}")
        return False

    with dest.open("wb") as f:
        for chunk in r.iter_content(chunk_size=8192):
            if chunk:
                f.write(chunk)

    return True


def main() -> int:
    configure_logging()

    if not Path("image_urls.txt").exists():
        LOG.error("image_urls.txt not found")
        return 2

    urls = [line.strip() for line in Path("image_urls.txt").read_text().splitlines() if line.strip()]

    LOG.info("Starting image download: %d URLs", len(urls))

    success = 0
    failures = 0

    for idx, url in enumerate(urls, start=1):
        dest = url_to_path(url)

        if dest.exists():
            LOG.info("[%d/%d] Skip (exists): %s", idx, len(urls), url)
            continue

        LOG.info("[%d/%d] Downloading: %s", idx, len(urls), url)

        ok = download_image(url, dest)

        if ok:
            success += 1
            polite_pause(1, 3, "normal")
        else:
            failures += 1
            polite_pause(10, 20, "failure")

    LOG.info("Done. Success=%d Failures=%d", success, failures)
    return 0


if __name__ == "__main__":
    sys.exit(main())

