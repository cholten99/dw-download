#!/usr/bin/env python3
from __future__ import annotations

import argparse
import logging
import random
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup  # pip install beautifulsoup4


LOG = logging.getLogger("dw_export")


@dataclass
class RateLimitConfig:
    min_delay_s: float = 0.8
    max_delay_s: float = 2.0
    max_retries: int = 6
    backoff_base_s: float = 1.6


def configure_logging(level: str) -> None:
    numeric = getattr(logging, level.upper(), None)
    if not isinstance(numeric, int):
        raise ValueError(f"Invalid log level: {level}")
    logging.basicConfig(
        level=numeric,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )


def jitter_sleep(cfg: RateLimitConfig) -> None:
    time.sleep(random.uniform(cfg.min_delay_s, cfg.max_delay_s))


def parse_cookie_header(header: str) -> Dict[str, str]:
    cookies: Dict[str, str] = {}
    for part in header.split(";"):
        part = part.strip()
        if not part or "=" not in part:
            continue
        k, v = part.split("=", 1)
        cookies[k.strip()] = v.strip()
    return cookies


def load_netscape_cookie_file(path: Path, domain="dreamwidth.org") -> Dict[str, str]:
    if not path.exists():
        raise FileNotFoundError(f"Cookie file not found: {path}")
    cookies: Dict[str, str] = {}
    for line in path.read_text(errors="ignore").splitlines():
        if not line or line.startswith("#"):
            continue
        parts = line.split("\t")
        if len(parts) >= 7 and domain in parts[0]:
            cookies[parts[5]] = parts[6]
    if not cookies:
        raise RuntimeError(f"No cookies for domain containing '{domain}' found in {path}")
    return cookies


def request_with_retries(
    session: requests.Session,
    method: str,
    url: str,
    cfg: RateLimitConfig,
    **kwargs,
) -> requests.Response:
    for attempt in range(1, cfg.max_retries + 1):
        try:
            r = session.request(method, url, timeout=30, allow_redirects=True, **kwargs)
            if r.status_code in (429, 500, 502, 503, 504):
                delay = (cfg.backoff_base_s ** attempt) + random.uniform(0, 0.4)
                LOG.warning("HTTP %s for %s %s (attempt %d/%d), backing off %.1fs",
                            r.status_code, method, url, attempt, cfg.max_retries, delay)
                time.sleep(delay)
                continue
            return r
        except requests.RequestException as e:
            delay = (cfg.backoff_base_s ** attempt) + random.uniform(0, 0.4)
            LOG.warning("Request failed (%s) for %s %s (attempt %d/%d), backing off %.1fs",
                        e, method, url, attempt, cfg.max_retries, delay)
            time.sleep(delay)
    raise RuntimeError(f"Failed after {cfg.max_retries} attempts: {method} {url}")


def choose_export_form(html: str):
    soup = BeautifulSoup(html, "html.parser")
    forms = soup.find_all("form")
    if not forms:
        raise RuntimeError("No <form> found on /export page. Are you logged in?")

    for f in forms:
        if f.find("select", {"name": "year"}) or f.find("select", {"name": "month"}):
            return f
        if "export" in (f.get_text(" ", strip=True) or "").lower():
            return f

    return forms[0]


def build_form_payload(form) -> Dict[str, str]:
    data: Dict[str, str] = {}

    for inp in form.find_all("input"):
        name = inp.get("name")
        if not name:
            continue
        itype = (inp.get("type") or "text").lower()
        value = inp.get("value", "")

        if itype in ("submit", "button", "image", "file"):
            continue

        if itype in ("checkbox", "radio"):
            if inp.has_attr("checked"):
                data[name] = value if value != "" else "1"
            continue

        data[name] = value

    for sel in form.find_all("select"):
        name = sel.get("name")
        if not name:
            continue
        opt = sel.find("option", selected=True)
        if opt is None:
            opt = sel.find("option")
        if opt is None:
            continue
        data[name] = opt.get("value") if opt.has_attr("value") else opt.get_text(strip=True)

    for ta in form.find_all("textarea"):
        name = ta.get("name")
        if not name:
            continue
        data[name] = ta.get_text() or ""

    return data


def guess_year_month_field_names(form) -> Tuple[str, str]:
    year_field = "year"
    month_field = "month"

    for sel in form.find_all("select"):
        name = sel.get("name")
        if not name:
            continue
        opts = [o.get_text(strip=True) for o in sel.find_all("option")]
        yearish = sum(1 for o in opts if o.isdigit() and len(o) == 4)
        monthish = sum(1 for o in opts if o.isdigit() and 1 <= int(o) <= 12) if opts else 0
        if yearish >= 5:
            year_field = name
        if monthish >= 5:
            month_field = name

    return year_field, month_field


def force_output_format_xml(form, payload: Dict[str, str]) -> bool:
    """
    Find a likely "output format" field and set it to XML.
    Returns True if we changed something.
    """
    changed = False
    candidates: List[Tuple[str, List[Tuple[str, str]]]] = []

    # Look at selects for an option that looks like XML
    for sel in form.find_all("select"):
        name = sel.get("name")
        if not name:
            continue
        opts: List[Tuple[str, str]] = []
        for o in sel.find_all("option"):
            label = (o.get_text(strip=True) or "")
            value = o.get("value", "")
            opts.append((value, label))
        candidates.append((name, opts))

        for value, label in opts:
            if "xml" in (value or "").lower() or "xml" in (label or "").lower():
                payload[name] = value if value != "" else label
                LOG.info("Set output format field '%s' to XML (%r)", name, payload[name])
                return True  # stop at first decisive match

    # If no select matched, try radios
    radio_groups: Dict[str, List[Tuple[str, str]]] = {}
    for inp in form.find_all("input", {"type": "radio"}):
        name = inp.get("name")
        if not name:
            continue
        value = inp.get("value", "")
        # label text is not always adjacent, but value is often enough
        radio_groups.setdefault(name, []).append((value, value))

    for name, opts in radio_groups.items():
        for value, _ in opts:
            if "xml" in (value or "").lower():
                payload[name] = value
                LOG.info("Set radio output format field '%s' to XML (%r)", name, value)
                return True

    # Nothing matched: log candidates so you can see what DW expects
    LOG.warning("Could not auto-detect XML output format field.")
    for name, opts in candidates:
        preview = ", ".join([f"{v!r}:{l!r}" for v, l in opts[:8]])
        LOG.warning("Select '%s' options (first 8): %s", name, preview)

    return changed


def looks_like_xml_export(resp: requests.Response) -> bool:
    ct = (resp.headers.get("Content-Type") or "").lower()
    text = resp.text.lstrip()

    if "xml" in ct:
        return True

    head = text[:5000].lower()
    markers = ["<?xml", "<livejournal", "<lj:livejournal", "<event", "<entry"]
    return any(m in head for m in markers)


def write_debug_artifacts(
    debug_dir: Path,
    year: int,
    month: int,
    stage: str,
    resp: requests.Response,
    request_url: str,
) -> None:
    debug_dir.mkdir(parents=True, exist_ok=True)
    ct = (resp.headers.get("Content-Type") or "").lower()
    ext = "xml" if "xml" in ct else "html"
    body_path = debug_dir / f"{stage}_{year:04d}-{month:02d}_body.{ext}"
    meta_path = debug_dir / f"{stage}_{year:04d}-{month:02d}_meta.txt"

    body_path.write_text(resp.text, encoding="utf-8", errors="replace")

    lines = [
        f"stage: {stage}",
        f"request_url: {request_url}",
        f"final_url: {resp.url}",
        f"status_code: {resp.status_code}",
        f"content_type: {resp.headers.get('Content-Type', '')}",
        "---- response headers ----",
    ]
    lines.extend([f"{k}: {v}" for k, v in resp.headers.items()])
    meta_path.write_text("\n".join(lines), encoding="utf-8")

    LOG.error("Wrote debug body to: %s", body_path)
    LOG.error("Wrote debug meta to: %s", meta_path)


def main() -> int:
    ap = argparse.ArgumentParser(description="Export a single Dreamwidth journal month as XML")
    ap.add_argument("--year", type=int, required=True, help="Year to export (e.g. 2006)")
    ap.add_argument("--month", type=int, required=True, help="Month to export (1–12)")
    ap.add_argument("--cookie-file", help="Netscape cookies.txt containing dreamwidth.org cookies")
    ap.add_argument("--cookie-header", help="Raw Cookie header from browser devtools")
    ap.add_argument("--journal", default="", help="Journal/community name (optional)")
    ap.add_argument("--outdir", default="dw_exports", help="Output directory for XML exports")
    ap.add_argument("--debug-dir", default="dw_debug", help="Directory to write failure responses")
    ap.add_argument("--print-snippet", action="store_true", help="Print first ~2k chars of failure response")
    ap.add_argument("--log-level", default="INFO", help="DEBUG, INFO, WARNING, ERROR")
    args = ap.parse_args()

    configure_logging(args.log_level)

    if not (1 <= args.month <= 12):
        LOG.error("--month must be between 1 and 12")
        return 2
    if not args.cookie_file and not args.cookie_header:
        LOG.error("You must supply --cookie-file or --cookie-header")
        return 2

    cfg = RateLimitConfig()
    session = requests.Session()
    session.headers.update(
        {
            "User-Agent": "dw-export-script/1.4",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        }
    )

    if args.cookie_file:
        session.cookies.update(load_netscape_cookie_file(Path(args.cookie_file)))
        LOG.info("Loaded cookies from %s", args.cookie_file)
    else:
        session.cookies.update(parse_cookie_header(args.cookie_header))
        LOG.info("Loaded cookies from --cookie-header")

    base = "https://www.dreamwidth.org"
    export_url = f"{base}/export"
    if args.journal:
        export_url += f"?journal={args.journal}"

    LOG.info("Fetching export page")
    r = request_with_retries(session, "GET", export_url, cfg)
    jitter_sleep(cfg)

    if "returnto=/export" in r.url or "login" in r.url.lower():
        write_debug_artifacts(Path(args.debug_dir), args.year, args.month, "get_export_redirect", r, export_url)
        LOG.error("Not authenticated (redirected to login). Re-export cookies.")
        return 3

    if r.status_code != 200:
        write_debug_artifacts(Path(args.debug_dir), args.year, args.month, "get_export_badstatus", r, export_url)
        return 3

    try:
        form = choose_export_form(r.text)
        action = form.get("action", "/export")
        payload = build_form_payload(form)
        year_field, month_field = guess_year_month_field_names(form)

        # Override year/month
        payload[year_field] = str(args.year)
        payload[month_field] = str(args.month)

        # Force output format to XML if possible
        force_output_format_xml(form, payload)

    except Exception:
        write_debug_artifacts(Path(args.debug_dir), args.year, args.month, "get_export_parsefail", r, export_url)
        raise

    post_url = urljoin(base + "/", action.lstrip("/"))
    LOG.debug("POST url: %s", post_url)
    LOG.debug("POST payload keys: %s", sorted(payload.keys()))

    LOG.info("Requesting export for %04d-%02d", args.year, args.month)
    pr = request_with_retries(session, "POST", post_url, cfg, data=payload)
    jitter_sleep(cfg)

    if not looks_like_xml_export(pr):
        write_debug_artifacts(Path(args.debug_dir), args.year, args.month, "post_export_notxml", pr, post_url)
        if args.print_snippet:
            print("\n--- response snippet (first 2000 chars) ---\n")
            print(pr.text[:2000])
            print("\n--- end snippet ---\n")
        LOG.error("Response does not look like XML export")
        return 4

    outdir = Path(args.outdir)
    outdir.mkdir(parents=True, exist_ok=True)
    name = args.journal or "default"
    out = outdir / f"dw_export_{name}_{args.year:04d}-{args.month:02d}.xml"
    out.write_text(pr.text, encoding="utf-8", errors="replace")

    LOG.info("Saved %s", out)
    return 0


if __name__ == "__main__":
    sys.exit(main())

