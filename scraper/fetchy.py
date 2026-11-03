#!/usr/bin/env python3
"""
Jackson County (Kansas City, MO) motivated-seller lead scraper.

Pipeline
  1. Recorder of Deeds portal (Aumentum Web, via Playwright async) -> last N days of recordings
  2. Keep only the lead types we care about (classified from the document-type text)
  3. County parcel DBF (dbfread) -> owner-name lookup -> property + mailing address
  4. Score, flag, write dashboard/records.json + data/records.json + data/ghl_export.csv

Env overrides (all optional)
  LOOKBACK_DAYS     default 7
  CLERK_URL         default https://aumentumweb.jacksongov.org/
  PARCEL_URL        page (or direct file URL) that offers the bulk parcel file
  PARCEL_LOCAL      path to a local .dbf / .zip (skips the download)
  NAME_ORDER        FIRST_LAST (default) | LAST_FIRST  -> used only to split names for the GHL CSV
  MAX_PAGES         max result pages to walk (default 200)
  HEADLESS          1 (default) | 0
"""
from __future__ import annotations

import asyncio
import csv
import functools
import hashlib
import io
import json
import logging
import os
import re
import sys
import tempfile
import time
import zipfile
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, List, Optional, Tuple
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

# --------------------------------------------------------------------------- #
# Config
# --------------------------------------------------------------------------- #
ROOT = Path(__file__).resolve().parent.parent
OUT_DASHBOARD = ROOT / "dashboard" / "records.json"
OUT_DATA = ROOT / "data" / "records.json"
OUT_GHL = ROOT / "data" / "ghl_export.csv"
DEBUG_DIR = ROOT / "data" / "debug"

LOOKBACK_DAYS = int(os.getenv("LOOKBACK_DAYS", "7"))
CLERK_URL = os.getenv("CLERK_URL", "https://aumentumweb.jacksongov.org/")
CLERK_INFO_URL = (
    "https://www.jacksongov.org/Government/Departments/Recorder-of-Deeds/Searching-Public-Records"
)
PARCEL_URL = os.getenv("PARCEL_URL", "").strip()
PARCEL_LOCAL = os.getenv("PARCEL_LOCAL", "").strip()
NAME_ORDER = os.getenv("NAME_ORDER", "FIRST_LAST").upper()
MAX_PAGES = int(os.getenv("MAX_PAGES", "200"))
HEADLESS = os.getenv("HEADLESS", "1") != "0"
SOURCE = "Jackson County MO Recorder of Deeds (Aumentum)"
DEFAULT_STATE = "MO"

# Pages on jacksongov.org that may link to bulk tabular/GIS data.
PARCEL_CANDIDATE_PAGES = [
    "https://www.jacksongov.org/Government/Departments/Assessment",
    "https://www.jacksongov.org/Government/Departments/Geographic-Information-Systems",
]

UA = (
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0 Safari/537.36"
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-7s %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("scraper")


# --------------------------------------------------------------------------- #
# Retry helper (sync + async), 3 attempts
# --------------------------------------------------------------------------- #
def retry(attempts: int = 3, delay: float = 2.0):
    def deco(fn: Callable):
        if asyncio.iscoroutinefunction(fn):

            @functools.wraps(fn)
            async def awrap(*a, **k):
                for i in range(1, attempts + 1):
                    try:
                        return await fn(*a, **k)
                    except Exception as e:  # noqa: BLE001
                        log.warning("%s failed (%d/%d): %s", fn.__name__, i, attempts, e)
                        if i == attempts:
                            raise
                        await asyncio.sleep(delay * i)

            return awrap

        @functools.wraps(fn)
        def wrap(*a, **k):
            for i in range(1, attempts + 1):
                try:
                    return fn(*a, **k)
                except Exception as e:  # noqa: BLE001
                    log.warning("%s failed (%d/%d): %s", fn.__name__, i, attempts, e)
                    if i == attempts:
                        raise
                    time.sleep(delay * i)

        return wrap

    return deco


# --------------------------------------------------------------------------- #
# Lead categories
# --------------------------------------------------------------------------- #
CAT_LABELS: Dict[str, str] = {
    "LP": "Lis Pendens",
    "NOFC": "Notice of Foreclosure",
    "TAXDEED": "Tax Deed",
    "JUD": "Judgment",
    "CCJ": "Certified Judgment",
    "DRJUD": "Domestic Judgment",
    "LNCORPTX": "Corp Tax Lien",
    "LNIRS": "IRS Lien",
    "LNFED": "Federal Lien",
    "LN": "Lien",
    "LNMECH": "Mechanic Lien",
    "LNHOA": "HOA Lien",
    "MEDLN": "Medicaid Lien",
    "PRO": "Probate Document",
    "NOC": "Notice of Commencement",
    "RELLP": "Release Lis Pendens",
}

# Order matters: most specific first.
CAT_PATTERNS: List[Tuple[str, re.Pattern]] = [
    ("RELLP", re.compile(r"(RELEASE|SATISF|DISMISS|WITHDRAW).*LIS\s*PENDENS|LIS\s*PENDENS.*(RELEASE|SATISF|DISMISS|WITHDRAW)", re.I)),
    ("LP", re.compile(r"LIS\s*PENDENS", re.I)),
    ("NOFC", re.compile(r"FORECLOS|NOTICE\s+OF\s+(TRUSTEE'?S?\s+)?SALE|TRUSTEE'?S?\s+SALE|NOTICE\s+OF\s+DEFAULT", re.I)),
    ("TAXDEED", re.compile(r"TAX\s+DEED|COLLECTOR'?S?\s+DEED|TAX\s+SALE\s+DEED", re.I)),
    ("CCJ", re.compile(r"CERTIFIED\s+JUDG", re.I)),
    ("DRJUD", re.compile(r"DOMESTIC.*JUDG|JUDG.*DOMESTIC", re.I)),
    ("JUD", re.compile(r"JUDG", re.I)),
    ("LNIRS", re.compile(r"\bIRS\b|INTERNAL\s+REVENUE", re.I)),
    ("LNFED", re.compile(r"FEDERAL.*(LIEN|TAX)|\bFED\b.*LIEN", re.I)),
    ("LNCORPTX", re.compile(r"(CORP|CORPORATE|STATE|DEPT\.?\s+OF\s+REVENUE|DEPARTMENT\s+OF\s+REVENUE).*(TAX\s+)?LIEN", re.I)),
    ("LNMECH", re.compile(r"MECHANIC|MATERIALM[AE]N", re.I)),
    ("LNHOA", re.compile(r"\bHOA\b|HOME\s*OWNER|ASSOCIATION.*LIEN|ASSESSMENT\s+LIEN", re.I)),
    ("MEDLN", re.compile(r"MEDICAID|HEALTHNET|MO\s*HEALTH", re.I)),
    ("LN", re.compile(r"\bLIENS?\b", re.I)),
    ("PRO", re.compile(r"PROBATE|LETTERS\s+(TESTAMENTARY|OF\s+ADMIN)|HEIRSHIP|ESTATE\s+OF", re.I)),
    ("NOC", re.compile(r"NOTICE\s+OF\s+COMMENCEMENT", re.I)),
]
RELEASE_RE = re.compile(r"RELEASE|SATISFACTION|DISCHARGE|WITHDRAW", re.I)


def classify(doc_type: str) -> Optional[str]:
    """Map the portal's document-type text to one of our category codes."""
    t = (doc_type or "").strip()
    if not t:
        return None
    for code, pat in CAT_PATTERNS:
        if pat.search(t):
            return code
    return None


def is_release_of_non_lp(doc_type: str, code: str) -> bool:
    """'RELEASE OF LIEN' etc. are not leads; only RELLP is wanted."""
    return code != "RELLP" and bool(RELEASE_RE.search(doc_type or ""))


# --------------------------------------------------------------------------- #
# Small utils
# --------------------------------------------------------------------------- #
CORP_RE = re.compile(
    r"\b(LLC|L\.?L\.?C|INC|INCORPORATED|CORP|CORPORATION|CO|COMPANY|LP|LLP|LTD|TRUST|BANK|HOLDINGS|PROPERTIES|"
    r"INVESTMENTS?|PARTNERS|PARTNERSHIP|ENTERPRISES|ASSOCIATION|ASSN|HOMES|REALTY|GROUP|VENTURES)\b",
    re.I,
)
MONEY_RE = re.compile(r"\$\s*([\d,]+(?:\.\d{1,2})?)")
DATE_FORMATS = ("%m/%d/%Y", "%m/%d/%y", "%Y-%m-%d", "%m-%d-%Y", "%m/%d/%Y %I:%M:%S %p", "%m/%d/%Y %H:%M", "%b %d, %Y")


def clean(s: Any) -> str:
    return re.sub(r"\s+", " ", str(s or "")).strip()


def parse_date(s: str) -> Optional[datetime]:
    s = clean(s)
    if not s:
        return None
    for f in DATE_FORMATS:
        try:
            return datetime.strptime(s, f)
        except ValueError:
            continue
    m = re.search(r"(\d{1,2})/(\d{1,2})/(\d{4})", s)
    if m:
        try:
            return datetime(int(m.group(3)), int(m.group(1)), int(m.group(2)))
        except ValueError:
            return None
    return None


def parse_amount(*texts: str) -> Optional[float]:
    for t in texts:
        t = clean(t)
        if not t:
            continue
        m = MONEY_RE.search(t)
        if m:
            try:
                return float(m.group(1).replace(",", ""))
            except ValueError:
                pass
        if re.fullmatch(r"[\d,]+(\.\d{1,2})?", t):
            try:
                return float(t.replace(",", ""))
            except ValueError:
                pass
    return None


def is_corp(name: str) -> bool:
    return bool(CORP_RE.search(name or ""))


def split_names(raw: str) -> List[str]:
    parts = re.split(r"\s*(?:;|\n|\|| / | & (?=[A-Z]+,))\s*", raw or "")
    return [clean(p) for p in parts if clean(p)]


# --------------------------------------------------------------------------- #
# 1) Clerk portal (Playwright async)
# --------------------------------------------------------------------------- #
HEADER_ALIASES: Dict[str, List[str]] = {
    "doc_num": ["document number", "doc number", "doc #", "doc no", "instrument number", "instrument #", "instrument", "reception", "doc num", "number"],
    "doc_type": ["document type", "doc type", "instrument type", "type"],
    "filed": ["recorded date", "record date", "date recorded", "recording date", "date filed", "file date", "filed", "recorded"],
    "grantor": ["grantor", "first party", "party 1", "from", "debtor", "owner"],
    "grantee": ["grantee", "second party", "party 2", "to", "creditor"],
    "legal": ["legal description", "legal", "description"],
    "amount": ["lien amount", "amount", "consideration", "debt"],
}


def map_headers(headers: List[str]) -> Dict[str, int]:
    norm = [clean(h).lower() for h in headers]
    out: Dict[str, int] = {}
    for field, aliases in HEADER_ALIASES.items():
        for alias in aliases:
            hit = next(
                (i for i, h in enumerate(norm) if i not in out.values() and (h == alias or (len(alias) > 4 and alias in h))),
                None,
            )
            if hit is not None:
                out[field] = hit
                break
    return out


def parse_results_table(html: str, base_url: str) -> List[Dict[str, str]]:
    """Find the best results table in the page and return row dicts."""
    soup = BeautifulSoup(html, "lxml")
    best: List[Dict[str, str]] = []
    for table in soup.find_all("table"):
        rows = table.find_all("tr")
        if len(rows) < 2:
            continue
        head_cells = rows[0].find_all(["th", "td"])
        headers = [clean(c.get_text(" ")) for c in head_cells]
        mapping = map_headers(headers)
        if "doc_type" not in mapping or not ({"doc_num", "filed"} & set(mapping)):
            continue
        parsed: List[Dict[str, str]] = []
        for tr in rows[1:]:
            try:
                cells = tr.find_all("td")
                if len(cells) < len(mapping):
                    continue
                texts = [clean(c.get_text("\n")) for c in cells]
                rec = {
                    f: (texts[i].replace("\n", "; ") if f in ("grantor", "grantee") else clean(texts[i]))
                    for f, i in mapping.items()
                    if i < len(texts)
                }
                a = tr.find("a", href=True)
                rec["url"] = urljoin(base_url, a["href"]) if a and not a["href"].startswith("javascript") else ""
                parsed.append(rec)
            except Exception as e:  # noqa: BLE001
                log.debug("bad row skipped: %s", e)
        if len(parsed) > len(best):
            best = parsed
    return best


async def _dump_debug(page, tag: str) -> None:
    try:
        DEBUG_DIR.mkdir(parents=True, exist_ok=True)
        (DEBUG_DIR / f"{tag}.html").write_text(await page.content(), encoding="utf-8")
        await page.screenshot(path=str(DEBUG_DIR / f"{tag}.png"), full_page=True)
        log.info("debug snapshot saved: data/debug/%s.*", tag)
    except Exception as e:  # noqa: BLE001
        log.debug("debug dump failed: %s", e)


async def _click_first(page, texts: Iterable[str], timeout: int = 2500) -> bool:
    for t in texts:
        for role in ("link", "button"):
            try:
                loc = page.get_by_role(role, name=re.compile(rf"^\s*{re.escape(t)}\s*$|{re.escape(t)}", re.I))
                if await loc.count():
                    await loc.first.click(timeout=timeout)
                    await page.wait_for_load_state("domcontentloaded")
                    return True
            except Exception:  # noqa: BLE001
                continue
    return False


@retry(attempts=3, delay=3)
async def _open_portal(page) -> None:
    await page.goto(CLERK_URL, wait_until="domcontentloaded", timeout=60000)
    await page.wait_for_load_state("networkidle", timeout=30000)
    # Disclaimer / guest login gates
    await _click_first(page, ["I Agree", "Accept", "Agree", "Continue as Guest", "Guest", "Enter"])
    # Navigate to the records search
    await _click_first(
        page,
        ["Official Records", "Recorded Documents", "Document Search", "Records Search", "Search Records", "Recorder", "Name Search"],
    )
    await page.wait_for_load_state("networkidle", timeout=30000)


async def _fill_dates(page, start: datetime, end: datetime) -> bool:
    start_s, end_s = start.strftime("%m/%d/%Y"), end.strftime("%m/%d/%Y")
    inputs = page.locator("input:visible")
    n = await inputs.count()
    start_el = end_el = None
    for i in range(n):
        el = inputs.nth(i)
        attrs = " ".join(
            [(await el.get_attribute(a)) or "" for a in ("id", "name", "placeholder", "aria-label", "title")]
        ).lower()
        if "date" not in attrs and "record" not in attrs:
            continue
        if re.search(r"(from|start|begin|after)", attrs) and start_el is None:
            start_el = el
        elif re.search(r"(to|end|thru|through|before)", attrs) and end_el is None:
            end_el = el
    if start_el is None or end_el is None:
        return False
    for el, val in ((start_el, start_s), (end_el, end_s)):
        await el.click()
        await el.fill("")
        await el.type(val, delay=20)
        await el.press("Tab")
    return True


@retry(attempts=3, delay=3)
async def _run_search(page, start: datetime, end: datetime) -> None:
    if not await _fill_dates(page, start, end):
        await _dump_debug(page, "no_date_fields")
        raise RuntimeError("could not locate From/To date inputs on the search form")
    if not await _click_first(page, ["Search", "Submit", "Find", "Go"], timeout=5000):
        await page.keyboard.press("Enter")
    await page.wait_for_load_state("networkidle", timeout=60000)


async def _click_next(page) -> bool:
    selectors = [
        'a[aria-label*="next" i]', 'button[aria-label*="next" i]',
        'a:has-text("Next")', 'button:has-text("Next")',
        'li.next a', 'a.next', 'a:has-text(">")',
    ]
    for sel in selectors:
        try:
            loc = page.locator(sel + ":visible")
            if not await loc.count():
                continue
            el = loc.first
            cls = ((await el.get_attribute("class")) or "").lower()
            if (
                await el.get_attribute("disabled") is not None
                or (await el.get_attribute("aria-disabled")) == "true"
                or "disabled" in cls
            ):
                return False
            await el.click(timeout=5000)
            try:
                await page.wait_for_load_state("networkidle", timeout=30000)
            except Exception:  # noqa: BLE001
                await asyncio.sleep(1.5)
            return True
        except Exception:  # noqa: BLE001
            continue
    return False


async def scrape_clerk(start: datetime, end: datetime) -> List[Dict[str, str]]:
    from playwright.async_api import async_playwright

    rows: List[Dict[str, str]] = []
    async with async_playwright() as pw:
        browser = await pw.chromium.launch(headless=HEADLESS, args=["--no-sandbox"])
        ctx = await browser.new_context(user_agent=UA, viewport={"width": 1440, "height": 900})
        page = await ctx.new_page()
        page.set_default_timeout(30000)
        try:
            await _open_portal(page)
            await _run_search(page, start, end)
            seen_fp = set()
            for n in range(1, MAX_PAGES + 1):
                html = await page.content()
                batch = parse_results_table(html, page.url)
                if not batch:
                    if n == 1:
                        await _dump_debug(page, "no_results_table")
                    break
                fp = hashlib.md5(json.dumps(batch[:3], sort_keys=True).encode()).hexdigest()
                if fp in seen_fp:
                    break
                seen_fp.add(fp)
                rows.extend(batch)
                log.info("clerk page %d: %d rows (total %d)", n, len(batch), len(rows))
                if not await _click_next(page):
                    break
        except Exception as e:  # noqa: BLE001
            log.error("clerk scrape error: %s", e)
            await _dump_debug(page, "clerk_error")
        finally:
            await ctx.close()
            await browser.close()
    return rows


def build_clerk_records(raw_rows: List[Dict[str, str]], start: datetime, end: datetime) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    seen = set()
    for r in raw_rows:
        try:
            doc_type = clean(r.get("doc_type"))
            code = classify(doc_type)
            if not code or is_release_of_non_lp(doc_type, code):
                continue
            doc_num = clean(r.get("doc_num"))
            if not doc_num:
                continue
            filed_dt = parse_date(r.get("filed", ""))
            if filed_dt and not (start - timedelta(days=1) <= filed_dt <= end + timedelta(days=1)):
                continue
            key = (doc_num, code)
            if key in seen:
                continue
            seen.add(key)
            out.append(
                {
                    "doc_num": doc_num,
                    "doc_type": doc_type,
                    "filed": filed_dt.strftime("%Y-%m-%d") if filed_dt else clean(r.get("filed")),
                    "cat": code,
                    "cat_label": CAT_LABELS[code],
                    "owner": clean(r.get("grantor")),
                    "grantee": clean(r.get("grantee")),
                    "amount": parse_amount(r.get("amount", ""), r.get("legal", "")),
                    "legal": clean(r.get("legal")),
                    "clerk_url": r.get("url") or CLERK_URL,
                }
            )
        except Exception as e:  # noqa: BLE001
            log.debug("record skipped: %s", e)
    return out


# --------------------------------------------------------------------------- #
# 2) Parcel data (requests + BeautifulSoup + dbfread)
# --------------------------------------------------------------------------- #
FILE_LINK_RE = re.compile(r"\.(dbf|zip)(\?|#|$)", re.I)
POSTBACK_RE = re.compile(r"__doPostBack\(\s*'([^']*)'\s*,\s*'([^']*)'\s*\)")
LINK_TEXT_RE = re.compile(r"parcel|real\s*(property|estate)|assessment|tabular|dbf|download|data\s*(file|extract)", re.I)


def _session() -> requests.Session:
    s = requests.Session()
    s.headers.update({"User-Agent": UA, "Accept-Language": "en-US,en;q=0.9"})
    return s


@retry(attempts=3, delay=2)
def _get(s: requests.Session, url: str, **kw) -> requests.Response:
    r = s.get(url, timeout=kw.pop("timeout", 90), **kw)
    r.raise_for_status()
    return r


@retry(attempts=3, delay=2)
def _post(s: requests.Session, url: str, data: Dict[str, str], **kw) -> requests.Response:
    r = s.post(url, data=data, timeout=kw.pop("timeout", 180), **kw)
    r.raise_for_status()
    return r


def _is_file_response(r: requests.Response) -> bool:
    ct = r.headers.get("Content-Type", "").lower()
    cd = r.headers.get("Content-Disposition", "").lower()
    return "attachment" in cd or any(x in ct for x in ("zip", "octet-stream", "dbase", "x-dbf"))


def _aspnet_form(soup: BeautifulSoup) -> Dict[str, str]:
    data: Dict[str, str] = {}
    for inp in soup.find_all("input", {"type": "hidden"}):
        if inp.get("name"):
            data[inp["name"]] = inp.get("value", "")
    return data


def _do_postback(s: requests.Session, page_url: str, soup: BeautifulSoup, target: str, arg: str) -> requests.Response:
    """Replay an ASP.NET __doPostBack(target, arg) as a real POST."""
    data = _aspnet_form(soup)
    data["__EVENTTARGET"] = target
    data["__EVENTARGUMENT"] = arg
    return _post(s, page_url, data, headers={"Referer": page_url})


def _find_file_in_page(s: requests.Session, url: str, depth: int = 0) -> Optional[bytes]:
    """Scan a static page for a downloadable parcel file (direct link or __doPostBack)."""
    r = _get(s, url)
    if _is_file_response(r):
        return r.content
    soup = BeautifulSoup(r.text, "lxml")
    anchors = soup.find_all("a", href=True)

    # direct links first
    for a in anchors:
        href, text = a["href"], clean(a.get_text(" "))
        if FILE_LINK_RE.search(href) and (LINK_TEXT_RE.search(text + href) or True):
            try:
                fr = _get(s, urljoin(url, href), timeout=300)
                if _is_file_response(fr) or FILE_LINK_RE.search(fr.url):
                    log.info("parcel file: %s", fr.url)
                    return fr.content
            except Exception as e:  # noqa: BLE001
                log.warning("direct link failed (%s): %s", href, e)

    # __doPostBack links
    for a in anchors:
        m = POSTBACK_RE.search(a["href"]) or POSTBACK_RE.search(a.get("onclick", ""))
        if not m or not LINK_TEXT_RE.search(clean(a.get_text(" "))):
            continue
        try:
            pr = _do_postback(s, url, soup, m.group(1), m.group(2))
            if _is_file_response(pr):
                log.info("parcel file via postback: %s", clean(a.get_text(" ")))
                return pr.content
            if depth < 1:  # postback returned another page that may hold the file link
                sub = BeautifulSoup(pr.text, "lxml")
                for sa in sub.find_all("a", href=True):
                    if FILE_LINK_RE.search(sa["href"]):
                        return _get(s, urljoin(url, sa["href"]), timeout=300).content
        except Exception as e:  # noqa: BLE001
            log.warning("postback failed: %s", e)

    # ASP.NET buttons (<input type=submit>)
    for btn in soup.find_all("input", {"type": ["submit", "button"]}):
        label = btn.get("value", "") + " " + btn.get("name", "")
        if btn.get("name") and LINK_TEXT_RE.search(label) and re.search(r"download|export|dbf", label, re.I):
            try:
                data = _aspnet_form(soup)
                data[btn["name"]] = btn.get("value", "")
                pr = _post(s, url, data, headers={"Referer": url})
                if _is_file_response(pr):
                    return pr.content
            except Exception as e:  # noqa: BLE001
                log.warning("button postback failed: %s", e)
    return None


def _write_dbf(content: bytes, workdir: Path) -> Optional[Path]:
    """Accepts raw DBF or a ZIP containing one; returns path to the .dbf on disk."""
    if content[:2] == b"PK":
        with zipfile.ZipFile(io.BytesIO(content)) as z:
            members = [n for n in z.namelist() if n.lower().endswith((".dbf", ".dbt", ".fpt"))]
            dbfs = [n for n in members if n.lower().endswith(".dbf")]
            if not dbfs:
                return None
            # pick the biggest .dbf (the parcel table)
            dbfs.sort(key=lambda n: z.getinfo(n).file_size, reverse=True)
            for n in members:
                z.extract(n, workdir)
            return workdir / dbfs[0]
    p = workdir / "parcels.dbf"
    p.write_bytes(content)
    return p


def download_parcel_dbf(workdir: Path) -> Optional[Path]:
    if PARCEL_LOCAL:
        p = Path(PARCEL_LOCAL)
        if p.exists():
            return _write_dbf(p.read_bytes(), workdir)
        log.warning("PARCEL_LOCAL not found: %s", p)
    s = _session()
    pages = [PARCEL_URL] if PARCEL_URL else PARCEL_CANDIDATE_PAGES
    for url in pages:
        try:
            log.info("looking for parcel file on %s", url)
            content = _find_file_in_page(s, url)
            if content:
                path = _write_dbf(content, workdir)
                if path:
                    return path
        except Exception as e:  # noqa: BLE001
            log.warning("parcel page failed (%s): %s", url, e)
    return None


COL_ALIASES: Dict[str, List[str]] = {
    "owner": ["OWNER", "OWN1", "OWNER1", "OWNER_NAME", "OWNERNAME", "TAXPAYER", "NAME"],
    "site_addr": ["SITE_ADDR", "SITEADDR", "SITE_ADDRESS", "PROP_ADDR", "PROPADDR", "SITUS", "LOCADDR", "ADDRESS"],
    "site_city": ["SITE_CITY", "SITECITY", "PROP_CITY", "SITUS_CITY"],
    "site_zip": ["SITE_ZIP", "SITEZIP", "PROP_ZIP", "SITUS_ZIP"],
    "mail_addr": ["ADDR_1", "MAILADR1", "MAIL_ADDR", "MAILADDR", "MAIL_ADDR1", "OWNER_ADDR", "MAILADDRESS"],
    "mail_city": ["CITY", "MAILCITY", "MAIL_CITY"],
    "mail_state": ["STATE", "MAILSTATE", "MAIL_STATE"],
    "mail_zip": ["ZIP", "MAILZIP", "MAIL_ZIP"],
}


def norm_name(s: str) -> str:
    s = re.sub(r"[^\w\s,]", " ", (s or "").upper())
    return re.sub(r"\s+", " ", s).strip()


def name_variants(name: str) -> List[str]:
    """'FIRST LAST', 'LAST FIRST', 'LAST, FIRST' (+ full normalized string)."""
    n = norm_name(name)
    if not n:
        return []
    out = [n, n.replace(",", "")]
    if is_corp(n):
        return list(dict.fromkeys(out))
    if "," in n:
        last, _, rest = n.partition(",")
        toks = rest.split()
        if last.strip() and toks:
            first = toks[0]
            last = last.strip()
            out += [f"{first} {last}", f"{last} {first}", f"{last}, {first}"]
    else:
        toks = n.split()
        if len(toks) >= 2:
            a, b, z = toks[0], toks[1], toks[-1]
            out += [f"{a} {b}", f"{b} {a}", f"{a}, {b}"]
            if len(z) > 1 and z != b:  # skip middle initials as a "last name"
                out += [f"{a} {z}", f"{z} {a}", f"{z}, {a}"]
    return list(dict.fromkeys(v for v in out if v))


def build_owner_lookup(dbf_path: Path) -> Dict[str, Dict[str, str]]:
    from dbfread import DBF

    table = DBF(str(dbf_path), encoding="latin-1", ignore_missing_memofile=True, char_decode_errors="ignore", lowernames=False)
    fields = {f.upper(): f for f in table.field_names}
    colmap = {k: next((fields[a] for a in aliases if a in fields), None) for k, aliases in COL_ALIASES.items()}
    log.info("parcel columns mapped: %s", {k: v for k, v in colmap.items()})
    if not colmap["owner"]:
        raise RuntimeError(f"no owner column found in DBF (have: {list(fields)})")

    lookup: Dict[str, Dict[str, str]] = {}
    count = 0
    for rec in table:
        try:
            owner = clean(rec.get(colmap["owner"]))
            if not owner:
                continue
            row = {k: clean(rec.get(c)) if c else "" for k, c in colmap.items() if k != "owner"}
            count += 1
            for v in name_variants(owner):
                lookup.setdefault(v, row)
        except Exception:  # noqa: BLE001
            continue
    log.info("parcels read: %d, lookup keys: %d", count, len(lookup))
    return lookup


def load_parcel_lookup() -> Dict[str, Dict[str, str]]:
    try:
        with tempfile.TemporaryDirectory() as td:
            path = download_parcel_dbf(Path(td))
            if not path:
                log.warning(
                    "No parcel DBF obtained. Set PARCEL_URL (page/file) or PARCEL_LOCAL (path) to enable address enrichment."
                )
                return {}
            return build_owner_lookup(path)
    except Exception as e:  # noqa: BLE001
        log.error("parcel lookup failed: %s", e)
        return {}


def enrich_with_parcels(records: List[Dict[str, Any]], lookup: Dict[str, Dict[str, str]]) -> None:
    for r in records:
        r.update(
            {
                "prop_address": "", "prop_city": "", "prop_state": DEFAULT_STATE, "prop_zip": "",
                "mail_address": "", "mail_city": "", "mail_state": "", "mail_zip": "",
            }
        )
        if not lookup:
            continue
        try:
            hit = None
            for nm in split_names(r.get("owner", "")):
                for v in name_variants(nm):
                    if v in lookup:
                        hit = lookup[v]
                        break
                if hit:
                    break
            if hit:
                r["prop_address"] = hit.get("site_addr", "")
                r["prop_city"] = hit.get("site_city", "")
                r["prop_zip"] = hit.get("site_zip", "")
                r["mail_address"] = hit.get("mail_addr", "")
                r["mail_city"] = hit.get("mail_city", "")
                r["mail_state"] = hit.get("mail_state", "")
                r["mail_zip"] = hit.get("mail_zip", "")
        except Exception as e:  # noqa: BLE001
            log.debug("enrich failed for %s: %s", r.get("doc_num"), e)


# --------------------------------------------------------------------------- #
# 3) Flags + scoring
# --------------------------------------------------------------------------- #
CAT_FLAGS: Dict[str, List[str]] = {
    "LP": ["Lis pendens"],
    "NOFC": ["Pre-foreclosure"],
    "TAXDEED": ["Tax lien"],
    "JUD": ["Judgment lien"], "CCJ": ["Judgment lien"], "DRJUD": ["Judgment lien"],
    "LNCORPTX": ["Tax lien"], "LNIRS": ["Tax lien"], "LNFED": ["Tax lien"],
    "LNMECH": ["Mechanic lien"],
    "PRO": ["Probate / estate"],
}


def owner_key(owner: str) -> str:
    return norm_name(split_names(owner)[0]) if split_names(owner) else ""


def apply_scores(records: List[Dict[str, Any]]) -> None:
    # owners that have both an LP and a foreclosure notice anywhere in the batch
    cats_by_owner: Dict[str, set] = {}
    for r in records:
        k = owner_key(r["owner"])
        if k:
            cats_by_owner.setdefault(k, set()).add(r["cat"])

    for r in records:
        try:
            flags: List[str] = list(CAT_FLAGS.get(r["cat"], []))
            owners = cats_by_owner.get(owner_key(r["owner"]), set())
            combo = "LP" in owners and "NOFC" in owners
            if combo:
                for f in ("Lis pendens", "Pre-foreclosure"):
                    if f not in flags:
                        flags.append(f)
            if is_corp(r["owner"]):
                flags.append("LLC / corp owner")

            filed = parse_date(r.get("filed", ""))
            is_new = bool(filed and filed >= datetime.now() - timedelta(days=LOOKBACK_DAYS + 1))
            if is_new:
                flags.append("New this week")

            score = 30
            score += 10 * len([f for f in flags if f != "New this week"])
            if combo:
                score += 20
            amt = r.get("amount") or 0
            if amt > 100_000:
                score += 15
            elif amt > 50_000:
                score += 10
            if is_new:
                score += 5
            if r.get("prop_address") or r.get("mail_address"):
                score += 5
            r["flags"] = flags
            r["score"] = max(0, min(100, score))
        except Exception as e:  # noqa: BLE001
            log.debug("score failed: %s", e)
            r["flags"], r["score"] = [], 30


# --------------------------------------------------------------------------- #
# 4) Output + GHL export
# --------------------------------------------------------------------------- #
RECORD_KEYS = [
    "doc_num", "doc_type", "filed", "cat", "cat_label", "owner", "grantee", "amount", "legal",
    "prop_address", "prop_city", "prop_state", "prop_zip",
    "mail_address", "mail_city", "mail_state", "mail_zip", "clerk_url", "flags", "score",
]


def write_json(path: Path, payload: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")
    tmp.replace(path)


def split_owner_name(owner: str) -> Tuple[str, str]:
    name = split_names(owner)[0] if split_names(owner) else ""
    if not name:
        return "", ""
    if is_corp(name):
        return "", name
    if "," in name:
        last, _, first = name.partition(",")
        return clean(first), clean(last)
    toks = name.split()
    if len(toks) == 1:
        return "", toks[0]
    if NAME_ORDER == "LAST_FIRST":
        return " ".join(toks[1:]), toks[0]
    return toks[0], toks[-1]


GHL_COLUMNS = [
    "First Name", "Last Name", "Mailing Address", "Mailing City", "Mailing State", "Mailing Zip",
    "Property Address", "Property City", "Property State", "Property Zip", "Lead Type",
    "Document Type", "Date Filed", "Document Number", "Amount/Debt Owed", "Seller Score",
    "Motivated Seller Flags", "Source", "Public Records URL",
]


def export_ghl_csv(records: List[Dict[str, Any]], path: Path = OUT_GHL) -> Path:
    """GoHighLevel-ready CSV import file."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8-sig") as fh:
        w = csv.writer(fh)
        w.writerow(GHL_COLUMNS)
        for r in records:
            try:
                first, last = split_owner_name(r.get("owner", ""))
                amt = r.get("amount")
                w.writerow(
                    [
                        first, last,
                        r.get("mail_address", ""), r.get("mail_city", ""), r.get("mail_state", ""), r.get("mail_zip", ""),
                        r.get("prop_address", ""), r.get("prop_city", ""), r.get("prop_state", ""), r.get("prop_zip", ""),
                        r.get("cat_label", ""), r.get("doc_type", ""), r.get("filed", ""), r.get("doc_num", ""),
                        f"{amt:.2f}" if isinstance(amt, (int, float)) else "",
                        r.get("score", ""), "; ".join(r.get("flags", [])), SOURCE, r.get("clerk_url", ""),
                    ]
                )
            except Exception as e:  # noqa: BLE001
                log.debug("csv row skipped: %s", e)
    log.info("GHL export: %s", path)
    return path


# --------------------------------------------------------------------------- #
# Main
# --------------------------------------------------------------------------- #
async def run() -> Dict[str, Any]:
    end = datetime.now()
    start = end - timedelta(days=LOOKBACK_DAYS)
    log.info("Jackson County MO scrape: %s -> %s", start.date(), end.date())

    records: List[Dict[str, Any]] = []
    try:
        raw = await scrape_clerk(start, end)
        log.info("raw clerk rows: %d", len(raw))
        records = build_clerk_records(raw, start, end)
        log.info("lead records after classification: %d", len(records))
    except Exception as e:  # noqa: BLE001
        log.error("clerk stage failed: %s", e)

    try:
        lookup = await asyncio.to_thread(load_parcel_lookup)
    except Exception as e:  # noqa: BLE001
        log.error("parcel stage failed: %s", e)
        lookup = {}
    enrich_with_parcels(records, lookup)
    apply_scores(records)
    records.sort(key=lambda r: (-r.get("score", 0), r.get("filed", "")), reverse=False)

    final = [{k: r.get(k, "" if k not in ("flags",) else []) for k in RECORD_KEYS} for r in records]
    with_addr = sum(1 for r in final if r["prop_address"] or r["mail_address"])
    payload = {
        "fetched_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "source": SOURCE,
        "date_range": f"{start:%Y-%m-%d} to {end:%Y-%m-%d}",
        "total": len(final),
        "with_address": with_addr,
        "records": final,
    }
    write_json(OUT_DASHBOARD, payload)
    write_json(OUT_DATA, payload)
    export_ghl_csv(final)
    log.info("done: %d records (%d with address)", len(final), with_addr)
    return payload


def main() -> int:
    try:
        asyncio.run(run())
    except Exception as e:  # noqa: BLE001
        log.exception("fatal: %s", e)
        # still leave valid (empty) output so the workflow/dashboard never break
        now = datetime.now(timezone.utc).isoformat(timespec="seconds")
        empty = {"fetched_at": now, "source": SOURCE, "date_range": "", "total": 0, "with_address": 0, "records": []}
        try:
            if not OUT_DASHBOARD.exists():
                write_json(OUT_DASHBOARD, empty)
            if not OUT_DATA.exists():
                write_json(OUT_DATA, empty)
        except Exception:  # noqa: BLE001
            pass
    return 0


if __name__ == "__main__":
    sys.exit(main())
