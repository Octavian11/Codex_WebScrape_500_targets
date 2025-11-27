import argparse
import csv
import os
import re
import time
from dataclasses import dataclass
from typing import Callable, Dict, List
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

try:
    import pandas as pd  # noqa: F401  # optional dependency
except ImportError:
    pd = None
    # If pandas is unavailable, the script will fall back to pure CSV handling.


# ===============================
# CONFIG
# ===============================

# Brave Search API (free tier: 1 req/sec, 2000/mo)
SEARCH_API_KEY = "BSA6xlB-aAzqj3rwIAPMJzXfa9kURaO"
SEARCH_ENDPOINT = "https://api.search.brave.com/res/v1/web/search"
SEARCH_HEADERS = {
    "Accept": "application/json",
    "X-Subscription-Token": SEARCH_API_KEY,
}

# Curated search queries that target Devonshire’s niche
SEARCH_QUERIES = [
    # Financial-vertical MSPs
    "managed IT services for hedge funds",
    "hedge fund MSP New York",
    "hedge fund IT support NYC",
    "IT managed services for investment management firms",
    "managed IT services for broker dealers",
    "MSP for alternative investment firms",
    "MSP for asset management industry",
    "IT services for private equity firms",

    # Market data ops / admin
    "\"market data\" \"managed services\"",
    "\"market data operations\" consulting",
    "\"market data administration\" services",
    "\"market data\" \"vendor management\" firm",
    "\"market data\" \"exchange reporting\" services",
    "\"market data\" cost optimization consultancy",
    "\"market data\" inventory management consulting",

    # Trading infrastructure MSPs
    "\"trading infrastructure\" \"managed services\"",
    "\"low latency\" \"trading\" \"managed\"",
    "\"ultra low latency\" \"trading\" \"infrastructure\"",
    "\"exchange connectivity\" \"managed\"",
    "\"colocation\" \"trading\" \"managed services\"",

    # OMS/EMS/FIX services
    "\"OMS implementation\" buy side consulting",
    "\"EMS implementation\" trading",
    "\"FIX connectivity\" consulting firm",
    "\"FIX onboarding\" services",
    "\"trading platform\" implementation consulting",
    "\"order management system\" integration partner",

    # Reg-ops / compliance ops
    "\"outsourced trade surveillance\"",
    "\"managed\" \"trade surveillance\" services",
    "\"outsourced compliance\" \"broker dealer\"",
    "\"outsourced CCO\" services",
    "\"best execution\" \"outsourced\" testing",
    "\"regulatory reporting\" managed services",
]


# ===============================
# UTILITIES
# ===============================

def safe_get(url: str, headers: Dict = None, timeout: int = 15, params: Dict = None) -> requests.Response:
    """Wrapper around requests.get with basic exception handling and params support."""
    h = headers or {}
    resp = requests.get(url, headers=h, timeout=timeout, params=params)
    resp.raise_for_status()
    return resp


def domain_from_url(url: str) -> str:
    """Extract domain from URL."""
    try:
        return url.split("/")[2].lower()
    except IndexError:
        return url.lower()


def clean_result_name(title: str, domain_fallback: str) -> str:
    """
    Strip common site-brand suffixes. Prefer returning the brand segment if it
    looks like a firm name (keeps the company, drops the marketing headline).
    Examples:
        "Managed IT Services ... | Linedata" -> "Linedata"
        "Hedge Fund Cybersecurity – Omega Systems" -> "Omega Systems"
        "NextGen IT Services For Hedge Funds | Managed IT Services" -> "Managed IT Services"
    """
    if not title:
        return domain_fallback

    # Normalize separators
    separators = [r"\|", r"–", r"—", r"-", r":"]
    pattern = r"\s*(?:%s)\s*" % "|".join(separators)
    parts = re.split(pattern, title)
    parts = [p.strip() for p in parts if p.strip()]

    if not parts:
        return title.strip() or domain_fallback

    base = parts[0]
    brand = parts[-1] if len(parts) > 1 else ""

    marketing_words = {
        "managed", "services", "service", "cloud", "it", "support", "cybersecurity",
        "security", "hedge", "fund", "trading", "infrastructure", "platform",
        "managed services", "consulting", "implementation", "solutions"
    }

    def looks_like_marketing(text: str) -> bool:
        lower = text.lower()
        return any(word in lower for word in marketing_words)

    # If brand exists and is not marketing-heavy, prefer returning just the brand.
    if brand and len(brand.split()) <= 6 and not looks_like_marketing(brand):
        return brand

    # If base is marketing but brand exists, still prefer brand.
    if brand and looks_like_marketing(base) and not looks_like_marketing(brand):
        return brand

    # Otherwise return base (trimmed) as fallback.
    return base or domain_fallback


def ensure_dir(path: str) -> None:
    """Ensure parent directory exists for a given file path."""
    directory = os.path.dirname(path)
    if directory:
        os.makedirs(directory, exist_ok=True)


# ===============================
# DISCOVERY – WEB SEARCH
# ===============================

def search_web(query: str, count: int = 20, offset: int = 0) -> List[dict]:
    """
    Uses Brave Search API to execute a web search.
    Returns a list of result dicts (title, url, description).
    """
    params = {
        "q": query,
        "count": count,
        "offset": offset,
        "search_lang": "en",
        "country": "us",
    }
    resp = safe_get(SEARCH_ENDPOINT, headers=SEARCH_HEADERS, timeout=15, params=params)
    data = resp.json()
    return data.get("web", {}).get("results", [])


def run_search_discovery(output_csv: str = "data/firms_raw_search.csv", max_rows: int = None) -> None:
    """
    Executes search-based discovery and writes unique website rows into CSV.
    Schema:
        Name, Website, HQ, Category, Fit (Core/Stretch), Notes, Source, Conference, Classification
    """
    ensure_dir(output_csv)
    seen_domains = set()
    rows = []
    total_rows = 0

    for q in SEARCH_QUERIES:
        print(f"[SEARCH] Query: {q}")
        try:
            results = search_web(q, count=20)
        except requests.HTTPError as e:
            body = ""
            if e.response is not None:
                try:
                    body = e.response.text[:500]
                except Exception:
                    body = ""
            print(f"[!] Search error for query '{q}': {e} {body}")
            continue
        except Exception as e:
            print(f"[!] Search error for query '{q}': {e}")
            continue

        for r in results:
            url = r.get("url")
            if not url:
                continue
            domain = domain_from_url(url)
            name = clean_result_name(r.get("title"), domain)
            snippet = r.get("description", "")

            # Basic filters to avoid noise
            if any(skip in domain for skip in [
                "linkedin.com", "indeed.com", "glassdoor.com", "facebook.com",
                "twitter.com", "youtube.com", "wikipedia.org"
            ]):
                continue
            if domain in seen_domains:
                continue

            seen_domains.add(domain)
            total_rows += 1
            rows.append({
                "Name": name or domain,
                "Website": f"https://{domain}",
                "HQ": "",
                "Category": "",
                "Fit (Core/Stretch)": "",
                "Notes": snippet,
                "Source": f"search:{q}",
                "Conference": "",
                "Classification": "",
            })

            if total_rows % 25 == 0:
                print("[SEARCH] Reached 25-row batch; sleeping 30s for throttling...")
                time.sleep(30)

            if max_rows and total_rows >= max_rows:
                print(f"[SEARCH] Reached max_rows={max_rows}; stopping search discovery.")
                break

        if max_rows and total_rows >= max_rows:
            break

        print("[SEARCH] Sleeping 5s between queries for rate limiting...")
        time.sleep(5)

    if not rows:
        print("[SEARCH] No rows discovered.")
        return

    fieldnames = [
        "Name", "Website", "HQ", "Category", "Fit (Core/Stretch)",
        "Notes", "Source", "Conference", "Classification"
    ]
    with open(output_csv, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    print(f"[SEARCH] Wrote {len(rows)} rows to {output_csv}")


# ===============================
# DISCOVERY – CONFERENCES
# ===============================

@dataclass
class ConferenceConfig:
    name: str                # e.g., "FIA_Expo_2024"
    url: str                 # exhibitor listing URL
    parser: Callable         # function(html: str, conf_name: str) -> List[dict]


def fetch_html(url: str) -> str:
    # Use a browsery UA for sites that gate default clients.
    resp = safe_get(url, headers={"User-Agent": "Mozilla/5.0"})
    return resp.text


def parse_fia_expo(html: str, conf_name: str) -> List[dict]:
    """
    Parser for FIA Expo sponsor/exhibitor list:
    https://s7.goeshow.com/fia/expo/2024/sponsor_exhibitor_list.cfm
    """
    soup = BeautifulSoup(html, "html.parser")
    rows = []

    table = soup.select_one("table#exh_list tbody")
    if not table:
        return rows

    for tr in table.select("tr"):
        tds = tr.select("td")
        if len(tds) < 2:
            continue
        booth = tds[0].get_text(strip=True)
        name_cell = tds[1]
        name = name_cell.get_text(" ", strip=True)
        # Attempt to construct a profile URL from href or onclick
        profile_url = ""
        link = name_cell.find("a")
        if link:
            href = link.get("href", "")
            onclick = link.get("onclick", "")
            if href and href.startswith("http"):
                profile_url = href
            elif href and "profile.cfm" in href:
                profile_url = urljoin("https://s7.goeshow.com/fia/expo/2024/", href)
            elif onclick and "profile.cfm" in onclick:
                m = re.search(r"ExhibitorPopup\\('([^']+)", onclick)
                if m:
                    profile_url = urljoin("https://s7.goeshow.com/fia/expo/2024/", m.group(1))

        if not name:
            continue
        rows.append({
            "Name": name,
            "Website": profile_url,
            "HQ": "",
            "Category": "",
            "Fit (Core/Stretch)": "",
            "Notes": f"Booth: {booth}" if booth else "",
            "Source": f"conf:{conf_name}",
            "Conference": conf_name,
            "Classification": "",
        })

    return rows


def parse_tradetech_fx(html: str, conf_name: str) -> List[dict]:
    """
    Parser for TradeTech FX USA sponsors/exhibitors.
    Target URL (2025 sponsors page):
        https://tradetechfxus.wbresearch.com/sponsors/2025
    """
    soup = BeautifulSoup(html, "html.parser")
    rows: List[dict] = []

    # The page uses rows with the "sponsor" class for each sponsor entry.
    cards = soup.select("div.sponsor")

    for c in cards:
        name_el = c.select_one("h3")
        link_el = c.find("a", href=True)
        desc_el = c.select_one(".description")

        if not name_el:
            continue

        name = name_el.get_text(strip=True)

        url = ""
        if link_el and link_el.get("href"):
            href = link_el["href"]
            if href.startswith("http"):
                url = href
            elif href.startswith("www"):
                url = f"https://{href}"

        notes = desc_el.get_text(" ", strip=True) if desc_el else ""

        rows.append({
            "Name": name,
            "Website": url,
            "HQ": "",
            "Category": "",
            "Fit (Core/Stretch)": "",
            "Notes": notes,
            "Source": f"conf:{conf_name}",
            "Conference": conf_name,
            "Classification": "",
        })

    return rows


def run_conference_scrape(output_csv: str = "data/firms_raw_conferences.csv", max_rows: int = None) -> None:
    """
    Scrapes multiple conference exhibitor lists and writes them to CSV.

    Currently automated:
        - FIA Expo 2024 (sponsor & exhibitor list)
        - TradeTech FX USA 2025 (sponsors)

    SIFMA Ops and Advent Connect do NOT expose public HTML exhibitor lists
    suitable for scraping, so they are intentionally handled manually.
    """
    ensure_dir(output_csv)

    conferences: List[ConferenceConfig] = [
        ConferenceConfig(
            name="FIA_Expo_2024",
            url="https://s7.goeshow.com/fia/expo/2024/sponsor_exhibitor_list.cfm",
            parser=parse_fia_expo,
        ),
        ConferenceConfig(
            name="TradeTech_FX_USA_2025",
            url="https://tradetechfxus.wbresearch.com/sponsors/2025",
            parser=parse_tradetech_fx,
        ),
        # NOTE: SIFMA Ops and Advent Connect are omitted because there is no
        # stable public HTML exhibitor directory to scrape. Treat them as manual
        # sourcing channels instead.
    ]

    all_rows: List[dict] = []

    for conf in conferences:
        print(f"[CONF] Scraping {conf.name} from {conf.url}")
        try:
            html = fetch_html(conf.url)
            rows = conf.parser(html, conf.name)
            all_rows.extend(rows)
            print(f"[CONF] {conf.name}: {len(rows)} exhibitors scraped")
            if max_rows and len(all_rows) >= max_rows:
                print(f"[CONF] Reached max_rows={max_rows}; stopping conference scrape.")
                break
        except Exception as e:
            print(f"[!] Error scraping {conf.name}: {e}")
        if max_rows and len(all_rows) >= max_rows:
            break

    if max_rows and len(all_rows) > max_rows:
        all_rows = all_rows[:max_rows]

    if not all_rows:
        print("[CONF] No exhibitors scraped; check URLs and selectors.")
        return

    fieldnames = [
        "Name", "Website", "HQ", "Category", "Fit (Core/Stretch)",
        "Notes", "Source", "Conference", "Classification"
    ]
    with open(output_csv, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(all_rows)

    print(f"[CONF] Wrote {len(all_rows)} rows to {output_csv}")


# ===============================
# ENRICHMENT & CLASSIFICATION
# ===============================

# Keyword sets to classify categories
FINANCE_KEYWORDS = [
    "hedge fund", "alternative investment", "asset management",
    "broker-dealer", "broker dealer", "investment manager",
    "private equity", "family office", "trading firm", "proprietary trading",
]

MSP_KEYWORDS = [
    "managed services", "managed it", "outsourced", "24x7 support",
    "24/7 support", "service level agreement", "sla", "help desk", "monitoring",
]

MARKET_DATA_KEYWORDS = [
    "market data", "market-data", "data feeds", "entitlements",
    "exchange reporting", "vendor management", "dacs", "inventory",
]

TRADING_INFRA_KEYWORDS = [
    "trading infrastructure", "low latency", "low-latency", "colocation",
    "proximity hosting", "exchange connectivity", "fix connectivity",
]

OMS_EMS_KEYWORDS = [
    "oms", "order management system", "ems", "execution management system",
    "trading platform implementation", "trading system implementation",
    "fix onboarding",
]

REG_OPS_KEYWORDS = [
    "trade surveillance", "best execution", "best-execution",
    "outsourced compliance", "regulatory reporting", "finra", "sec rule",
]

# Known overrides for Financial MSPs (force category/fit)
KNOWN_FINANCIAL_MSPS = {
    "ceutechnologies.com",
    "omegasystemscorp.com",
    "thrivenextgen.com",
    "aag-it.com",
    "silverlinetech.com",
    "eze-castle.com",
}

# Known giants/competitors to tag as comps
KNOWN_COMP_NAMES = {
    "bloomberg",
    "eurex",
    "cme group",
}


def fetch_site_text(url: str) -> str:
    try:
        resp = safe_get(url, timeout=10)
    except Exception:
        return ""
    soup = BeautifulSoup(resp.text, "html.parser")
    text_parts = []
    # Include footer/address to surface location hints.
    for t in soup.find_all(["p", "li", "h1", "h2", "h3", "footer", "address"]):
        text_parts.append(t.get_text(separator=" ", strip=True))
    return " ".join(text_parts).lower()


def classify_category(text: str) -> str:
    # Count keyword hits instead of just "any"
    finance_hits = sum(1 for k in FINANCE_KEYWORDS if k in text)
    msp_hits = sum(1 for k in MSP_KEYWORDS if k in text)
    trading_hits = sum(1 for k in TRADING_INFRA_KEYWORDS if k in text)
    md_hits = sum(1 for k in MARKET_DATA_KEYWORDS if k in text)
    oms_hits = sum(1 for k in OMS_EMS_KEYWORDS if k in text)
    reg_hits = sum(1 for k in REG_OPS_KEYWORDS if k in text)

    score = {
        "Financial MSP": 0,
        "Trading Infra MSP": 0,
        "Market Data Ops": 0,
        "OMS/EMS & FIX": 0,
        "RegOps / Surveillance": 0,
        "Generic IT": 0,
    }

    # --- Financial MSP ---
    # Finance + MSP together is the classic hedge-fund/PE MSP pattern.
    if finance_hits > 0 and msp_hits > 0:
        score["Financial MSP"] += finance_hits * 3 + msp_hits * 2
    elif finance_hits > 0:
        score["Financial MSP"] += finance_hits * 2
    elif msp_hits > 0:
        # MSP but no explicit finance – light score, may end up as Generic IT
        score["Financial MSP"] += msp_hits

    # --- Trading Infra MSP ---
    if trading_hits > 0:
        score["Trading Infra MSP"] += trading_hits * 3

    # --- Market Data Ops ---
    if md_hits > 0:
        score["Market Data Ops"] += md_hits * 3

    # --- OMS/EMS & FIX ---
    # Be stricter: require at least 2 hits, or 1 hit + strong trading/FX language.
    if oms_hits >= 2:
        score["OMS/EMS & FIX"] += oms_hits * 3
    elif oms_hits == 1 and (trading_hits > 0 or md_hits > 0 or "fx" in text):
        score["OMS/EMS & FIX"] += 3

    # --- RegOps / Surveillance ---
    if reg_hits > 0:
        score["RegOps / Surveillance"] += reg_hits * 3

    # --- Generic IT ---
    # Only use Generic IT when it looks like a plain MSP (msp_hits) with no clear niche.
    if (
        msp_hits > 0
        and finance_hits == 0
        and trading_hits == 0
        and md_hits == 0
        and oms_hits == 0
        and reg_hits == 0
    ):
        score["Generic IT"] = 1

    # Pick best scoring category
    cat = max(score, key=score.get)

    # If everything is zero, fall back to Generic IT
    if score[cat] == 0:
        cat = "Generic IT"

    # Final override: if finance + MSP signals are strong, don’t let a stray OMS/EMS hit steal it
    if finance_hits > 0 and msp_hits > 0 and score["Financial MSP"] >= score[cat]:
        cat = "Financial MSP"

    return cat


def guess_hq(text: str) -> str:
    """
    Rough HQ guesser that looks for:
      1) Explicit 'headquartered in' / 'based in' phrases
      2) Known city/region hub names
      3) Generic 'City, ST [ZIP]' patterns
    """
    t = text.lower()

    # 1) Explicit "Headquartered in ..." or "Based in ..."
    hq_patterns = [
        r"headquartered in ([a-z ,]+,\s*[a-z]{2})",
        r"based in ([a-z ,]+,\s*[a-z]{2})",
        r"headquarters in ([a-z ,]+,\s*[a-z]{2})",
    ]
    for pat in hq_patterns:
        m = re.search(pat, t)
        if m:
            loc = m.group(1).strip()
            parts = [p.strip() for p in loc.split(",")]
            if len(parts) == 2:
                city = parts[0].title()
                st = parts[1].upper()
                return f"{city}, {st}"
            return loc.title()

    # 2) Known hubs (removed overly-generic 'la')
    hub_map = [
        (["new york", "nyc"], "New York, NY"),
        (["brooklyn"], "Brooklyn, NY"),
        (["jersey city"], "Jersey City, NJ"),
        (["stamford", "greenwich", "norwalk"], "Fairfield County, CT"),
        (["boston"], "Boston, MA"),
        (["philadelphia"], "Philadelphia, PA"),
        (["chicago"], "Chicago, IL"),
        (["miami", "orlando", "tampa"], "Florida"),
        (["dallas", "austin", "houston"], "Texas"),
        (["san francisco"], "San Francisco, CA"),
        (["los angeles"], "Los Angeles, CA"),
        (["san jose", "palo alto", "silicon valley"], "Bay Area, CA"),
        (["seattle"], "Seattle, WA"),
        (["denver", "boulder"], "Colorado"),
        (["charlotte"], "Charlotte, NC"),
        (["washington dc", "washington d.c.", "washington, dc"], "Washington, DC"),
        # International hubs
        (["london"], "London, UK"),
        (["toronto"], "Toronto, Canada"),
        (["montreal"], "Montreal, Canada"),
        (["vancouver"], "Vancouver, Canada"),
        (["singapore"], "Singapore"),
        (["hong kong"], "Hong Kong"),
    ]

    for keys, label in hub_map:
        if any(k in t for k in keys):
            return label

    # 3) Generic US pattern: "City, ST [ZIP]"
    states = (
        "al|ak|az|ar|ca|co|ct|de|fl|ga|hi|id|il|in|ia|ks|ky|la|me|md|ma|mi|mn|ms|mo|mt|"
        "ne|nv|nh|nj|nm|ny|nc|nd|oh|ok|or|pa|ri|sc|sd|tn|tx|ut|vt|va|wa|wv|wi|wy"
    )
    generic_us = rf"\b([a-z][a-z .]{{2,40}}),\s*({states})(?:\s+\d{{5}})?\b"
    m = re.search(generic_us, t)
    if m:
        city = m.group(1).strip().title()
        st = m.group(2).upper()
        return f"{city}, {st}"

    return ""


def classify_fit(category: str) -> str:
    """
    Simple rule: non-Generic IT categories are Core, Generic IT is Stretch.
    """
    if category == "Generic IT":
        return "Stretch"
    return "Core"


def classify_firm(row: dict) -> str:
    """
    High-level classification:
    - Potential acquisition target (near-term) if Core & in key lanes
    - Otherwise Pattern / Comp / Future Partner
    """
    core_lanes = {
        "Financial MSP",
        "Trading Infra MSP",
        "Market Data Ops",
        "OMS/EMS & FIX",
        "RegOps / Surveillance",
    }
    if row.get("Fit (Core/Stretch)") == "Core" and row.get("Category") in core_lanes:
        return "Potential acquisition target (near-term)"
    return "Pattern / Comp / Future Partner"


def normalize_hq(hq: str) -> str:
    """Normalize HQ string casing and common country/state tokens."""
    if not hq:
        return ""
    hq = hq.strip()
    # Normalize UK
    hq = hq.replace("Uk", "UK").replace("uk", "UK")
    # Title-case city part, upper-case 2-letter state if present
    parts = [p.strip() for p in hq.split(",")]
    if len(parts) == 2:
        city, state = parts
        city = city.title()
        state = state.upper()
        return f"{city}, {state}"
    return hq.title()


def enrich(input_csv: str, output_csv: str) -> None:
    """
    Enriches firms:
        - Fetches website text
        - Classifies category
        - Guesses HQ (very rough)
        - Assigns Fit and Classification

    Assumes input CSV has:
        Name, Website, HQ, Category, Fit (Core/Stretch), Notes, Source, Conference, Classification
    """
    ensure_dir(output_csv)
    rows_out: List[dict] = []

    with open(input_csv, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            url = row.get("Website", "").strip()
            if not url:
                continue

            domain = domain_from_url(url)

            print(f"[ENRICH] Fetching {url}")
            text = fetch_site_text(url)
            category = classify_category(text)
            fit = classify_fit(category)

            hq = row.get("HQ", "").strip()
            if not hq:
                hq_guess = guess_hq(text)
                if hq_guess:
                    hq = hq_guess

            # Domain-based overrides for known financial MSPs
            if domain in KNOWN_FINANCIAL_MSPS:
                category = "Financial MSP"
                fit = "Core"

            row["Category"] = category
            row["Fit (Core/Stretch)"] = fit
            row["HQ"] = normalize_hq(hq)
            name_l = row.get("Name", "").strip().lower()
            if name_l in KNOWN_COMP_NAMES:
                row["Classification"] = "Pattern / Comp / Future Partner"
            else:
                row["Classification"] = classify_firm(row)

            rows_out.append(row)

    if not rows_out:
        print("[ENRICH] No rows to write.")
        return

    fieldnames = [
        "Name", "Website", "HQ", "Category", "Fit (Core/Stretch)",
        "Notes", "Source", "Conference", "Classification"
    ]
    with open(output_csv, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows_out)

    print(f"[ENRICH] Wrote {len(rows_out)} enriched rows to {output_csv}")


# ===============================
# MERGING SOURCES
# ===============================

def merge_raw_sources(
    search_csv: str = "data/firms_raw_search.csv",
    conf_csv: str = "data/firms_raw_conferences.csv",
    output_csv: str = "data/firms_raw_all.csv",
    max_total_rows: int = None,
) -> None:
    """
    Merge search-discovered and conference-discovered firms into one CSV.
    Deduplicate on Website. Uses pandas if available, otherwise falls back to CSV.
    """
    ensure_dir(output_csv)
    fieldnames = [
        "Name", "Website", "HQ", "Category", "Fit (Core/Stretch)",
        "Notes", "Source", "Conference", "Classification"
    ]

    if pd is not None:
        frames = []
        for path in (search_csv, conf_csv):
            if not os.path.exists(path):
                print(f"[MERGE] File not found, skipping: {path}")
                continue
            df = pd.read_csv(path, dtype=str).fillna("")
            frames.append(df)

        if not frames:
            print("[MERGE] No rows to merge.")
            return

        merged = pd.concat(frames, ignore_index=True)
        if "Website" not in merged.columns:
            merged["Website"] = ""

        merged["website_norm"] = merged["Website"].astype(str).str.strip().str.lower()

        for col in fieldnames:
            if col not in merged.columns:
                merged[col] = ""

        with_site = merged[merged["website_norm"] != ""].copy()
        without_site = merged[merged["website_norm"] == ""].copy()

        with_site = with_site.drop_duplicates(subset=["website_norm"])
        without_site = without_site.drop_duplicates(subset=["Name"])

        merged = pd.concat([with_site, without_site], ignore_index=True)
        merged = merged[fieldnames + ["website_norm"]]
        merged = merged.drop(columns=["website_norm"])

        if max_total_rows is not None and max_total_rows > 0 and len(merged) > max_total_rows:
            merged = merged.head(max_total_rows)
            print(f"[MERGE] Truncated merged rows to {max_total_rows} per max_total_rows limit.")

        merged.to_csv(output_csv, index=False, encoding="utf-8")
        print(f"[MERGE] Merged {len(merged)} unique firms into {output_csv}")
        return

    # Fallback to pure-CSV handling if pandas is unavailable
    all_rows: List[dict] = []
    websites_seen = set()

    def load(path: str):
        try:
            with open(path, newline="", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                for r in reader:
                    w = r.get("Website", "").strip().lower()
                    if not w:
                        continue
                    if w in websites_seen:
                        continue
                    websites_seen.add(w)
                    all_rows.append(r)
        except FileNotFoundError:
            print(f"[MERGE] File not found, skipping: {path}")

    load(search_csv)
    load(conf_csv)

    if not all_rows:
        print("[MERGE] No rows to merge.")
        return

    if max_total_rows is not None and max_total_rows > 0 and len(all_rows) > max_total_rows:
        all_rows = all_rows[:max_total_rows]
        print(f"[MERGE] Truncated merged rows to {max_total_rows} per max_total_rows limit.")

    with open(output_csv, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(all_rows)

    print(f"[MERGE] Merged {len(all_rows)} unique firms into {output_csv}")


# ===============================
# MAIN ORCHESTRATION
# ===============================

def main():
    """
    High-level flow:
        1) Discover via search -> data/firms_raw_search.csv
        2) Discover via conferences -> data/firms_raw_conferences.csv
        3) Merge -> data/firms_raw_all.csv
        4) Enrich -> data/firms_enriched.csv
    """
    parser = argparse.ArgumentParser(description="Discover, merge, and enrich firms.")
    parser.add_argument("--max-search-rows", type=int, default=10,
                        help="Limit number of rows collected from web search (<=0 for unlimited).")
    parser.add_argument("--max-conf-rows", type=int, default=10,
                        help="Limit number of rows collected from conference scraping (<=0 for unlimited).")
    parser.add_argument("--max-total-rows", type=int, default=None,
                        help="Cap total merged rows before enrichment (<=0 for unlimited).")
    parser.add_argument("--skip-search", action="store_true",
                        help="Skip search-based discovery.")
    parser.add_argument("--skip-conferences", action="store_true",
                        help="Skip conference scraping.")
    parser.add_argument("--skip-enrich", action="store_true",
                        help="Skip enrichment step.")
    args = parser.parse_args()

    max_search = None if (args.max_search_rows is not None and args.max_search_rows <= 0) else args.max_search_rows
    max_conf = None if (args.max_conf_rows is not None and args.max_conf_rows <= 0) else args.max_conf_rows

    if not args.skip_search:
        run_search_discovery(
            output_csv="data/firms_raw_search.csv",
            max_rows=max_search,
        )

    if not args.skip_conferences:
        run_conference_scrape(
            output_csv="data/firms_raw_conferences.csv",
            max_rows=max_conf,
        )

    merge_raw_sources(
        search_csv="data/firms_raw_search.csv",
        conf_csv="data/firms_raw_conferences.csv",
        output_csv="data/firms_raw_all.csv",
        max_total_rows=None if (args.max_total_rows is not None and args.max_total_rows <= 0) else args.max_total_rows,
    )

    if not args.skip_enrich:
        enrich(input_csv="data/firms_raw_all.csv", output_csv="data/firms_enriched.csv")


if __name__ == "__main__":
    main()
