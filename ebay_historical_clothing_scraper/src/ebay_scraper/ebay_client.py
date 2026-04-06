from __future__ import annotations

import re
import time
from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from http.cookiejar import MozillaCookieJar
from pathlib import Path
from typing import Iterable
from urllib.parse import quote_plus

import requests
from bs4 import BeautifulSoup

from .config import Settings


ITEM_ID_PATTERNS = [
    re.compile(r"/itm/(?:[^/]+/)?(\d{8,})"),
    re.compile(r"item=(\d{8,})"),
]

# Placeholder hrefs in sponsored tiles
_IGNORE_ITEM_IDS = frozenset({"123456"})

SOLD_DATE_RE = re.compile(
    r"Sold\s*[:\s]*([A-Za-z]{3,9}\s+\d{1,2},\s+\d{4})",
    re.IGNORECASE,
)


class EbayAccessError(RuntimeError):
    """Raised when eBay serves a bot / access control page instead of results."""


def _navigation_headers(
    *,
    referer: str | None,
    sec_fetch_site: str,
) -> dict[str, str]:
    h: dict[str, str] = {
        "Accept": (
            "text/html,application/xhtml+xml,application/xml;q=0.9,"
            "image/avif,image/webp,image/apng,*/*;q=0.8"
        ),
        "Accept-Language": "en-US,en;q=0.9",
        "Cache-Control": "max-age=0",
        "Sec-CH-UA": (
            '"Google Chrome";v="131", "Chromium";v="131", "Not_A Brand";v="24"'
        ),
        "Sec-CH-UA-Mobile": "?0",
        "Sec-CH-UA-Platform": '"macOS"',
        "Sec-Fetch-Dest": "document",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-Site": sec_fetch_site,
        "Sec-Fetch-User": "?1",
        "Upgrade-Insecure-Requests": "1",
    }
    if referer:
        h["Referer"] = referer
    return h


def _load_netscape_cookies(session: requests.Session, path: Path) -> None:
    jar = MozillaCookieJar()
    jar.load(str(path), ignore_discard=True, ignore_expires=True)
    for cookie in jar:
        session.cookies.set(
            cookie.name,
            cookie.value,
            domain=cookie.domain,
            path=cookie.path or "/",
        )


_ACCESS_HINT = (
    "eBay returned a bot interstitial ('Pardon Our Interruption'), not search results.\n"
    "What often works: (1) In your browser, sign in to ebay.com and run a search, then "
    "export cookies as Netscape cookies.txt (e.g. 'Get cookies.txt LOCALLY' extension), "
    "save to data/ebay_cookies.txt, and set EBAY_COOKIES_FILE=data/ebay_cookies.txt in .env. "
    "(2) Increase SLEEP_BETWEEN_REQUESTS_SECONDS and try again later. "
    "(3) Run from the same network/IP you use for normal browsing."
)


@dataclass
class EbayListing:
    item_id: str
    query: str
    title: str
    price_text: str
    price_value: float | None
    shipping_text: str
    condition_text: str
    sold_date_text: str
    item_url: str
    page_number: int
    scraped_at_utc: str

    def to_row(self) -> dict:
        return asdict(self)


def _extract_item_id(url: str) -> str:
    for pattern in ITEM_ID_PATTERNS:
        match = pattern.search(url)
        if match:
            return match.group(1)
    return ""


def _is_bot_interstitial(html: str) -> bool:
    return "Pardon Our Interruption" in html


def _parse_price_value(price_text: str) -> float | None:
    match = re.search(r"[\d,.]+", price_text.replace(",", ""))
    if not match:
        return None
    try:
        return float(match.group(0))
    except ValueError:
        return None


class EbayClient:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self._session_warmed = False
        self.session = requests.Session()
        self.session.headers.update(
            {
                "User-Agent": (
                    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/131.0.0.0 Safari/537.36"
                ),
                "Accept-Language": "en-US,en;q=0.9",
            }
        )
        if settings.cookies_file is not None:
            path = settings.cookies_file
            if not path.is_file():
                raise FileNotFoundError(
                    f"EBAY_COOKIES_FILE is set but file does not exist: {path}"
                )
            _load_netscape_cookies(self.session, path)

    def build_search_url(self, query: str, page_number: int) -> str:
        encoded_query = quote_plus(query)
        return (
            f"https://{self.settings.ebay_domain}/sch/i.html"
            f"?_nkw={encoded_query}"
            f"&_sacat=11450"  # Clothing category anchor
            f"&LH_Sold=1&LH_Complete=1"
            f"&LH_ItemCondition=3"
            f"&_ipg=240"
            f"&_pgn={page_number}"
            f"&LH_PrefLoc={self.settings.ebay_location}"
        )

    def _warm_session(self) -> None:
        if self._session_warmed:
            return
        home = f"https://{self.settings.ebay_domain}/"
        self.session.get(
            home,
            timeout=self.settings.request_timeout_seconds,
            headers=_navigation_headers(referer=None, sec_fetch_site="none"),
        )
        pause = max(0.5, self.settings.sleep_between_requests_seconds / 2.0)
        time.sleep(pause)
        self._session_warmed = True

    def fetch_sold_listings(self, query: str) -> Iterable[EbayListing]:
        self._warm_session()
        home = f"https://{self.settings.ebay_domain}/"
        nav = _navigation_headers(referer=home, sec_fetch_site="same-origin")

        for page_number in range(1, self.settings.max_pages_per_query + 1):
            url = self.build_search_url(query=query, page_number=page_number)
            resp = self.session.get(
                url,
                timeout=self.settings.request_timeout_seconds,
                headers=nav,
            )
            resp.raise_for_status()
            if _is_bot_interstitial(resp.text):
                raise EbayAccessError(_ACCESS_HINT)

            listings = self._parse_result_page(
                html=resp.text, query=query, page_number=page_number
            )
            if not listings:
                break

            for listing in listings:
                yield listing

            time.sleep(self.settings.sleep_between_requests_seconds)

    def _parse_result_page(
        self, html: str, query: str, page_number: int
    ) -> list[EbayListing]:
        soup = BeautifulSoup(html, "html.parser")
        scraped_at_utc = datetime.now(timezone.utc).isoformat()
        legacy = self._parse_s_item_rows(
            soup, query=query, page_number=page_number, scraped_at_utc=scraped_at_utc
        )
        if legacy:
            return legacy
        return self._parse_s_card_rows(
            soup, query=query, page_number=page_number, scraped_at_utc=scraped_at_utc
        )

    def _parse_s_item_rows(
        self,
        soup: BeautifulSoup,
        query: str,
        page_number: int,
        scraped_at_utc: str,
    ) -> list[EbayListing]:
        results: list[EbayListing] = []
        for node in soup.select("li.s-item"):
            link = node.select_one("a.s-item__link")
            title_node = node.select_one(".s-item__title")
            price_node = node.select_one(".s-item__price")
            shipping_node = node.select_one(
                ".s-item__shipping, .s-item__logisticsCost"
            )
            condition_node = node.select_one(".SECONDARY_INFO")
            sold_date_node = node.select_one(
                ".s-item__title--tagblock, .POSITIVE"
            )

            if not link or not title_node or not price_node:
                continue

            item_url = link.get("href", "").strip()
            if not item_url:
                continue

            title = title_node.get_text(" ", strip=True)
            if not title or title.lower() == "shop on ebay":
                continue

            price_text = price_node.get_text(" ", strip=True)
            shipping_text = (
                shipping_node.get_text(" ", strip=True) if shipping_node else ""
            )
            condition_text = (
                condition_node.get_text(" ", strip=True) if condition_node else ""
            )
            sold_date_text = (
                sold_date_node.get_text(" ", strip=True) if sold_date_node else ""
            )
            item_id = _extract_item_id(item_url)
            if not item_id or item_id in _IGNORE_ITEM_IDS:
                continue

            results.append(
                EbayListing(
                    item_id=item_id,
                    query=query,
                    title=title,
                    price_text=price_text,
                    price_value=_parse_price_value(price_text),
                    shipping_text=shipping_text,
                    condition_text=condition_text,
                    sold_date_text=sold_date_text,
                    item_url=item_url,
                    page_number=page_number,
                    scraped_at_utc=scraped_at_utc,
                )
            )

        return results

    def _parse_s_card_rows(
        self,
        soup: BeautifulSoup,
        query: str,
        page_number: int,
        scraped_at_utc: str,
    ) -> list[EbayListing]:
        results: list[EbayListing] = []
        for node in soup.select("li.s-card"):
            link = node.select_one("a.s-card__link") or node.select_one(
                'a[href*="/itm/"]'
            )
            if not link:
                continue
            item_url = link.get("href", "").strip()
            item_id = _extract_item_id(item_url)
            if not item_id or item_id in _IGNORE_ITEM_IDS:
                continue

            title_node = node.select_one(".s-card__title")
            price_node = node.select_one(".s-card__price")
            if not title_node or not price_node:
                continue

            title = title_node.get_text(" ", strip=True)
            if not title or title.lower() == "shop on ebay":
                continue

            price_text = price_node.get_text(" ", strip=True)
            subtitle_node = node.select_one(".s-card__subtitle")
            shipping_text = ""
            for sel in (
                ".s-card__shipping",
                ".s-item__shipping",
                "[class*='logistic']",
            ):
                ship_el = node.select_one(sel)
                if ship_el and ship_el.get_text(strip=True):
                    shipping_text = ship_el.get_text(" ", strip=True)
                    break
            if not shipping_text:
                blob = node.get_text(" ", strip=True)
                m = re.search(
                    r"\+?\$[\d,.]+\s+(?:delivery|shipping)|"
                    r"\$[\d,.]+\s+delivery",
                    blob,
                    re.I,
                )
                if m:
                    shipping_text = m.group(0).strip()

            condition_text = (
                subtitle_node.get_text(" ", strip=True) if subtitle_node else ""
            )
            sold_date_node = node.select_one(
                ".s-item__title--tagblock, .POSITIVE, .s-card__tag"
            )
            sold_date_text = (
                sold_date_node.get_text(" ", strip=True) if sold_date_node else ""
            )
            if not sold_date_text:
                tag_match = SOLD_DATE_RE.search(node.get_text(" ", strip=True))
                if tag_match:
                    sold_date_text = f"Sold {tag_match.group(1)}"

            results.append(
                EbayListing(
                    item_id=item_id,
                    query=query,
                    title=title,
                    price_text=price_text,
                    price_value=_parse_price_value(price_text),
                    shipping_text=shipping_text,
                    condition_text=condition_text,
                    sold_date_text=sold_date_text,
                    item_url=item_url,
                    page_number=page_number,
                    scraped_at_utc=scraped_at_utc,
                )
            )

        return results
