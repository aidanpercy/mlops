"""Utilities for building raw, bronze, and silver sold-listing datasets."""

from __future__ import annotations

import json
import random
import re
import time
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import pandas as pd
import requests
from bs4 import BeautifulSoup

PARSER_VERSION = "v1"
DATA_ROOT = Path("data")
RAW_DIR = DATA_ROOT / "raw"
BRONZE_DIR = DATA_ROOT / "bronze"
SILVER_DIR = DATA_ROOT / "silver"


@dataclass(frozen=True)
class SoldListingRecord:
    listing_id: str
    source_url: str
    scrape_timestamp: str
    parser_version: str
    sold_date: str
    title: str
    brand: str
    category: str
    size: str
    condition: str
    color: str
    material: str
    listing_price: float
    shipping_price: float
    sale_price: float
    seller_feedback: int


def ensure_data_dirs(base_dir: Path = DATA_ROOT) -> dict[str, Path]:
    raw_dir = base_dir / "raw"
    bronze_dir = base_dir / "bronze"
    silver_dir = base_dir / "silver"
    raw_dir.mkdir(parents=True, exist_ok=True)
    bronze_dir.mkdir(parents=True, exist_ok=True)
    silver_dir.mkdir(parents=True, exist_ok=True)
    return {"raw": raw_dir, "bronze": bronze_dir, "silver": silver_dir}


def generate_sample_records(seed: int = 42) -> list[SoldListingRecord]:
    rng = random.Random(seed)
    brands = {
        "Patagonia": ("Jacket", "Blue", "Polyester", 120),
        "Arc'teryx": ("Shell", "Black", "Nylon", 210),
        "Levi's": ("Jeans", "Blue", "Denim", 65),
        "Lululemon": ("Leggings", "Black", "Nylon", 85),
        "Nike": ("Sneakers", "White", "Leather", 110),
        "Theory": ("Blazer", "Gray", "Wool", 145),
    }
    sizes = ["XS", "S", "M", "L", "XL"]
    conditions = ["new", "used_good", "used_very_good"]
    sold_dates = pd.date_range("2025-01-01", periods=180, freq="D")

    records: list[SoldListingRecord] = []
    listing_counter = 100000
    for sold_date in sold_dates:
        for brand, (category, base_color, material, base_price) in brands.items():
            for size in sizes[:3]:
                condition = rng.choice(conditions)
                color = base_color if rng.random() < 0.7 else rng.choice(
                    ["Black", "Blue", "Gray", "Green", "White"]
                )
                shipping_price = round(rng.uniform(0, 18), 2)
                condition_delta = {
                    "new": 1.15,
                    "used_very_good": 1.0,
                    "used_good": 0.88,
                }[condition]
                size_delta = {"XS": 0.95, "S": 0.98, "M": 1.0, "L": 1.03, "XL": 1.05}[size]
                noise = rng.uniform(-10, 10)
                listing_price = round(base_price * size_delta * condition_delta + noise, 2)
                sale_price = round(listing_price * rng.uniform(0.84, 1.08), 2)
                source_url = f"https://example.com/listing/{listing_counter}"
                title = f"{brand} {color} {category} size {size}"
                records.append(
                    SoldListingRecord(
                        listing_id=str(listing_counter),
                        source_url=source_url,
                        scrape_timestamp=f"{sold_date.date()}T12:00:00Z",
                        parser_version=PARSER_VERSION,
                        sold_date=str(sold_date.date()),
                        title=title,
                        brand=brand,
                        category=category,
                        size=size,
                        condition=condition,
                        color=color,
                        material=material,
                        listing_price=max(listing_price, 10.0),
                        shipping_price=shipping_price,
                        sale_price=max(sale_price, 8.0),
                        seller_feedback=rng.randint(90, 5000),
                    )
                )
                listing_counter += 1
    return records


def render_listing_html(record: SoldListingRecord) -> str:
    return f"""<!doctype html>
<html lang="en">
  <body>
    <article class="sold-listing" data-listing-id="{record.listing_id}">
      <a class="listing-url" href="{record.source_url}">{record.title}</a>
      <span class="sold-date">{record.sold_date}</span>
      <span class="scrape-timestamp">{record.scrape_timestamp}</span>
      <span class="brand">{record.brand}</span>
      <span class="category">{record.category}</span>
      <span class="size">{record.size}</span>
      <span class="condition">{record.condition}</span>
      <span class="color">{record.color}</span>
      <span class="material">{record.material}</span>
      <span class="listing-price">{record.listing_price:.2f}</span>
      <span class="shipping-price">{record.shipping_price:.2f}</span>
      <span class="sale-price">{record.sale_price:.2f}</span>
      <span class="seller-feedback">{record.seller_feedback}</span>
    </article>
  </body>
</html>
"""


def write_raw_html(records: list[SoldListingRecord], raw_dir: Path) -> Path:
    manifest_rows: list[dict[str, Any]] = []
    for record in records:
        path = raw_dir / f"{record.listing_id}.html"
        path.write_text(render_listing_html(record), encoding="utf-8")
        manifest_rows.append(
            {
                "listing_id": record.listing_id,
                "source_url": record.source_url,
                "raw_path": str(path),
                "scrape_timestamp": record.scrape_timestamp,
            }
        )
    manifest_path = raw_dir / "manifest.csv"
    pd.DataFrame(manifest_rows).to_csv(manifest_path, index=False)
    return manifest_path


def parse_raw_html(raw_dir: Path, bronze_dir: Path) -> tuple[Path, pd.DataFrame]:
    rows: list[dict[str, Any]] = []
    for html_path in sorted(raw_dir.glob("*.html")):
        soup = BeautifulSoup(html_path.read_text(encoding="utf-8"), "html.parser")
        article = soup.select_one("article.sold-listing")
        if article is None:
            continue
        rows.append(
            {
                "listing_id": article["data-listing-id"],
                "source_url": article.select_one(".listing-url")["href"],
                "title": article.select_one(".listing-url").get_text(strip=True),
                "sold_date": article.select_one(".sold-date").get_text(strip=True),
                "scrape_timestamp": article.select_one(".scrape-timestamp").get_text(strip=True),
                "parser_version": PARSER_VERSION,
                "brand": article.select_one(".brand").get_text(strip=True),
                "category": article.select_one(".category").get_text(strip=True),
                "size": article.select_one(".size").get_text(strip=True),
                "condition": article.select_one(".condition").get_text(strip=True),
                "color": article.select_one(".color").get_text(strip=True),
                "material": article.select_one(".material").get_text(strip=True),
                "listing_price": float(article.select_one(".listing-price").get_text(strip=True)),
                "shipping_price": float(article.select_one(".shipping-price").get_text(strip=True)),
                "sale_price": float(article.select_one(".sale-price").get_text(strip=True)),
                "seller_feedback": int(article.select_one(".seller-feedback").get_text(strip=True)),
                "raw_path": str(html_path),
            }
        )

    bronze_df = pd.DataFrame(rows).sort_values("listing_id").drop_duplicates("listing_id")
    bronze_path = bronze_dir / "sold_listings_bronze.csv"
    bronze_df.to_csv(bronze_path, index=False)
    return bronze_path, bronze_df


def build_silver_dataset(bronze_df: pd.DataFrame, silver_dir: Path) -> tuple[Path, pd.DataFrame]:
    silver_df = bronze_df.copy()
    silver_df["total_cost"] = silver_df["listing_price"] + silver_df["shipping_price"]
    silver_df["sale_minus_listing"] = silver_df["sale_price"] - silver_df["listing_price"]
    silver_df["sale_ratio"] = silver_df["sale_price"] / silver_df["listing_price"]
    silver_df["is_new"] = (silver_df["condition"] == "new").astype(int)
    silver_df["sold_month"] = pd.to_datetime(silver_df["sold_date"]).dt.strftime("%Y-%m")
    silver_path = silver_dir / "sold_listings_training.csv"
    silver_df.to_csv(silver_path, index=False)
    return silver_path, silver_df


def build_dataset_artifacts(base_dir: Path = DATA_ROOT, seed: int = 42) -> dict[str, Any]:
    dirs = ensure_data_dirs(base_dir)
    records = generate_sample_records(seed=seed)
    manifest_path = write_raw_html(records, dirs["raw"])
    bronze_path, bronze_df = parse_raw_html(dirs["raw"], dirs["bronze"])
    silver_path, silver_df = build_silver_dataset(bronze_df, dirs["silver"])
    summary = {
        "manifest_path": str(manifest_path),
        "bronze_path": str(bronze_path),
        "silver_path": str(silver_path),
        "record_count": int(len(silver_df)),
        "date_min": str(silver_df["sold_date"].min()),
        "date_max": str(silver_df["sold_date"].max()),
        "parser_version": PARSER_VERSION,
        "feature_columns": feature_columns(),
    }
    summary_path = dirs["silver"] / "dataset_summary.json"
    summary_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")
    summary["summary_path"] = str(summary_path)
    return summary


def feature_columns() -> list[str]:
    return [
        "brand",
        "category",
        "size",
        "condition",
        "color",
        "material",
        "listing_price",
        "shipping_price",
    ]


def target_column() -> str:
    return "sale_price"


def prediction_example() -> dict[str, Any]:
    return {
        "brand": "Patagonia",
        "category": "Jacket",
        "size": "M",
        "condition": "used_very_good",
        "color": "Blue",
        "material": "Polyester",
        "listing_price": 129.0,
        "shipping_price": 12.5,
    }


def sample_records_json(limit: int = 5) -> list[dict[str, Any]]:
    return [asdict(record) for record in generate_sample_records()[:limit]]


def _clean_text(value: str | None) -> str | None:
    if value is None:
        return None
    return " ".join(value.split()).strip() or None


def _extract_schema_product(soup: BeautifulSoup) -> dict[str, Any]:
    for script in soup.find_all("script", attrs={"type": "application/ld+json"}):
        text = script.get_text(strip=True)
        if not text:
            continue
        try:
            payload = json.loads(text)
        except json.JSONDecodeError:
            continue
        if isinstance(payload, dict) and payload.get("@type") == "Product":
            return payload
    return {}


def _extract_label_value_map(html: str) -> dict[str, str]:
    pairs: dict[str, str] = {}
    pattern = re.compile(
        r'elevated-info__item__label[^>]*>.*?-->([^<]+?)<!--.*?'
        r'elevated-info__item__value[^>]*>(.*?)</div>',
        re.DOTALL,
    )
    for label, raw_value in pattern.findall(html):
        value = _clean_text(BeautifulSoup(raw_value, "html.parser").get_text(" ", strip=True))
        if value:
            pairs[_clean_text(label) or ""] = value
    return pairs


def parse_saved_ebay_item_html(html_path: Path) -> dict[str, Any]:
    html_path = Path(html_path)
    html = html_path.read_text(encoding="utf-8")
    soup = BeautifulSoup(html, "html.parser")
    schema_product = _extract_schema_product(soup)
    labels = _extract_label_value_map(html)

    canonical_url = (
        soup.select_one('link[rel="canonical"]')["href"]
        if soup.select_one('link[rel="canonical"]')
        else None
    )
    title = _clean_text(
        schema_product.get("name")
        or (soup.title.get_text(strip=True) if soup.title else None)
    )
    title = title.removesuffix(" | eBay") if title else title
    listing_id_match = re.search(r"/itm/(\d+)", canonical_url or "")
    sold_line_match = re.search(r"This listing sold on ([^.]+)\.", html)
    price = schema_product.get("offers", {}).get("price")
    shipping = None
    shipping_details = schema_product.get("offers", {}).get("shippingDetails", [])
    if shipping_details:
        shipping = (
            shipping_details[0]
            .get("shippingRate", {})
            .get("value")
        )

    breadcrumb = {}
    for script in soup.find_all("script", attrs={"type": "application/ld+json"}):
        text = script.get_text(strip=True)
        if not text:
            continue
        try:
            payload = json.loads(text)
        except json.JSONDecodeError:
            continue
        if isinstance(payload, dict) and payload.get("@type") == "BreadcrumbList":
            items = payload.get("itemListElement", [])
            breadcrumb = items[-1] if items else {}
            break

    return {
        "listing_id": listing_id_match.group(1) if listing_id_match else None,
        "source_url": canonical_url,
        "raw_path": str(html_path),
        "title": title,
        "sold_text": sold_line_match.group(1) if sold_line_match else None,
        "brand": _clean_text(
            labels.get("Brand")
            or schema_product.get("brand", {}).get("name")
        ),
        "category": _clean_text(breadcrumb.get("name")),
        "size": _clean_text(labels.get("Size")),
        "condition": _clean_text(labels.get("Condition")),
        "color": _clean_text(
            labels.get("Color")
            or schema_product.get("color")
        ),
        "material": _clean_text(labels.get("Material")),
        "price_currency": _clean_text(schema_product.get("offers", {}).get("priceCurrency")),
        "sale_price": float(price) if price is not None else None,
        "shipping_price": float(shipping) if shipping is not None else None,
        "description": _clean_text(
            schema_product.get("description")
            or (
                soup.select_one('meta[name="description"]').get("content")
                if soup.select_one('meta[name="description"]')
                else None
            )
        ),
        "parser_version": "ebay_saved_html_v1",
    }


def _safe_filename(url: str, index: int) -> str:
    parsed = urlparse(url)
    slug = f"{parsed.netloc}{parsed.path}".strip("/").replace("/", "_")
    slug = re.sub(r"[^A-Za-z0-9._-]+", "_", slug)[:120].strip("_")
    slug = slug or f"page_{index:04d}"
    return f"{index:04d}_{slug}.html"


def download_html_pages(
    urls: list[str],
    output_dir: Path,
    delay_seconds: float = 1.0,
    timeout_seconds: float = 20.0,
    user_agent: str = "Mozilla/5.0 (compatible; clothing-mlops-downloader/0.1)",
) -> list[dict[str, Any]]:
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    session = requests.Session()
    session.headers.update({"User-Agent": user_agent})

    manifest: list[dict[str, Any]] = []
    for index, url in enumerate(urls, start=1):
        row: dict[str, Any] = {
            "index": index,
            "url": url,
            "status_code": None,
            "saved_path": None,
            "ok": False,
            "error": None,
        }
        try:
            response = session.get(url, timeout=timeout_seconds)
            row["status_code"] = response.status_code
            response.raise_for_status()
            path = output_dir / _safe_filename(url, index)
            path.write_text(response.text, encoding="utf-8")
            row["saved_path"] = str(path)
            row["ok"] = True
        except requests.RequestException as exc:
            row["error"] = str(exc)
        manifest.append(row)
        if index < len(urls):
            time.sleep(delay_seconds)

    manifest_path = output_dir / "download_manifest.csv"
    pd.DataFrame(manifest).to_csv(manifest_path, index=False)
    return manifest
