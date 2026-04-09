from __future__ import annotations

import argparse
import re
import sqlite3
from pathlib import Path

import pandas as pd


ITEM_TYPE_PATTERNS: list[tuple[str, str]] = [
    (r"\bhoodie|sweatshirt|pullover\b", "hoodie"),
    (r"\bt[\-\s]?shirt|tee|tshirt|t-shirt\b", "tshirt"),
    (r"\bjeans|jean\b", "jeans"),
    (r"\bshorts\b", "shorts"),
    (r"\bdress|gown|shirtdress\b", "dress"),
    (r"\bskirt\b", "skirt"),
    (r"\bleggings\b", "leggings"),
    (r"\bjacket|coat|blazer|parka|windbreaker|puffer|peacoat|bomber|vest\b", "jacket"),
    (r"\bsweater|cardigan\b", "sweater"),
    (r"\bpants|trousers|joggers\b", "pants"),
    (r"\bboot|boots|sandal|sandals|loafer|loafers|heel|heels|slides|clog|clogs\b", "footwear"),
    (r"\bsneaker|sneakers|jordans|jordan|air jordan|running|dunk|dunks|air max|air force\b", "sneaker"),
    (r"\bbag|handbag|tote|backpack|wallet|purse|satchel|crossbody|clutch|waistbag\b", "bag"),
    (r"\bhat|cap|beanie\b", "headwear"),
    (r"\bbelt\b", "belt"),
    (r"\bwatch\b", "watch"),
    (r"\bsunglasses|glasses|eyewear\b", "eyewear"),
]

# Alias text in titles -> canonical brand names in brands.csv.
BRAND_ALIASES: dict[str, list[str]] = {
    "Nike": ["air jordan", "jordan", "jumpman", 'jordans'],
    "Levi's": ["levis", "levi"],
    "Fear of God": ["essentials", "fog", "fear of gods"],
    "The North Face": ["north face", "tnf"],
    "Louis Vuitton": ["lv", "louis vuitton"],
    "Saint Laurent": ["ysl", "saint laurent"],
    "Dolce & Gabbana": ["d&g", "dolce and gabbana"],
    "Abercrombie & Fitch": ["abercrombie", "a&f"],
}


def normalize_text(value: object) -> str:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return ""
    text = str(value).strip().lower()
    text = re.sub(r"[^\w&+'\. ]+", " ", text)
    text = re.sub(r"\s+", " ", text)
    return text


def load_brands(brand_csv_path: Path) -> list[str]:
    brands_df = pd.read_csv(brand_csv_path)
    if "Brand" not in brands_df.columns:
        raise ValueError(f"Expected 'Brand' column in {brand_csv_path}")

    brands = (
        brands_df["Brand"]
        .dropna()
        .astype(str)
        .str.strip()
        .replace("", pd.NA)
        .dropna()
        .tolist()
    )
    # Longest first so "Fear of God" matches before "God" style substrings.
    return sorted(brands, key=len, reverse=True)


def build_brand_regex(brand: str) -> re.Pattern[str]:
    escaped = re.escape(brand.lower())
    # Treat ampersand and apostrophe variants more flexibly.
    escaped = escaped.replace(r"\&", r"(?:&|and)")
    escaped = escaped.replace(r"\'", r"(?:'|)")
    return re.compile(rf"(?<!\w){escaped}(?!\w)", flags=re.IGNORECASE)


def extract_brand(title: str, query: str, brand_patterns: list[tuple[str, re.Pattern[str]]]) -> str:
    search_text = title.strip()
    brand = "unknown"
    for brand_name, pattern in brand_patterns:
        if pattern.search(search_text):
            brand = brand_name
            return brand
    if brand == "unknown":
        for brand_name, pattern in brand_patterns:
            if pattern.search(query):
                brand = brand_name
                return brand
    return brand
        


def extract_item_type(title: str) -> str:
    search_text = f"{title}".lower()
    for pattern, item_type in ITEM_TYPE_PATTERNS:
        if re.search(pattern, search_text):
            return item_type
    return "other"


def normalize_condition(value: object) -> str:
    text = normalize_text(value)
    if not text:
        return "unknown"
    if "new (other)" in text:
        return "new_other"
    if "brand new" in text:
        return "new"
    if "pre-owned" in text or "pre owned" in text or "used" in text:
        return "used"
    if "refurbished" in text:
        return "refurbished"
    return "other"


def clean_price_column(df: pd.DataFrame) -> pd.Series:
    if "price_value" in df.columns:
        values = pd.to_numeric(df["price_value"], errors="coerce")
    else:
        values = pd.Series([pd.NA] * len(df), index=df.index, dtype="float64")

    missing_mask = values.isna()
    if "price_text" in df.columns and missing_mask.any():
        extracted = (
            df.loc[missing_mask, "price_text"]
            .astype(str)
            .str.replace(r"[^0-9.]", "", regex=True)
            .replace("", pd.NA)
        )
        values.loc[missing_mask] = pd.to_numeric(extracted, errors="coerce")
    return values.round(2)


def find_input_csvs(base_dir: Path) -> list[Path]:
    all_csvs = sorted(base_dir.rglob("*.csv"))
    # Exclude output folders to avoid re-ingesting generated files.
    filtered = [p for p in all_csvs if "processed" not in p.parts]
    return filtered


def load_source_frames(csv_paths: list[Path]) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    for path in csv_paths:
        if path.name == "brands.csv":
            continue
        try:
            df = pd.read_csv(path)
        except Exception:
            continue
        expected_cols = {"title", "query", "condition_text"}
        if not expected_cols.issubset(set(df.columns)):
            continue
        if df.empty:
            continue
        df["source_file"] = str(path)
        frames.append(df)

    if not frames:
        raise ValueError("No valid scraped CSV files found with expected columns.")
    return pd.concat(frames, ignore_index=True)


def clean_dataset(data_dir: Path) -> pd.DataFrame:
    brand_csv_path = data_dir / "brands.csv"
    brands = load_brands(brand_csv_path)
    brand_patterns: list[tuple[str, re.Pattern[str]]] = []
    for brand in brands:
        brand_patterns.append((brand, build_brand_regex(brand)))
        for alias in BRAND_ALIASES.get(brand, []):
            brand_patterns.append((brand, build_brand_regex(alias)))

    csv_paths = find_input_csvs(data_dir.parent)
    raw = load_source_frames(csv_paths)

    raw["title"] = raw["title"].fillna("").astype(str).str.strip()
    raw["query"] = raw["query"].fillna("").astype(str).str.strip()
    raw["condition_text"] = raw["condition_text"].fillna("").astype(str).str.strip()

    cleaned = pd.DataFrame(
        {
            "item_id": raw.get("item_id"),
            "title": raw["title"],
            "query": raw["query"],
            "brand_name": [
                extract_brand(t, q, brand_patterns) for t, q in zip(raw["title"], raw["query"])
            ],
            "item_type": [extract_item_type(t) for t in zip(raw["title"])],
            "price": clean_price_column(raw),
            "condition": raw["condition_text"].map(normalize_condition),
            "sold_date_text": raw.get("sold_date_text"),
            "condition_raw": raw["condition_text"],
            "item_url": raw.get("item_url"),
            "scraped_at_utc": raw.get("scraped_at_utc")
        }
    )

    cleaned = cleaned.dropna(subset=["title"], how="any")
    cleaned = cleaned[cleaned["title"].str.len() > 0]
    cleaned = cleaned.drop_duplicates(subset=["item_id", "title", "price"], keep="first")
    cleaned = cleaned.sort_values(by="scraped_at_utc", ascending=False, na_position="last")
    cleaned.reset_index(drop=True, inplace=True)
    return cleaned


def write_outputs(cleaned_df: pd.DataFrame, output_dir: Path, sqlite_path: Path) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)

    csv_output = output_dir / "ebay_historical_cleaned.csv"
    parquet_output = output_dir / "ebay_historical_cleaned.parquet"

    cleaned_df.to_csv(csv_output, index=False)
    cleaned_df.to_parquet(parquet_output, index=False)

    sqlite_path.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(sqlite_path) as conn:
        cleaned_df.to_sql("ebay_historical_cleaned", conn, if_exists="replace", index=False)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Clean eBay historical clothing exports and build a modeling dataset."
    )
    parser.add_argument(
        "--data-dir",
        type=Path,
        default=Path("ebay_historical_clothing_scraper/data"),
        help="Path to scraper data directory containing brands.csv and exports.",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("ebay_historical_clothing_scraper/data/processed"),
        help="Directory for cleaned CSV/Parquet outputs.",
    )
    parser.add_argument(
        "--sqlite-path",
        type=Path,
        default=Path("ebay_historical_clothing_scraper/data/processed/ebay_cleaned.db"),
        help="SQLite file path for cleaned table storage.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    cleaned_df = clean_dataset(args.data_dir)
    write_outputs(cleaned_df, args.output_dir, args.sqlite_path)

    print(f"Rows cleaned: {len(cleaned_df):,}")
    print("Output files:")
    print(f"- {args.output_dir / 'ebay_historical_cleaned.csv'}")
    print(f"- {args.output_dir / 'ebay_historical_cleaned.parquet'}")
    print(f"- {args.sqlite_path} (table: ebay_historical_cleaned)")


if __name__ == "__main__":
    main()
