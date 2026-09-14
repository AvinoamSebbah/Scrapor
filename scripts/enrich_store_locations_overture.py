"""Confirm unresolved store locations against the free Overture Maps catalog.

The matcher is deliberately conservative.  A store is promoted to ``verified``
only when the chain/brand matches and Overture provides one unique candidate
with either the exact structured address, an already-known phone number, or an
exact branch name in the exact city.  Ambiguous candidates remain hidden from
the frontend as ``review_required``.
"""

from __future__ import annotations

import argparse
import os
import re
import unicodedata
from collections.abc import Iterable

import duckdb
import psycopg2
import requests


STAC_CATALOG = "https://stac.overturemaps.org/catalog.json"
AZURE_RELEASE_ROOT = "az://overturemapswestus2.blob.core.windows.net/release"
ISRAEL_BBOX = (34.1, 29.4, 35.95, 33.4)
PLACE_CATEGORIES = (
    "food_and_beverage_store",
    "pharmacy_and_drug_store",
    "convenience_store",
    "department_store",
    "discount_store",
    "gas_station",
    "shopping",
)

CHAIN_ALIASES = {
    "קרפור": ("קרפור", "carrefour"),
    "Dor Alon": ("דור אלון", "dor alon", "doralon"),
    "BE": ("be", "be pharm"),
    "סיטי מרקט": ("סיטי מרקט", "city market"),
    "רמי לוי שיווק השקמה": ("רמי לוי", "rami levy"),
    "רמי לוי בשכונה": ("רמי לוי", "rami levy"),
    "סופר פארם ישראל": ("סופר פארם", "super pharm", "super-pharm"),
    "מ. יוחננוף ובניו": ("יוחננוף", "yochananof"),
    "שופרסל דיל": ("שופרסל", "shufersal"),
    "שופרסל שלי": ("שופרסל", "shufersal"),
    "שופרסל אקספרס": ("שופרסל", "shufersal"),
    "שופרסל": ("שופרסל", "shufersal"),
    "שופרסל ONLINE": ("שופרסל", "shufersal"),
    "סופר ספיר בע\"מ": ("סופר ספיר", "super sapir"),
    "פרש מרקט": ("פרש מרקט", "fresh market", "freshmarket"),
    "טיב טעם": ("טיב טעם", "tiv taam"),
    "נתיב החסד- סופר חסד בע\"מ": ("נתיב החסד", "netiv hachesed"),
    "קשת טעמים": ("קשת טעמים", "keshet teamim"),
    "גוד פארם בע\"מ": ("גוד פארם", "good pharm", "goodpharm"),
    "שוק העיר (ט.ע.מ.ס.) בע\"מ": ("שוק העיר",),
    "וולט מרקט": ("וולט מרקט", "wolt market"),
    "ג.מ. מעיין אלפיים (07) בע\"מ": ("מעיין 2000", "מעיין אלפיים", "maayan 2000"),
    "אושר עד": ("אושר עד", "osher ad"),
    "יש חסד": ("יש חסד", "yesh chesed"),
    "Dabach": ("דבאח", "dabach"),
    "זול ובגדול בע\"מ": ("זול ובגדול",),
    "סטופמרקט": ("סטופמרקט", "stopmarket", "stop market"),
    "פוליצר": ("פוליצר",),
    "שפע ברכת השם בע\"מ": ("שפע ברכת השם",),
    "סופר יודה": ("סופר יודה", "super yuda"),
    "סופר ברקת קמעונאות בע\"מ": ("סופר ברקת",),
    "גוד מרקט": ("גוד מרקט", "good market"),
}

IGNORED_ADDRESS_WORDS = {
    "רחוב", "רח", "קניון", "מרכז", "מסחרי", "ישראל", "israel", "street", "st",
}
IGNORED_BRANCH_WORDS = {
    "סניף", "ישראל", "שיווק", "השקמה", "ובניו", "בעמ", "store", "branch",
}


def database_url() -> str:
    value = os.getenv("POSTGRESQL_URL") or os.getenv("DATABASE_URL")
    if not value:
        raise RuntimeError("POSTGRESQL_URL or DATABASE_URL is required")
    if os.getenv("AGALI_LOCAL_DB_TUNNEL") == "1":
        value = value.replace("@db:", "@127.0.0.1:")
    return value


def normalized(value: str | None) -> str:
    text = unicodedata.normalize("NFKD", (value or "").lower()).replace("־", "-")
    return re.sub(r"[^0-9a-zא-ת]+", "", text)


def tokens(value: str | None) -> set[str]:
    return {
        token for token in re.split(r"[^0-9a-zא-ת]+", (value or "").lower())
        if token and token not in IGNORED_ADDRESS_WORDS
    }


def digits(value: str | None) -> set[str]:
    return set(re.findall(r"(?<!\d)\d{1,4}(?!\d)", value or ""))


def normalized_phone(value: str | None) -> str:
    number = re.sub(r"\D", "", value or "")
    if number.startswith("972"):
        number = "0" + number[3:]
    return number


def flatten_strings(value: object) -> Iterable[str]:
    if isinstance(value, str):
        if value.strip():
            yield value
    elif isinstance(value, dict):
        for nested in value.values():
            yield from flatten_strings(nested)
    elif isinstance(value, (list, tuple)):
        for nested in value:
            yield from flatten_strings(nested)


def aliases_for(chain_name: str) -> tuple[str, ...]:
    aliases = CHAIN_ALIASES.get(chain_name)
    if aliases:
        return aliases
    cleaned = re.sub(r"(?:בע\"?מ|בעמ|\([^)]*\))", " ", chain_name)
    return (cleaned.strip(),)


def alias_matches(alias: str, candidate_text: str) -> bool:
    wanted = normalized(alias)
    candidate = normalized(candidate_text)
    if not wanted or not candidate:
        return False
    if len(wanted) <= 2:
        return wanted in {normalized(item) for item in tokens(candidate_text)}
    return wanted in candidate or candidate in wanted


def chain_matches(store: dict, place: dict) -> bool:
    names = " ".join(flatten_strings((place.get("names"), place.get("brand"))))
    return any(alias_matches(alias, names) for alias in aliases_for(store["chain_name"]))


def city_matches(store: dict, address: dict, place: dict) -> bool:
    wanted = normalized(store.get("city"))
    if not wanted:
        return False
    values = [address.get("locality"), address.get("freeform")]
    values.extend(flatten_strings(place.get("names")))
    return any(wanted == normalized(value) or wanted in normalized(value) for value in values if value)


def exact_address_matches(store: dict, address: dict) -> bool:
    wanted_number = digits(store.get("address"))
    found_number = digits(address.get("freeform"))
    if len(wanted_number) != 1 or wanted_number != found_number:
        return False
    city_words = tokens(store.get("city"))
    wanted_words = {
        word for word in tokens(store.get("address"))
        if not word.isdigit() and word not in city_words
    }
    found_words = {word for word in tokens(address.get("freeform")) if not word.isdigit()}
    return bool(wanted_words) and wanted_words <= found_words


def exact_branch_name_matches(store: dict, place: dict) -> bool:
    alias_words = set().union(*(tokens(alias) for alias in aliases_for(store["chain_name"])))
    wanted_words = {
        normalized(word) for word in tokens(store.get("store_name"))
        if word not in IGNORED_BRANCH_WORDS
        and word not in alias_words
        and len(normalized(word)) >= 2
    }
    if not wanted_words:
        return False
    candidate_words = {
        normalized(word) for word in tokens(" ".join(flatten_strings(place.get("names"))))
        if word not in IGNORED_BRANCH_WORDS
    }
    return wanted_words <= candidate_words


def source_datasets(place: dict) -> list[str]:
    return sorted({
        str(item.get("dataset")) for item in place.get("sources") or []
        if isinstance(item, dict) and item.get("dataset")
    })


def proof_for(store: dict, place: dict) -> str | None:
    virtual_name = normalized(store.get("store_name"))
    if any(marker in virtual_name for marker in ("אתרסחר", "אונליין", "online")):
        return None
    if not chain_matches(store, place):
        return None
    known_phone = normalized_phone(store.get("phone"))
    candidate_phones = {normalized_phone(value) for value in place.get("phones") or []}
    if known_phone and known_phone in candidate_phones:
        return "exact_chain_and_phone"
    for address in place.get("addresses") or []:
        if not isinstance(address, dict):
            continue
        if exact_address_matches(store, address):
            return "exact_chain_and_address"
        if city_matches(store, address, place) and exact_branch_name_matches(store, place):
            return "exact_chain_city_and_branch_name"
    return None


def latest_release() -> str:
    verify_tls = os.getenv("AGALI_ALLOW_INSECURE_TLS") != "1"
    response = requests.get(
        STAC_CATALOG,
        headers={"User-Agent": "AgaliStoreLocations/1.0 (+https://agali.co.il)"},
        timeout=30,
        verify=verify_tls,
    )
    response.raise_for_status()
    value = response.json().get("latest")
    if not isinstance(value, str) or not re.fullmatch(r"\d{4}-\d{2}-\d{2}\.\d+", value):
        raise RuntimeError("Overture STAC catalog did not expose a valid latest release")
    return value


def fetch_places(release: str) -> list[dict]:
    west, south, east, north = ISRAEL_BBOX
    url = f"{AZURE_RELEASE_ROOT}/{release}/theme=places/type=place/*"
    connection = duckdb.connect()
    connection.execute("INSTALL azure; LOAD azure")
    rows = connection.execute(
        """
        SELECT id, names, brand, addresses, phones, websites, bbox.xmin, bbox.ymin, sources
          FROM read_parquet(?)
         WHERE bbox.xmin BETWEEN ? AND ?
           AND bbox.ymin BETWEEN ? AND ?
           AND basic_category IN (SELECT * FROM unnest(?))
        """,
        [url, west, east, south, north, list(PLACE_CATEGORIES)],
    ).fetchall()
    columns = [item[0] for item in connection.description]
    connection.close()
    return [dict(zip(columns, row)) for row in rows]


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--verbose", action="store_true")
    args = parser.parse_args()

    release = latest_release()
    places = fetch_places(release)
    connection = psycopg2.connect(database_url())
    cursor = connection.cursor()
    cursor.execute(
        """
        SELECT id, chain_name, store_name, address, city, phone
          FROM stores
         WHERE geocode_status = 'review_required'
        """
    )
    columns = [item.name for item in cursor.description]
    stores = [dict(zip(columns, row)) for row in cursor.fetchall()]

    verified = 0
    ambiguous = 0
    by_proof: dict[str, int] = {}
    examples = []
    places_by_chain = {
        chain_name: [place for place in places if chain_matches({"chain_name": chain_name}, place)]
        for chain_name in {store["chain_name"] for store in stores}
    }
    for store in stores:
        matches = [(place, proof_for(store, place)) for place in places_by_chain[store["chain_name"]]]
        matches = [(place, proof) for place, proof in matches if proof]
        unique_points = {(round(float(place["xmin"]), 6), round(float(place["ymin"]), 6)) for place, _ in matches}
        if len(unique_points) != 1:
            ambiguous += int(bool(matches))
            continue
        place, proof = matches[0]
        verified += 1
        by_proof[proof] = by_proof.get(proof, 0) + 1
        datasets = source_datasets(place)
        source_url = next((url for url in place.get("websites") or [] if url), None)
        matched_name = next(iter(flatten_strings(place.get("names"))), None)
        if args.verbose or len(examples) < 12:
            examples.append((store["id"], store["chain_name"], store["store_name"], proof, matched_name))
        if args.apply:
            phone = next((value for value in place.get("phones") or [] if value), None)
            website = source_url
            cursor.execute(
                """
                UPDATE stores
                   SET latitude = %s,
                       longitude = %s,
                       geocode_status = 'verified',
                       geocode_source = 'overture-maps-place',
                       geocode_precision = 'official_poi',
                       geocode_reason = %s,
                       geocode_matched_address = %s,
                       geocode_verified_at = NOW(),
                       phone = COALESCE(phone, %s),
                       website_url = COALESCE(website_url, %s),
                       store_details_source = COALESCE(store_details_source, 'overture-maps'),
                       store_details_source_url = COALESCE(store_details_source_url, %s),
                       store_details_updated_at = CASE
                           WHEN phone IS NULL AND %s IS NOT NULL THEN NOW()
                           WHEN website_url IS NULL AND %s IS NOT NULL THEN NOW()
                           ELSE store_details_updated_at
                       END
                 WHERE id = %s AND geocode_status = 'review_required'
                """,
                (
                    place["ymin"], place["xmin"],
                    f"{proof};release={release};datasets={','.join(datasets)}",
                    matched_name, phone, website, source_url, phone, website, store["id"],
                ),
            )

    if args.apply:
        connection.commit()
    else:
        connection.rollback()
    cursor.close()
    connection.close()
    print({
        "mode": "apply" if args.apply else "dry-run",
        "release": release,
        "places": len(places),
        "review_stores": len(stores),
        "verified": verified,
        "ambiguous": ambiguous,
        "by_proof": by_proof,
        "examples": examples,
    })


if __name__ == "__main__":
    main()
