#!/usr/bin/env python3
"""
ChathamEngine v2 â Claude-Native Python Pipeline
Commercial Real Estate Intelligence for Chatham County, GA
For Joe Solana | The Commercial Group | Savannah, GA

Pulls parcel + zoning data from SAGIS ArcGIS REST APIs,
scores every parcel as an off-market deal candidate (0â100),
and outputs data/parcels.json for the Harbor Intel dashboard.

Usage:
    python chatham_engine.py

Requirements (install once):
    pip install requests geopandas shapely pandas
"""

import requests
import pandas as pd
import geopandas as gpd
from shapely.geometry import shape
import json
import os
import time
from datetime import datetime

# âââââââââââââââââââââââââââââââââââââââââââââ
# CONFIGURATION â edit these if needed
# âââââââââââââââââââââââââââââââââââââââââââââ

# Primary parcel source: SAGIS CitizenServe BOA layer
SAGIS_PARCEL_URL        = "https://pub.sagis.org/arcgis/rest/services/Savannah/CitizenServe/MapServer/1/query"
# Fallback parcel source: SAGIS OpenData Parcels (different field names â see fetch_parcels)
SAGIS_PARCEL_URL_ALT    = "https://pub.sagis.org/arcgis/rest/services/OpenData/Parcels/MapServer/0/query"
# Zoning: use MapServer (NOT FeatureServer â FeatureServer returns empty on this server)
SAGIS_ZONING_URL        = "https://pub.sagis.org/arcgis/rest/services/OpenData/Boundaries/MapServer/4/query"
SAGIS_ZONING_CHANGES_URL = "https://pub.sagis.org/arcgis/rest/services/OpenData/Boundaries/MapServer/13/query"

OUTPUT_DIR        = "data"          # folder where output files are written
OUTPUT_JSON       = os.path.join(OUTPUT_DIR, "parcels.json")
OUTPUT_CSV        = os.path.join(OUTPUT_DIR, "parcel_master_chatham.csv")

TOP_N_DEALS       = 300            # max parcels in dashboard output
CURRENT_YEAR      = datetime.now().year
PERMIT_STALE_YRS  = 3              # "no recent permit" threshold
MIN_DEAL_VALUE    = 250_000        # deal size lower bound
MAX_DEAL_VALUE    = 5_000_000      # deal size upper bound
PAGE_SIZE         = 1_000          # records per API page (ArcGIS max is usually 1000â2000)
MAX_RETRIES       = 3
RETRY_DELAY       = 2              # seconds between retries

# Zoning codes flagged as industrial/flex
INDUSTRIAL_ZONES  = {"M-1", "M-2", "B-I", "I-L", "I-H", "LI", "HI", "M1", "M2", "BI", "IH", "IL"}

# âââââââââââââââââââââââââââââââââââââââââââââ
# UTILITY: PAGINATED ARCGIS FETCH
# âââââââââââââââââââââââââââââââââââââââââââââ

def fetch_all_records(url, params_base, layer_name="layer"):
    """Fetch ALL records from an ArcGIS REST endpoint using pagination.

    ArcGIS caps responses at maxRecordCount (usually 1,000â2,000). Without
    pagination, you'd silently receive only the first page. This function
    loops using resultOffset until exceededTransferLimit is False.
    """
    all_records = []
    offset = 0

    print(f"  Fetching {layer_name}...")

    while True:
        params = {
            **params_base,
            "resultOffset":      offset,
            "resultRecordCount": PAGE_SIZE,
            "f":                 "json",
        }

        for attempt in range(MAX_RETRIES):
            try:
                resp = requests.get(url, params=params, timeout=30)
                resp.raise_for_status()
                data = resp.json()
                break
            except Exception as e:
                if attempt < MAX_RETRIES - 1:
                    print(f"    Retry {attempt + 1}/{MAX_RETRIES} after: {e}")
                    time.sleep(RETRY_DELAY)
                else:
                    raise RuntimeError(f"Failed to fetch {layer_name} after {MAX_RETRIES} attempts: {e}")

        # Detect API-level errors before checking features
        if "error" in data:
            err = data["error"]
            print(f"    API ERROR from {layer_name}: code={err.get('code')} msg={err.get('message')}")
            print(f"    Full error: {err}")
            break

        features = data.get("features", [])
        if not features:
            # Print diagnostic info so we know what the server actually returned
            if offset == 0:
                snippet = str(data)[:600]
                print(f"    WARNING: 0 features returned for {layer_name}")
                print(f"    Response keys: {list(data.keys())}")
                print(f"    Response snippet: {snippet}")
            break

        all_records.extend(features)
        print(f"    ...{len(all_records)} records", end="\r")

        if not data.get("exceededTransferLimit", False):
            break

        offset += PAGE_SIZE
        time.sleep(0.25)  # polite delay

    print(f"    Done â {len(all_records):,} {layer_name} records fetched")
    return all_records


# âââââââââââââââââââââââââââââââââââââââââââââ
# STEP 1: FETCH PARCELS
# âââââââââââââââââââââââââââââââââââââââââââââ

def _parse_parcel_features_primary(features):
    """Parse features from the CitizenServe BOA layer (primary field names)."""
    rows = []
    for f in features:
        attr = f.get("attributes", {})
        geom = f.get("geometry")

        lat, lon = None, None
        if geom:
            try:
                if "rings" in geom:
                    poly = shape({"type": "Polygon", "coordinates": geom["rings"]})
                    c = poly.centroid
                    lat, lon = round(c.y, 6), round(c.x, 6)
                elif "x" in geom:
                    lat, lon = round(geom["y"], 6), round(geom["x"], 6)
            except Exception:
                pass

        rows.append({
            "parcel_id":     str(attr.get("PARCEL_ID") or "").strip(),
            "owner_name":    str(attr.get("OWNNAME1") or "").strip(),
            "owner_addr":    str(attr.get("OWNADDR1") or "").strip(),
            "owner_city":    str(attr.get("OWNCITY") or "").strip(),
            "owner_state":   str(attr.get("OWNSTATE") or "").strip().upper(),
            "owner_zip":     str(attr.get("OWNZIP") or "").strip(),
            "address":       str(attr.get("SITUS_ADDR") or "").strip(),
            "situs_city":    str(attr.get("SITUS_CITY") or "Savannah").strip(),
            "land_value":    float(attr.get("LND_VAL") or 0),
            "bldg_value":    float(attr.get("BLDG_VAL") or 0),
            "total_value":   float(attr.get("TOTAL_VAL") or 0),
            "acreage":       float(attr.get("ACREAGE") or 0),
            "use_code":      str(attr.get("USE_CODE") or "").strip(),
            "use_desc":      str(attr.get("USE_DESC") or "").strip(),
            "last_permit_yr": attr.get("LAST_PERMIT_YR"),
            "nbhd_code":     str(attr.get("NBHD_CODE") or "").strip(),
            "lat":           lat,
            "lon":           lon,
        })
    return rows


def _parse_parcel_features_alt(features):
    """Parse features from the OpenData/Parcels layer (alternate field names)."""
    rows = []
    for f in features:
        attr = f.get("attributes", {})
        geom = f.get("geometry")

        lat, lon = None, None
        if geom:
            try:
                if "rings" in geom:
                    poly = shape({"type": "Polygon", "coordinates": geom["rings"]})
                    c = poly.centroid
                    lat, lon = round(c.y, 6), round(c.x, 6)
                elif "x" in geom:
                    lat, lon = round(geom["y"], 6), round(geom["x"], 6)
            except Exception:
                pass

        # OpenData/Parcels uses different field names than CitizenServe
        rows.append({
            "parcel_id":     str(attr.get("PIN") or attr.get("PIN_NUMBER") or "").strip(),
            "owner_name":    str(attr.get("NAME") or "").strip(),
            "owner_addr":    str(attr.get("ADDRESS_1") or "").strip(),
            "owner_city":    str(attr.get("CITY") or "").strip(),
            "owner_state":   str(attr.get("STATE") or "").strip().upper(),
            "owner_zip":     str(attr.get("ZIP_CODE") or "").strip(),
            "address":       str(attr.get("PROP_ADDRESS") or attr.get("ADDRESS_1") or "").strip(),
            "situs_city":    "Savannah",
            "land_value":    float(attr.get("LAND_VALUE") or 0),
            "bldg_value":    float(attr.get("BUILDING_VALUE") or 0),
            "total_value":   float(attr.get("REAL_ESTATE_VALUE") or attr.get("TOTAL_ASSESSMENT") or 0),
            "acreage":       float(attr.get("LAND_UNITS") or attr.get("ACREAGE") or 0),
            "use_code":      str(attr.get("LAND_USE_CODE") or attr.get("PROP_CLASS_CODE") or "").strip(),
            "use_desc":      str(attr.get("USE_DESCRIPTION") or "").strip(),
            "last_permit_yr": None,   # not available in this layer
            "nbhd_code":     "",
            "lat":           lat,
            "lon":           lon,
        })
    return rows


def fetch_parcels():
    """Pull all parcel records from SAGIS. Tries primary endpoint first, falls back to alt."""
    params_primary = {
        "where":          "1=1",
        "outFields":      ("PARCEL_ID,OWNNAME1,OWNADDR1,OWNCITY,OWNSTATE,OWNZIP,"
                           "LND_VAL,BLDG_VAL,TOTAL_VAL,ACREAGE,USE_CODE,USE_DESC,"
                           "SITUS_ADDR,SITUS_CITY,LAST_PERMIT_YR,NBHD_CODE"),
        "returnGeometry": "true",
        "outSR":          "4326",
    }

    features = fetch_all_records(SAGIS_PARCEL_URL, params_primary, "parcels")

    if not features:
        print("  Primary parcel URL returned 0 records â trying fallback URL...")
        params_alt = {
            "where":          "1=1",
            "outFields":      ("PIN,PIN_NUMBER,NAME,ADDRESS_1,CITY,STATE,ZIP_CODE,"
                               "PROP_ADDRESS,LAND_VALUE,BUILDING_VALUE,REAL_ESTATE_VALUE,"
                               "TOTAL_ASSESSMENT,LAND_UNITS,ACREAGE,LAND_USE_CODE,PROP_CLASS_CODE"),
            "returnGeometry": "true",
            "outSR":          "4326",
        }
        features = fetch_all_records(SAGIS_PARCEL_URL_ALT, params_alt, "parcels (alt)")
        if features:
            print("  Fallback parcel URL succeeded!")
            return pd.DataFrame(_parse_parcel_features_alt(features))

    # Parse using primary field names (CitizenServe BOA layer)
    rows = _parse_parcel_features_primary(features)
    return pd.DataFrame(rows) if rows else pd.DataFrame(columns=[
        "parcel_id", "owner_name", "owner_addr", "owner_city", "owner_state",
        "owner_zip", "address", "situs_city", "land_value", "bldg_value",
        "total_value", "acreage", "use_code", "use_desc", "last_permit_yr",
        "nbhd_code", "lat", "lon",
    ])


# âââââââââââââââââââââââââââââââââââââââââââââ
# STEP 2: FETCH ZONING LAYERS
# âââââââââââââââââââââââââââââââââââââââââââââ

def fetch_zoning_gdf():
    """Pull zoning polygons from SAGIS and return a GeoDataFrame."""
    params = {
        "where":          "1=1",
        "outFields":      "ZONE_CODE,ZONE_DESC",
        "returnGeometry": "true",
        "outSR":          "4326",
    }
    features = fetch_all_records(SAGIS_ZONING_URL, params, "zoning polygons")

    rows = []
    for f in features:
        geom = f.get("geometry")
        attr = f.get("attributes", {})
        zone_code = str(attr.get("ZONE_CODE") or "").strip().upper()
        if not geom or not zone_code:
            continue
        try:
            shapely_geom = shape({"type": "Polygon", "coordinates": geom.get("rings", [])})
            rows.append({
                "geometry":  shapely_geom,
                "zone_code": zone_code,
                "zone_desc": str(attr.get("ZONE_DESC") or "").strip(),
            })
        except Exception:
            pass

    return gpd.GeoDataFrame(rows, crs="EPSG:4326") if rows else gpd.GeoDataFrame()


def fetch_zoning_changes_gdf():
    """Pull pending/recent zoning change polygons from SAGIS."""
    params = {
        "where":          "1=1",
        "outFields":      "PETITION_NO,CHANGE_TYPE,STATUS,HEARING_DATE",
        "returnGeometry": "true",
        "outSR":          "4326",
    }
    features = fetch_all_records(SAGIS_ZONING_CHANGES_URL, params, "zoning change areas")

    rows = []
    for f in features:
        geom = f.get("geometry")
        attr = f.get("attributes", {})
        if not geom:
            continue
        try:
            shapely_geom = shape({"type": "Polygon", "coordinates": geom.get("rings", [])})
            rows.append({
                "geometry":    shapely_geom,
                "petition_no": str(attr.get("PETITION_NO") or ""),
                "change_type": str(attr.get("CHANGE_TYPE") or ""),
                "zc_status":   str(attr.get("STATUS") or ""),
            })
        except Exception:
            pass

    return gpd.GeoDataFrame(rows, crs="EPSG:4326") if rows else None


# âââââââââââââââââââââââââââââââââââââââââââââ
# STEP 3: SPATIAL JOINS
# âââââââââââââââââââââââââââââââââââââââââââââ

def spatial_join_zoning(df_parcels, gdf_zoning, gdf_zoning_changes):
    """
    Join parcel centroids â zoning polygons and â zoning change areas.

    We use centroid points rather than full polygon overlays because it's
    dramatically faster and sufficient for parcel-level classification.
    """
    print("  Building parcel GeoDataFrame from centroids...")
    valid = df_parcels.dropna(subset=["lat", "lon"]).copy()

    gdf_pts = gpd.GeoDataFrame(
        valid[["parcel_id"]],
        geometry=gpd.points_from_xy(valid["lon"], valid["lat"]),
        crs="EPSG:4326",
    )

    # Join 1: parcel centroid â zoning polygon
    print("  Joining parcels to zoning polygons...")
    if not gdf_zoning.empty:
        j1 = gpd.sjoin(
            gdf_pts,
            gdf_zoning[["zone_code", "zone_desc", "geometry"]],
            how="left",
            predicate="within",
        ).drop_duplicates(subset=["parcel_id"], keep="first")

        zone_map = j1.set_index("parcel_id")[["zone_code", "zone_desc"]].to_dict("index")
        df_parcels["zone_code"] = df_parcels["parcel_id"].map(
            lambda pid: zone_map.get(pid, {}).get("zone_code", "")
        )
        df_parcels["zone_desc"] = df_parcels["parcel_id"].map(
            lambda pid: zone_map.get(pid, {}).get("zone_desc", "")
        )
        assigned = df_parcels["zone_code"].ne("").sum()
        print(f"    Zoning assigned to {assigned:,} of {len(df_parcels):,} parcels")
    else:
        df_parcels["zone_code"] = ""
        df_parcels["zone_desc"] = ""
        print("    WARNING: No zoning polygons loaded â zone_code will be empty")

    # Join 2: parcel centroid â zoning change area
    df_parcels["near_zoning_change"] = False
    if gdf_zoning_changes is not None and not gdf_zoning_changes.empty:
        print("  Flagging parcels inside zoning change areas...")
        j2 = gpd.sjoin(
            gdf_pts,
            gdf_zoning_changes[["geometry"]],
            how="inner",
            predicate="within",
        )
        in_change = set(j2["parcel_id"].unique())
        df_parcels["near_zoning_change"] = df_parcels["parcel_id"].isin(in_change)
        print(f"    {len(in_change):,} parcels inside zoning change areas")

    return df_parcels


# âââââââââââââââââââââââââââââââââââââââââââââ
# STEP 4: SCORING ENGINE
# âââââââââââââââââââââââââââââââââââââââââââââ

def score_parcel(row):
    """
    Score a single parcel row from 0 to 100 as an off-market deal candidate.
    Returns (score: int, tier: str, tags: str).

    Scoring breakdown (100 pts max):
      +15  Industrial/flex zoning
      +15  Vacant / no improvement
      +10  Land-heavy ratio (land > 70% of total value)
      +10  No recent permits (>3 years stale)
      +15  Inside a zoning change petition area
      +10  Out-of-state owner
      +10  Trust / estate / heir ownership
      +10  Use-zoning mismatch (residential use on commercial zone)
      + 5  Deal size in sweet spot ($250kâ$5M)
    """
    score = 0
    tags  = []

    zone      = (row.get("zone_code") or "").upper()
    use_code  = str(row.get("use_code") or "")
    use_desc  = (row.get("use_desc") or "").upper()
    owner     = (row.get("owner_name") or "").upper()
    state     = (row.get("owner_state") or "").upper()
    land_val  = float(row.get("land_value") or 0)
    bldg_val  = float(row.get("bldg_value") or 0)
    total_val = float(row.get("total_value") or 0)
    permit_yr = row.get("last_permit_yr")
    near_zc   = bool(row.get("near_zoning_change"))

    # 1 â Industrial / flex zoning (+15)
    if zone and (any(iz in zone for iz in INDUSTRIAL_ZONES) or zone.startswith("M")):
        score += 15
        tags.append("industrial-zone")

    # 2 â Vacant / no improvement (+15)
    vacant_codes = {"00", "0000", "0", "VAC"}
    if use_code in vacant_codes or "VACANT" in use_desc or bldg_val == 0:
        score += 15
        tags.append("vacant-land")

    # 3 â Land-heavy ratio (+10)
    if total_val > 0 and (land_val / total_val) > 0.70:
        score += 10
        tags.append("land-heavy")

    # 4 â No recent permits (+10)
    permit_stale = (
        permit_yr is None
        or (isinstance(permit_yr, (int, float)) and permit_yr < CURRENT_YEAR - PERMIT_STALE_YRS)
    )
    if permit_stale:
        score += 10
        tags.append("no-recent-permits")

    # 5 â Inside zoning change area (+15)
    if near_zc:
        score += 15
        tags.append("zoning-change-area")

    # 6 â Out-of-state owner (+10)
    if state and state not in {"GA", ""}:
        score += 10
        tags.append("out-of-state-owner")

    # 7 â Trust / estate / heir ownership (+10)
    trust_kw = ["TRUST", "ESTATE", "HEIR", "REVOC", "IRREV", "TESTAMENTARY", "DECEDENT"]
    if any(kw in owner for kw in trust_kw):
        score += 10
        tags.append("trust-estate")

    # 8 â Use-zoning mismatch (+10)
    residential_use = (
        use_code.startswith("1")
        or "RESID" in use_desc
        or "SINGLE FAM" in use_desc
    )
    commercial_zone = any(z in zone for z in ["B-", "C-", "M-", "LI", "HI", "I-"])
    if residential_use and commercial_zone:
        score += 10
        tags.append("use-zoning-mismatch")

    # 9 â Deal size sweet spot (+5)
    if MIN_DEAL_VALUE <= total_val <= MAX_DEAL_VALUE:
        score += 5
        tags.append("deal-size-range")

    score = min(score, 100)

    if   score >= 70: tier = "A"
    elif score >= 50: tier = "B"
    elif score >= 30: tier = "C"
    else:             tier = "D"

    return score, tier, ", ".join(tags)


def apply_scoring(df):
    """Apply scoring function to all parcels and add result columns."""
    print(f"  Scoring {len(df):,} parcels...")
    results = df.apply(score_parcel, axis=1, result_type="expand")
    df["deal_score"] = results[0].astype(int)
    df["deal_tier"]  = results[1]
    df["deal_tags"]  = results[2]
    return df


# âââââââââââââââââââââââââââââââââââââââââââââ
# STEP 5: BUILD OUTPUT
# âââââââââââââââââââââââââââââââââââââââââââââ

def fmt_currency(v):
    try:
        return f"${int(float(v)):,}" if v and float(v) > 0 else "â"
    except Exception:
        return "â"


def build_output(df):
    """Filter to top deals, add display fields and deep-link URLs."""
    print(f"  Filtering to top {TOP_N_DEALS} deals (excluding near-zero value parcels)...")

    # Drop parcels with essentially no assessed value
    df = df[df["total_value"] > 10_000].copy()

    # Sort by deal score descending, take top N
    df = df.sort_values("deal_score", ascending=False).head(TOP_N_DEALS)

    # Deep-link URLs
    df["chathamtax_url"] = df["parcel_id"].apply(
        lambda pid: f"https://www.chathamtax.org/PT/search.aspx?ParcelNumber={pid}" if pid else ""
    )
    df["sagis_url"] = df["parcel_id"].apply(
        lambda pid: f"https://www.sagis.org/BOA/boa_property_info.html?PIN={pid}" if pid else ""
    )

    # Human-readable display fields
    df["land_value_fmt"]  = df["land_value"].apply(fmt_currency)
    df["bldg_value_fmt"]  = df["bldg_value"].apply(fmt_currency)
    df["total_value_fmt"] = df["total_value"].apply(fmt_currency)
    df["acreage_fmt"]     = df["acreage"].apply(
        lambda v: f"{float(v):.2f} ac" if v and float(v) > 0 else "â"
    )
    df["permit_display"]  = df["last_permit_yr"].apply(
        lambda y: str(int(y)) if y and not pd.isna(y) else "None on record"
    )

    output_fields = [
        "parcel_id", "address", "situs_city",
        "owner_name", "owner_state", "owner_city", "owner_zip",
        "zone_code", "zone_desc",
        "use_code", "use_desc",
        "land_value",  "bldg_value",  "total_value",
        "land_value_fmt", "bldg_value_fmt", "total_value_fmt",
        "acreage", "acreage_fmt",
        "last_permit_yr", "permit_display", "near_zoning_change",
        "deal_score", "deal_tier", "deal_tags",
        "lat", "lon",
        "chathamtax_url", "sagis_url",
    ]
    output_fields = [c for c in output_fields if c in df.columns]
    return df[output_fields].reset_index(drop=True)


# âââââââââââââââââââââââââââââââââââââââââââââ
# MAIN
# âââââââââââââââââââââââââââââââââââââââââââââ

def main():
    start = time.time()
    print("=" * 55)
    print("ChathamEngine v2 â Parcel Intelligence Pipeline")
    print(f"Run date: {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print("=" * 55)

    os.makedirs(OUTPUT_DIR, exist_ok=True)

    # ââ 1. Parcels ââââââââââââââââââââââââââââââââââââ
    print("\n[1/5] Fetching parcels from SAGIS BOA layer...")
    df_parcels = fetch_parcels()
    print(f"  â {len(df_parcels):,} parcels loaded")
    if df_parcels.empty:
        print("  ABORT: 0 parcels from SAGIS (both primary and fallback URLs).")
        print("  See diagnostic output above. API may be blocking this IP or URLs have changed.")
        raise SystemExit(1)

    # ââ 2. Zoning layers âââââââââââââââââââââââââââââ
    print("\n[2/5] Fetching zoning data from SAGIS...")
    gdf_zoning         = fetch_zoning_gdf()
    gdf_zoning_changes = fetch_zoning_changes_gdf()

    # ââ 3. Spatial join âââââââââââââââââââââââââââââââ
    print("\n[3/5] Running spatial joins (parcel centroids â zoning)...")
    df_parcels = spatial_join_zoning(df_parcels, gdf_zoning, gdf_zoning_changes)

    # ââ 4. Score ââââââââââââââââââââââââââââââââââââââ
    print("\n[4/5] Scoring all parcels...")
    df_parcels = apply_scoring(df_parcels)

    counts = df_parcels["deal_tier"].value_counts()
    print(f"  Tier A: {counts.get('A', 0):,}  "
          f"B: {counts.get('B', 0):,}  "
          f"C: {counts.get('C', 0):,}  "
          f"D: {counts.get('D', 0):,}")

    # ââ 5. Output âââââââââââââââââââââââââââââââââââââ
    print("\n[5/5] Building output files...")
    df_out = build_output(df_parcels)

    metadata = {
        "generated_at":          datetime.now().isoformat(),
        "total_parcels_scanned": len(df_parcels),
        "top_deals_included":    len(df_out),
        "county":                "Chatham County, GA",
        "source":                "SAGIS ArcGIS REST APIs",
        "scoring_version":       "ChathamEngine v2",
    }
    payload = {"meta": metadata, "parcels": df_out.to_dict(orient="records")}

    with open(OUTPUT_JSON, "w") as fh:
        json.dump(payload, fh, indent=2, default=str)

    df_out.to_csv(OUTPUT_CSV, index=False)

    elapsed = round(time.time() - start)
    json_kb  = os.path.getsize(OUTPUT_JSON) // 1024

    print(f"\n{'='*55}")
    print(f"  Done in {elapsed}s")
    print(f"  JSON  â {OUTPUT_JSON}  ({json_kb} KB)")
    print(f"  CSV   â {OUTPUT_CSV}")
    if len(df_out):
        top = df_out.iloc[0]
        print(f"  #1 deal: {top['address']}  score={top['deal_score']} tier={top['deal_tier']}")
    print(f"{'='*55}")


if __name__ == "__main__":
    main()
