"""
merge_btt_census.py
===================
Merge election data with INSEE BTT_TD_POP1A census files (population by age
and sex, long format) and write to newest_france_joined_outputs/.

Census file → election year mapping
------------------------------------
  BTT_TD_POP1A_2008.txt  →  2008 elections
  BTT_TD_POP1A_2014.txt  →  2014 elections
  BTT_TD_POP1A_2019.csv  →  2020 elections  (closest available proxy)
  BTT_TD_POP1A_2022.csv  →  2026 elections  (closest available proxy)

Available demographics (from BTT files)
-----------------------------------------
  P{YY}_POP     — total commune population
  pct_female / pct_male
  pct_age_0_14 … pct_age_75_plus  (approximated from BTT 10 age bands using
                                   uniform-distribution interpolation)

NOT available from BTT files (absent in output)
------------------------------------------------
  pct_edu_vocational, pct_edu_bac, pct_edu_higher, pct_unemployed

Age band interpolation
-----------------------
BTT AGEPYR10 groups → target 15-year bands (assuming uniform distribution
within each BTT group):
  00 (0–2)    03 (3–5)    06 (6–10)    11 (11–17)    18 (18–24)
  25 (25–39)  40 (40–54)  55 (55–64)   65 (65–79)    80 (80+)

  0–14  = 00+03+06 + (4/7)×11
  15–29 = (3/7)×11 + 18 + (1/3)×25
  30–44 = (2/3)×25 + (1/3)×40
  45–59 = (2/3)×40 + (1/2)×55
  60–74 = (1/2)×55 + (2/3)×65
  75+   = (1/3)×65 + 80

Commune code crosswalk
-----------------------
  new_france_census/table_passage_annuelle_2026.xlsx maps commune codes across
  years 2003–2026. Used to patch unmatched codes after the direct join.

Join key
---------
  election commune_code → PLM SR/SN suffix stripped → match CODGEO in BTT.
  Unmatched → look up election code in CODGEO_{election_year} column of
  crosswalk → translate to CODGEO_{census_year} → retry join.
"""

import re
import pandas as pd
from pathlib import Path

BASE            = Path("/Users/propadiene/cloned-repos/cities-webscraper")
DOSSIER_COMPLET = BASE / "archive/france_census/dossier_complet.csv"
BTT_DIR  = BASE / "new_france_census"
IN_DIR   = BASE / "new_france_joined_outputs"
OUT_DIR  = BASE / "newest_france_joined_outputs"

# BTT file configs: (path, encoding, CODGEO year used in crosswalk)
BTT_FILES = {
    "2008": (BTT_DIR / "BTT_TD_POP1A_2008.txt",  "latin-1", "2008"),
    "2014": (BTT_DIR / "BTT_TD_POP1A_2014.txt",  "latin-1", "2014"),
    "2019": (BTT_DIR / "BTT_TD_POP1A_2019.csv",  "utf-8",   "2019"),
    "2022": (BTT_DIR / "BTT_TD_POP1A_2022.csv",  "utf-8",   "2022"),
}

# Election year → (BTT key, census_year for crosswalk lookup, pop column name)
YEAR_CONFIG = {
    "2008": ("2008", "2008", "P08_POP"),
    "2014": ("2014", "2014", "P14_POP"),
    "2020": ("2019", "2019", "P19_POP"),
    "2026": ("2022", "2022", "P22_POP"),
}

# Input files: (election_year, label, relative path inside IN_DIR)
INPUT_FILES = [
    ("2008", "plus_1000_tour1",  "france_joined_2008/joined_plus_1000_tour1_2008.csv"),
    ("2008", "plus_1000_tour2",  "france_joined_2008/joined_plus_1000_tour2_2008.csv"),
    ("2014", "plus_1000_tour1",  "france_joined_2014/joined_plus_1000_tour1_2014.csv"),
    ("2014", "plus_1000_tour2",  "france_joined_2014/joined_plus_1000_tour2_2014.csv"),
    ("2014", "less_1000_tour1",  "france_joined_2014/joined_less_1000_tour1_2014.csv"),
    ("2014", "less_1000_tour2",  "france_joined_2014/joined_less_1000_tour2_2014.csv"),
    ("2020", "plus_1000_tour1",  "france_joined_2020/joined_plus_1000_tour1_2020.csv"),
    ("2020", "plus_1000_tour2",  "france_joined_2020/joined_plus_1000_tour2_2020.csv"),
    ("2020", "less_1000_tour1",  "france_joined_2020/joined_less_1000_tour1_2020.csv"),
    ("2020", "less_1000_tour2",  "france_joined_2020/joined_less_1000_tour2_2020.csv"),
    ("2026", "tour1",            "france_joined_2026/joined_tour1_2026.csv"),
    ("2026", "tour2",            "france_joined_2026/joined_tour2_2026.csv"),
]

ELECTION_COLS = [
    "commune_code", "commune_name",
    "last_name", "first_name", "gender",
    "party_code", "list_name",
    "votes", "elected",
]


# ---------------------------------------------------------------------------
# Census loading & pivoting
# ---------------------------------------------------------------------------

def load_btt(btt_key: str) -> pd.DataFrame:
    """Load BTT file and pivot to one row per commune with pop/gender/age cols."""
    path, enc, _ = BTT_FILES[btt_key]
    df = pd.read_csv(path, sep=";", dtype=str, encoding=enc, low_memory=False)
    df["NB"] = pd.to_numeric(df["NB"], errors="coerce").fillna(0)

    # Normalise column names (2008 uses different names)
    df = df.rename(columns={
        "NIVEAU":     "NIVGEO",
        "C_SEXE":     "SEXE",
        "C_AGEPYR10": "AGEPYR10",
    })

    df = df[df["NIVGEO"] == "COM"].copy()

    total_pop  = df.groupby("CODGEO")["NB"].sum()
    gender_pop = df.groupby(["CODGEO", "SEXE"])["NB"].sum().unstack(fill_value=0)
    # SEXE 1=male, 2=female
    gender_pop = gender_pop.rename(columns={"1": "POP_M", "2": "POP_F"})
    for col in ["POP_M", "POP_F"]:
        if col not in gender_pop.columns:
            gender_pop[col] = 0.0

    age_pop = df.groupby(["CODGEO", "AGEPYR10"])["NB"].sum().unstack(fill_value=0)
    age_pop.columns = [f"AGE_{c}" for c in age_pop.columns]
    # Ensure all 10 age group columns exist
    for grp in ["00", "03", "06", "11", "18", "25", "40", "55", "65", "80"]:
        if f"AGE_{grp}" not in age_pop.columns:
            age_pop[f"AGE_{grp}"] = 0.0

    result = pd.concat([total_pop.rename("POP_TOTAL"), gender_pop, age_pop], axis=1).reset_index()
    print(f"    BTT {btt_key}: {len(result):,} communes loaded")
    return result


def compute_derived(btt: pd.DataFrame, pop_col: str) -> pd.DataFrame:
    """Compute percentage columns from pivoted BTT data."""
    pop = btt["POP_TOTAL"].replace(0, float("nan"))

    btt[pop_col]          = btt["POP_TOTAL"]
    btt["pct_female"]     = btt["POP_F"] / pop * 100
    btt["pct_male"]       = btt["POP_M"] / pop * 100

    # Age interpolation (uniform distribution within BTT groups)
    a = {g: btt[f"AGE_{g}"] for g in ["00","03","06","11","18","25","40","55","65","80"]}
    btt["pct_age_0_14"]   = (a["00"] + a["03"] + a["06"] + (4/7)  * a["11"]) / pop * 100
    btt["pct_age_15_29"]  = ((3/7)  * a["11"] + a["18"] + (1/3)  * a["25"]) / pop * 100
    btt["pct_age_30_44"]  = ((2/3)  * a["25"] + (1/3)  * a["40"]) / pop * 100
    btt["pct_age_45_59"]  = ((2/3)  * a["40"] + (1/2)  * a["55"]) / pop * 100
    btt["pct_age_60_74"]  = ((1/2)  * a["55"] + (2/3)  * a["65"]) / pop * 100
    btt["pct_age_75_plus"]= ((1/3)  * a["65"] + a["80"]) / pop * 100

    pct_cols = [pop_col, "pct_female", "pct_male",
                "pct_age_0_14", "pct_age_15_29", "pct_age_30_44",
                "pct_age_45_59", "pct_age_60_74", "pct_age_75_plus"]
    btt[pct_cols] = btt[pct_cols].round(2)
    return btt[["CODGEO"] + pct_cols]


# ---------------------------------------------------------------------------
# dossier_complet fallback (2008 only — BTT 2008 suppresses small communes)
# ---------------------------------------------------------------------------

_dossier_cache = None

def load_dossier_complet_2008() -> pd.DataFrame:
    """
    Load the P11 columns from dossier_complet.csv and derive the same
    percentage columns as compute_derived(), but using the wide-format
    dossier_complet structure (same logic as merge_demographics.py P11 path).
    Output: one row per commune with CODGEO + P08_POP + pct_* columns.
    Note: P11_POP is used as a proxy for 2008 (same census product, closest year).
    """
    global _dossier_cache
    if _dossier_cache is not None:
        return _dossier_cache

    p = "P11_"
    cols_needed = [
        "CODGEO",
        f"{p}POP", f"{p}POPH", f"{p}POPF",
        f"{p}POP0014", f"{p}POP1529", f"{p}POP3044",
        f"{p}POP4559", f"{p}POP6074", f"{p}POP7589", f"{p}POP90P",
    ]
    df = pd.read_csv(
        DOSSIER_COMPLET, sep=";", dtype={"CODGEO": str},
        usecols=lambda c: c in cols_needed,
        low_memory=False, on_bad_lines="skip",
    )
    for col in cols_needed[1:]:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    pop = df[f"{p}POP"].replace(0, float("nan"))
    df["P08_POP"]        = df[f"{p}POP"]
    df["pct_female"]     = df[f"{p}POPF"]  / pop * 100
    df["pct_male"]       = df[f"{p}POPH"]  / pop * 100
    df["pct_age_0_14"]   = df[f"{p}POP0014"] / pop * 100
    df["pct_age_15_29"]  = df[f"{p}POP1529"] / pop * 100
    df["pct_age_30_44"]  = df[f"{p}POP3044"] / pop * 100
    df["pct_age_45_59"]  = df[f"{p}POP4559"] / pop * 100
    df["pct_age_60_74"]  = df[f"{p}POP6074"] / pop * 100
    df["pct_age_75_plus"]= (df[f"{p}POP7589"] + df[f"{p}POP90P"]) / pop * 100

    pct_cols = ["P08_POP", "pct_female", "pct_male",
                "pct_age_0_14", "pct_age_15_29", "pct_age_30_44",
                "pct_age_45_59", "pct_age_60_74", "pct_age_75_plus"]
    df[pct_cols] = df[pct_cols].round(2)
    _dossier_cache = df[["CODGEO"] + pct_cols].copy()
    print(f"    dossier_complet (P11 proxy for 2008): {len(_dossier_cache):,} communes loaded")
    return _dossier_cache


# ---------------------------------------------------------------------------
# Crosswalk loading
# ---------------------------------------------------------------------------

_crosswalk_cache = None

def load_crosswalk() -> pd.DataFrame:
    global _crosswalk_cache
    if _crosswalk_cache is None:
        xl = pd.read_excel(
            BTT_DIR / "table_passage_annuelle_2026.xlsx",
            header=5, dtype=str  # row 5 has machine-readable names (NIVGEO, CODGEO_2003, ...)
        )
        _crosswalk_cache = xl  # keep all rows; filter per-lookup in crosswalk_lookup()
        print(f"  Crosswalk loaded: {len(_crosswalk_cache):,} rows")
    return _crosswalk_cache


def crosswalk_lookup(codes: pd.Series, from_year: str, to_year: str) -> pd.Series:
    """Map commune codes from one year's geography to another using the crosswalk."""
    cw = load_crosswalk()
    from_col = f"CODGEO_{from_year}"
    to_col   = f"CODGEO_{to_year}"
    if from_col not in cw.columns or to_col not in cw.columns:
        print(f"  WARNING: crosswalk columns {from_col} or {to_col} not found")
        return pd.Series([None] * len(codes), index=codes.index)
    if from_col == to_col:
        # Same year: identity mapping — codes are already in the right geography
        return codes.copy()
    mapping = (
        cw.dropna(subset=[from_col, to_col])
        .drop_duplicates(subset=[from_col])
        .set_index(from_col)[to_col]
    )
    return codes.map(mapping)


# ---------------------------------------------------------------------------
# Main merge function
# ---------------------------------------------------------------------------

def process_file(election_year: str, label: str, rel_path: str):
    in_path = IN_DIR / rel_path
    out_subdir = OUT_DIR / in_path.parent.name
    out_path   = out_subdir / in_path.name

    print(f"\n{'='*60}")
    print(f"  {election_year} {label}")
    print(f"  Input:  {in_path}")
    print(f"  Output: {out_path}")

    btt_key, census_year, pop_col = YEAR_CONFIG[election_year]

    # --- Load election data ---
    elections = pd.read_csv(in_path, dtype={"commune_code": str}, low_memory=False)
    total_rows = len(elections)
    election_cols_present = [c for c in ELECTION_COLS if c in elections.columns]
    df = elections[election_cols_present].copy()
    print(f"  Election rows: {total_rows:,}  |  Communes: {df['commune_code'].nunique():,}")

    # --- Load & derive census stats ---
    # 2008: BTT file suppresses small communes (NB=0); use dossier_complet P11 instead
    if election_year == "2008":
        btt = load_dossier_complet_2008()
    else:
        btt_raw = load_btt(btt_key)
        btt     = compute_derived(btt_raw, pop_col)

    # --- Primary join (strip PLM SR/SN suffix) ---
    merge_code = df["commune_code"].str.replace(r"(SR|SN)\d+$", "", regex=True)
    merged = df.assign(merge_code=merge_code).merge(
        btt, left_on="merge_code", right_on="CODGEO", how="left"
    ).drop(columns=["merge_code", "CODGEO"])

    main_matched = merged[pop_col].notna().sum()
    unmatched_codes = merged[merged[pop_col].isna()]["commune_code"].unique()
    print(f"  Matched (direct):  {main_matched:,} / {total_rows:,} "
          f"({main_matched/total_rows*100:.1f}%)")
    if len(unmatched_codes) > 0:
        print(f"  Unmatched after direct join: {total_rows - main_matched:,} rows "
              f"across {len(unmatched_codes):,} communes")

    # --- Crosswalk patch ---
    patched_count = 0
    patched_communes = []
    if len(unmatched_codes) > 0:
        # Translate unmatched election codes from election_year geography → census_year geography
        unmatched_series  = pd.Series(unmatched_codes)
        translated        = crosswalk_lookup(unmatched_series, election_year, census_year)
        code_map          = dict(zip(unmatched_codes, translated))

        pct_cols = [c for c in merged.columns if c.startswith("pct_") or c == pop_col]
        unmatched_mask    = merged[pop_col].isna()
        translated_codes  = (
            merged.loc[unmatched_mask, "commune_code"]
            .str.replace(r"(SR|SN)\d+$", "", regex=True)
            .map(code_map)
        )
        patch_lookup = btt.set_index("CODGEO")
        for col in pct_cols:
            merged.loc[unmatched_mask, col] = translated_codes.map(
                patch_lookup[col] if col in patch_lookup.columns else pd.Series(dtype=float)
            ).values

        patched_mask     = unmatched_mask & merged[pop_col].notna()
        patched_communes = sorted(
            merged.loc[patched_mask, "commune_code"].unique()
        )
        patched_count    = patched_mask.sum()
        total_matched    = main_matched + patched_count
        print(f"  Patched via crosswalk: {patched_count:,} rows across "
              f"{len(patched_communes):,} communes")
        print(f"  Matched (incl. patch): {total_matched:,} / {total_rows:,} "
              f"({total_matched/total_rows*100:.1f}%)")

    still_unmatched = merged[merged[pop_col].isna()]["commune_code"].unique()
    print(f"  Still unmatched:   {len(still_unmatched):,} communes")

    # --- Save unmatched list ---
    out_subdir.mkdir(parents=True, exist_ok=True)
    unmatched_path = out_subdir / f"unmatched_communes_{label}.txt"
    unmatched_path.write_text("\n".join(sorted(still_unmatched)), encoding="utf-8")
    if len(still_unmatched) > 0:
        print(f"  Unmatched communes → {unmatched_path}")
    else:
        print(f"  No unmatched communes — {unmatched_path} cleared")

    # --- Save patched communes list ---
    if patched_communes:
        patch_path = out_subdir / f"patched_communes_{label}.csv"
        pd.DataFrame({
            "commune_code":    patched_communes,
            "census_year_used": census_year,
        }).to_csv(patch_path, index=False, encoding="utf-8-sig")
        print(f"  Patched communes → {patch_path}")

    # --- Write output ---
    merged = merged.drop(columns=["CODGEO"], errors="ignore")
    merged.to_csv(out_path, index=False, encoding="utf-8-sig")
    merged.to_json(out_path.with_suffix(".json"), orient="records",
                   force_ascii=False, indent=2)
    print(f"  Written: {len(merged):,} rows → {out_path}")
    print(f"  Columns: {list(merged.columns)}")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    print("merge_btt_census.py — merging BTT population census into election data")
    print(f"Output directory: {OUT_DIR}\n")

    for election_year, label, rel_path in INPUT_FILES:
        process_file(election_year, label, rel_path)

    print("\n\nDone.")
