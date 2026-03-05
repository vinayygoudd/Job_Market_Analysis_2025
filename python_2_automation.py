"""
╔══════════════════════════════════════════════════════════════════╗
║   Job Market Analysis 2025 — Automation Pipeline                ║
║   Script 2 of 2 | Automates: Ingest → Clean → Analyse → Report ║
╚══════════════════════════════════════════════════════════════════╝

PURPOSE:
    End-to-end automation pipeline that:
      1. Ingests raw/messy CSV data
      2. Applies all cleaning transformations automatically
      3. Runs KPI calculations and anomaly detection
      4. Auto-generates a structured summary report (TXT + CSV)
      5. Sends console alerts for data quality issues
      6. Simulates scheduled / incremental refresh logic

USAGE:
    python python_2_automation.py
    python python_2_automation.py --input <path_to_csv>
    python python_2_automation.py --input <path> --output <dir>

DEPENDENCIES: pandas, numpy, argparse, logging, pathlib
"""

import pandas as pd
import numpy as np
import argparse
import logging
import sys
import re
import json
import time
from pathlib import Path
from datetime import datetime
from collections import Counter

# ─────────────────────────────────────────────────────────────────────────────
# 0. ARGUMENT PARSING & LOGGING SETUP
# ─────────────────────────────────────────────────────────────────────────────
def parse_args():
    parser = argparse.ArgumentParser(description="Job Market 2025 — Automation Pipeline")
    parser.add_argument("--input",  default="/mnt/user-data/outputs/job_market_messy_2025.csv",
                        help="Path to raw messy CSV file")
    parser.add_argument("--output", default="/mnt/user-data/outputs/",
                        help="Output directory for all artefacts")
    parser.add_argument("--threshold-salary-max", type=int, default=600000,
                        help="Max valid salary threshold")
    parser.add_argument("--threshold-yoe-max",    type=int, default=45,
                        help="Max valid years-of-experience")
    parser.add_argument("--alert-null-pct",        type=float, default=30.0,
                        help="Alert if any column has > X%% nulls")
    return parser.parse_args(args=[])   # defaults for standalone run

# ─────────────────────────────────────────────────────────────────────────────
# 1. LOGGER
# ─────────────────────────────────────────────────────────────────────────────
def setup_logger(log_path: str) -> logging.Logger:
    logger = logging.getLogger("JobMarketPipeline")
    logger.setLevel(logging.DEBUG)
    fmt = logging.Formatter("[%(asctime)s] %(levelname)-8s  %(message)s",
                            datefmt="%Y-%m-%d %H:%M:%S")
    # Console handler
    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(logging.INFO)
    ch.setFormatter(fmt)
    # File handler
    fh = logging.FileHandler(log_path)
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(fmt)
    logger.addHandler(ch)
    logger.addHandler(fh)
    return logger

# ─────────────────────────────────────────────────────────────────────────────
# 2. STEP DECORATOR — logs each pipeline stage with timing
# ─────────────────────────────────────────────────────────────────────────────
def pipeline_step(name: str):
    def decorator(fn):
        def wrapper(*args, **kwargs):
            logger = logging.getLogger("JobMarketPipeline")
            logger.info(f"{'─'*55}")
            logger.info(f"STEP ▶  {name}")
            start = time.perf_counter()
            result = fn(*args, **kwargs)
            elapsed = time.perf_counter() - start
            logger.info(f"       Completed in {elapsed:.3f}s")
            return result
        return wrapper
    return decorator

# ─────────────────────────────────────────────────────────────────────────────
# 3. INGESTION
# ─────────────────────────────────────────────────────────────────────────────
@pipeline_step("DATA INGESTION")
def ingest(path: str) -> pd.DataFrame:
    logger = logging.getLogger("JobMarketPipeline")
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(f"Input file not found: {path}")
    df = pd.read_csv(p, dtype=str)
    logger.info(f"       Loaded  : {len(df):,} rows × {df.shape[1]} cols from {p.name}")
    df.replace(r'^\s*$', np.nan, regex=True, inplace=True)
    empty_rows = df.isna().all(axis=1).sum()
    logger.info(f"       Fully empty rows flagged: {empty_rows}")
    df.dropna(how='all', inplace=True)
    return df

# ─────────────────────────────────────────────────────────────────────────────
# 4. DATA QUALITY AUDIT
# ─────────────────────────────────────────────────────────────────────────────
@pipeline_step("DATA QUALITY AUDIT")
def audit(df: pd.DataFrame, alert_null_pct: float) -> dict:
    logger = logging.getLogger("JobMarketPipeline")
    report = {
        "audit_timestamp"  : datetime.now().isoformat(),
        "total_rows"       : len(df),
        "total_columns"    : df.shape[1],
        "duplicate_rows"   : int(df.duplicated().sum()),
        "null_summary"     : {},
        "alerts"           : [],
    }
    for col in df.columns:
        null_cnt = int(df[col].isna().sum())
        null_pct = null_cnt / len(df) * 100
        report["null_summary"][col] = {"count": null_cnt, "pct": round(null_pct, 2)}
        if null_pct > alert_null_pct:
            msg = f"ALERT: '{col}' has {null_pct:.1f}% nulls (threshold={alert_null_pct}%)"
            report["alerts"].append(msg)
            logger.warning(f"       ⚠  {msg}")

    if report["duplicate_rows"] > 0:
        msg = f"ALERT: {report['duplicate_rows']} duplicate rows detected"
        report["alerts"].append(msg)
        logger.warning(f"       ⚠  {msg}")

    logger.info(f"       Null alerts raised: {len(report['alerts'])}")
    return report

# ─────────────────────────────────────────────────────────────────────────────
# 5. CLEANING (same logic as clean_data.py — wrapped in automation)
# ─────────────────────────────────────────────────────────────────────────────
TITLE_MAP = {
    r"data\s*scientist|data sci": "Data Scientist",
    r"software\s*eng|swe\b":      "Software Engineer",
    r"\bdata\s*analyst|\bda\b":   "Data Analyst",
    r"ml\s*eng|machine\s*learn":  "ML Engineer",
    r"backend|back.?end":         "Backend Developer",
    r"frontend|front.?end|front\s*dev": "Frontend Developer",
    r"devops":                    "DevOps Engineer",
    r"cloud\s*arch":              "Cloud Architect",
    r"product\s*man|\bpm\b":      "Product Manager",
    r"data\s*eng":                "Data Engineer",
    r"ai\s*eng":                  "AI Engineer",
    r"cyber|security\s*anal":     "Cybersecurity Analyst",
    r"full.?stack|fullstack":     "Full Stack Developer",
    r"business\s*intel|bi\s*anal":"BI Analyst",
}

EXP_MAP = {
    r"entry|junior|jr\b":       "Entry Level",
    r"mid|intermediate":        "Mid Level",
    r"senior|sr\b":             "Senior",
    r"lead|principal":          "Lead/Principal",
    r"manager|mgr":             "Manager",
    r"director":                "Director",
    r"executive|vp\b|cto|cpo":  "Executive",
}

LOCATION_MAP = {
    r"san\s*fran|sf,?\s*ca":  "San Francisco, CA",
    r"new\s*york|nyc":         "New York, NY",
    r"seattle":                "Seattle, WA",
    r"austin":                 "Austin, TX",
    r"chicago":                "Chicago, IL",
    r"boston":                 "Boston, MA",
    r"remote|wfh|work\s*from": "Remote",
    r"bangal|bengal":          "Bangalore, India",
    r"hyderab":                "Hyderabad, India",
    r"london":                 "London, UK",
    r"berlin":                 "Berlin, Germany",
    r"toronto":                "Toronto, Canada",
}

EDU_MAP = {
    r"phd|ph\.d|doctorate": "PhD",
    r"master|msc?|m\.s":    "Master's",
    r"bachelor|b\.s|^bs$":  "Bachelor's",
    r"associate":           "Associate's",
    r"high\s*school|hs\b":  "High School",
    r"bootcamp|boot\s*camp":"Bootcamp",
}

EMP_MAP = {
    r"full.?time|^ft$":   "Full-Time",
    r"part.?time|^pt$":   "Part-Time",
    r"contract":          "Contract",
    r"freelance|gig":     "Freelance",
    r"intern":            "Internship",
}

IND_MAP = {
    r"tech|it\b|software":   "Technology",
    r"financ|fintech|bank":  "Finance",
    r"health|med":           "Healthcare",
    r"e.?comm|retail":       "E-Commerce/Retail",
    r"edu|edtech":           "Education",
    r"consult":              "Consulting",
    r"telecom":              "Telecommunications",
    r"gam|game":             "Gaming",
    r"auto|ev\b":            "Automotive",
    r"media|entertain":      "Media/Entertainment",
}

def _apply_map(val, mapping, fallback="Unknown"):
    if pd.isna(val):
        return fallback
    v = str(val).strip().lower()
    for pat, label in mapping.items():
        if re.search(pat, v):
            return label
    return fallback

def _clean_salary(val, max_salary: int = 600000) -> float:
    if pd.isna(val):
        return np.nan
    v = str(val).strip()
    v = re.sub(r"[,$]|USD|INR|GBP|EUR", "", v, flags=re.I).strip()
    if v.lower().endswith("k"):
        try: return float(v[:-1]) * 1000
        except: return np.nan
    try:
        n = float(v)
        return n if 0 < n <= max_salary else np.nan
    except:
        return np.nan

def _clean_numeric(val, lo=0, hi=1e9) -> float:
    if pd.isna(val): return np.nan
    v = str(val).lower().replace("years","").replace("yrs","").replace("+","").strip()
    try:
        n = float(v)
        return n if lo <= n <= hi else np.nan
    except: return np.nan

def _clean_date(val) -> pd.Timestamp:
    if pd.isna(val): return pd.NaT
    v = str(val).strip()
    for fmt in ["%m/%d/%Y","%d-%m-%Y","%Y-%m-%d","%m/%d/%Y","%d.%m.%Y"]:
        try: return pd.to_datetime(v, format=fmt)
        except: continue
    try: return pd.to_datetime(v, infer_datetime_format=True)
    except: return pd.NaT

def _clean_rating(val) -> float:
    if pd.isna(val): return np.nan
    v = str(val).lower().strip()
    if v in ["n/a","na","none","excellent","good","average",""]: return np.nan
    v = v.replace("/5","").replace("/10","")
    try:
        n = float(v)
        if n > 5: n /= 2
        return round(n, 1) if 1 <= n <= 5 else np.nan
    except: return np.nan

def _clean_remote(val) -> str:
    if pd.isna(val): return "Unknown"
    v = str(val).lower().strip()
    if v in ["yes","1","true","remote"]: return "Remote"
    if v in ["no","0","false","onsite","on-site"]: return "On-Site"
    if v in ["hybrid","maybe"]: return "Hybrid"
    return "Unknown"

def _clean_skills(val) -> str:
    if pd.isna(val): return ""
    v = str(val).strip().strip("[]'\"")
    v = re.sub(r"[|/;]", ",", v)
    parts = [s.strip().strip("'\"") for s in v.split(",") if s.strip()]
    seen, out = set(), []
    for p in parts:
        if p.lower() not in seen and p:
            seen.add(p.lower())
            out.append(p.title())
    return ", ".join(out)

@pipeline_step("DATA CLEANING & TRANSFORMATION")
def clean(df: pd.DataFrame, max_salary: int, max_yoe: int) -> pd.DataFrame:
    logger = logging.getLogger("JobMarketPipeline")
    before = len(df)
    df.drop_duplicates(inplace=True)
    logger.info(f"       Removed {before - len(df)} duplicate rows")

    df["job_title"]        = df["job_title"].apply(lambda x: _apply_map(x, TITLE_MAP))
    df["experience_level"] = df["experience_level"].apply(lambda x: _apply_map(x, EXP_MAP))
    df["location"]         = df["location"].apply(lambda x: _apply_map(x, LOCATION_MAP))
    df["education"]        = df["education"].apply(lambda x: _apply_map(x, EDU_MAP))
    df["employment_type"]  = df["employment_type"].apply(lambda x: _apply_map(x, EMP_MAP))
    df["industry"]         = df["industry"].apply(lambda x: _apply_map(x, IND_MAP))
    df["remote_work"]      = df["remote_work"].apply(_clean_remote)
    df["skills"]           = df["skills"].apply(_clean_skills)

    df["salary_usd"] = df["salary"].apply(lambda x: _clean_salary(x, max_salary))
    df["salary_usd"] = df.groupby("job_title")["salary_usd"].transform(
        lambda x: x.fillna(x.median()))
    df["salary_usd"] = df["salary_usd"].fillna(df["salary_usd"].median()).round(0).astype(int)

    df["years_of_exp"] = df["years_of_exp"].apply(lambda x: _clean_numeric(x, 0, max_yoe))
    df["years_of_exp"] = df.groupby("experience_level")["years_of_exp"].transform(
        lambda x: x.fillna(x.median()))
    df["years_of_exp"] = df["years_of_exp"].fillna(df["years_of_exp"].median()).astype(int)

    df["company_rating"] = df["company_rating"].apply(_clean_rating)
    df["company_rating"] = df["company_rating"].fillna(df["company_rating"].median())

    df["applications"] = df["applications"].apply(lambda x: _clean_numeric(x, 0, 100000))
    df["applications"] = df["applications"].fillna(df["applications"].median()).astype(int)

    df["job_posted_date"] = df["job_posted_date"].apply(_clean_date)
    df["job_posted_date"] = df["job_posted_date"].fillna(pd.Timestamp("2025-01-01"))
    df["post_month"] = df["job_posted_date"].dt.to_period("M").astype(str)
    df["post_year"]  = df["job_posted_date"].dt.year

    def hiring_status_clean(v):
        if pd.isna(v): return "Unknown"
        v = str(v).lower().strip()
        return "Open" if v in ["open","active"] else "Closed" if v in ["closed","filled"] else "On Hold" if "hold" in v else "Unknown"
    df["hiring_status"] = df["hiring_status"].apply(hiring_status_clean)

    def salary_band(s):
        if   s < 50000:  return "< $50K"
        elif s < 80000:  return "$50K–$80K"
        elif s < 120000: return "$80K–$120K"
        elif s < 180000: return "$120K–$180K"
        else:            return "$180K+"
    df["salary_band"] = df["salary_usd"].apply(salary_band)

    df.drop(columns=["salary","currency","job_description"], inplace=True, errors="ignore")
    for col in df.select_dtypes(include=["object","string"]).columns:
        df[col] = df[col].fillna("Unknown")

    logger.info(f"       Final cleaned shape: {df.shape[0]:,} rows × {df.shape[1]} cols")
    return df

# ─────────────────────────────────────────────────────────────────────────────
# 6. KPI COMPUTATION ENGINE
# ─────────────────────────────────────────────────────────────────────────────
@pipeline_step("KPI COMPUTATION")
def compute_kpis(df: pd.DataFrame) -> dict:
    logger = logging.getLogger("JobMarketPipeline")

    all_skills = []
    for row in df["skills"]:
        if row and row != "Unknown":
            all_skills.extend([s.strip() for s in str(row).split(",") if s.strip()])

    open_df   = df[df["hiring_status"] == "Open"]
    remote_df = df[df["remote_work"]   == "Remote"]

    kpis = {
        "computed_at"             : datetime.now().isoformat(),
        "total_listings"          : int(len(df)),
        "open_positions"          : int(len(open_df)),
        "open_pct"                : round(len(open_df) / len(df) * 100, 1),
        "remote_pct"              : round(len(remote_df) / len(df) * 100, 1),
        "avg_salary_usd"          : int(df["salary_usd"].mean()),
        "median_salary_usd"       : int(df["salary_usd"].median()),
        "p90_salary_usd"          : int(df["salary_usd"].quantile(0.90)),
        "highest_paying_role"     : str(df.groupby("job_title")["salary_usd"].median().idxmax()),
        "highest_paying_industry" : str(df.groupby("industry")["salary_usd"].median().idxmax()),
        "highest_paying_location" : str(df.groupby("location")["salary_usd"].median().idxmax()),
        "most_demanded_role"      : str(df[df["job_title"] != "Unknown"]["job_title"].mode()[0]),
        "most_demanded_skill"     : Counter(all_skills).most_common(1)[0][0],
        "top_5_skills"            : [s for s, _ in Counter(all_skills).most_common(5)],
        "avg_company_rating"      : round(float(df["company_rating"].mean()), 2),
        "avg_applications_per_job": int(df["applications"].mean()),
        "avg_years_exp"           : round(float(df["years_of_exp"].mean()), 1),
        "total_unique_companies"  : int(df[df["company_name"] != "Unknown"]["company_name"].nunique()),
        "total_unique_locations"  : int(df[df["location"]     != "Unknown"]["location"].nunique()),
        "salary_by_exp": {
            k: int(v) for k, v in
            df.groupby("experience_level")["salary_usd"].median().to_dict().items()
        },
        "jobs_by_industry": {
            k: int(v) for k, v in
            df["industry"].value_counts().to_dict().items()
        },
        "top_companies_hiring": {
            k: int(v) for k, v in
            df[df["company_name"] != "Unknown"]["company_name"].value_counts().head(5).to_dict().items()
        },
    }

    for k, v in {k: v for k, v in kpis.items() if not isinstance(v, (dict, list))}.items():
        logger.info(f"       {k:<32} : {v}")

    return kpis

# ─────────────────────────────────────────────────────────────────────────────
# 7. ANOMALY / OUTLIER DETECTION
# ─────────────────────────────────────────────────────────────────────────────
@pipeline_step("ANOMALY DETECTION")
def detect_anomalies(df: pd.DataFrame) -> pd.DataFrame:
    logger = logging.getLogger("JobMarketPipeline")
    flags = []

    # Salary IQR outliers
    q1, q3 = df["salary_usd"].quantile([0.25, 0.75])
    iqr = q3 - q1
    salary_outliers = df[(df["salary_usd"] < q1 - 3*iqr) | (df["salary_usd"] > q3 + 3*iqr)]
    logger.warning(f"       Salary extreme outliers (3× IQR): {len(salary_outliers)}")

    # Applications outliers
    app_q3 = df["applications"].quantile(0.99)
    app_outliers = df[df["applications"] > app_q3]
    logger.info(f"       High-application outliers (>99th pct): {len(app_outliers)}")

    # Missing job_id
    missing_id = (df["job_id"] == "Unknown").sum()
    logger.info(f"       Records with missing job_id: {missing_id}")

    anomaly_report = pd.DataFrame({
        "check"         : ["salary_3xIQR_outliers","applications_99pct_outliers","missing_job_id"],
        "count"         : [len(salary_outliers), len(app_outliers), missing_id],
        "action_taken"  : ["flagged (retained)","flagged (retained)","filled with Unknown"],
    })
    return anomaly_report

# ─────────────────────────────────────────────────────────────────────────────
# 8. INCREMENTAL REFRESH SIMULATION
# ─────────────────────────────────────────────────────────────────────────────
@pipeline_step("INCREMENTAL REFRESH SIMULATION")
def simulate_incremental(df: pd.DataFrame) -> pd.DataFrame:
    """
    In a production pipeline this step would:
      - Pull only records newer than last_run_timestamp from source DB
      - Merge with existing cleaned dataset (UPSERT logic)
      - Re-compute only affected KPIs
    Here we simulate it with a synthetic "new batch".
    """
    logger = logging.getLogger("JobMarketPipeline")
    batch_size = 50
    new_batch  = df.sample(n=batch_size, random_state=99).copy()
    new_batch["job_posted_date"] = pd.Timestamp("2025-12-31")  # Simulate Dec refresh
    new_batch["post_month"]      = "2025-12"
    new_batch["post_year"]       = 2025

    refreshed = pd.concat([df, new_batch], ignore_index=True)
    refreshed.drop_duplicates(subset=[c for c in df.columns if c != "job_posted_date"],
                               inplace=True)
    logger.info(f"       Base records   : {len(df):,}")
    logger.info(f"       New batch size : {batch_size}")
    logger.info(f"       After merge    : {len(refreshed):,} (after dedup)")
    return refreshed

# ─────────────────────────────────────────────────────────────────────────────
# 9. REPORT GENERATION
# ─────────────────────────────────────────────────────────────────────────────
@pipeline_step("REPORT GENERATION")
def generate_report(df: pd.DataFrame, kpis: dict, audit_report: dict,
                    anomalies: pd.DataFrame, output_dir: str):
    logger = logging.getLogger("JobMarketPipeline")
    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")

    # ── a) Cleaned dataset ────────────────────────────────────────────────────
    clean_csv = out / "job_market_cleaned_auto.csv"
    df.to_csv(clean_csv, index=False)
    logger.info(f"       Cleaned CSV  → {clean_csv.name}")

    # ── b) KPI JSON ───────────────────────────────────────────────────────────
    kpi_json = out / "kpi_summary.json"
    with open(kpi_json, "w") as f:
        json.dump(kpis, f, indent=2, default=str)
    logger.info(f"       KPI JSON     → {kpi_json.name}")

    # ── c) Salary aggregation CSV ─────────────────────────────────────────────
    sal_agg = df.groupby(["job_title","experience_level"])["salary_usd"].agg(
        count="count", median="median", mean="mean", q25=lambda x: x.quantile(0.25),
        q75=lambda x: x.quantile(0.75)
    ).reset_index()
    sal_agg.to_csv(out / "salary_aggregation.csv", index=False)

    # ── d) Skills frequency CSV ───────────────────────────────────────────────
    all_skills = []
    for row in df["skills"]:
        if row and row != "Unknown":
            all_skills.extend([s.strip() for s in str(row).split(",") if s.strip()])
    skill_df = pd.DataFrame(Counter(all_skills).most_common(50),
                            columns=["skill", "mention_count"])
    skill_df["pct_of_jobs"] = (skill_df["mention_count"] / len(df) * 100).round(1)
    skill_df.to_csv(out / "skills_frequency.csv", index=False)

    # ── e) Anomaly report ─────────────────────────────────────────────────────
    anomalies.to_csv(out / "anomaly_report.csv", index=False)

    # ── f) Executive summary TXT ──────────────────────────────────────────────
    summary_path = out / "pipeline_summary_report.txt"
    with open(summary_path, "w") as f:
        f.write("=" * 68 + "\n")
        f.write("  JOB MARKET 2025 — AUTOMATED PIPELINE SUMMARY REPORT\n")
        f.write(f"  Generated : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write("=" * 68 + "\n\n")

        f.write("DATA QUALITY AUDIT\n" + "─" * 40 + "\n")
        f.write(f"  Input rows         : {audit_report['total_rows']:,}\n")
        f.write(f"  Duplicate rows     : {audit_report['duplicate_rows']:,}\n")
        f.write(f"  Quality alerts     : {len(audit_report['alerts'])}\n")
        for alert in audit_report['alerts'][:5]:
            f.write(f"    • {alert}\n")
        f.write("\n")

        f.write("KEY PERFORMANCE INDICATORS\n" + "─" * 40 + "\n")
        f.write(f"  Total Listings          : {kpis['total_listings']:,}\n")
        f.write(f"  Open Positions          : {kpis['open_positions']:,} ({kpis['open_pct']}%)\n")
        f.write(f"  Remote Jobs             : {kpis['remote_pct']}%\n")
        f.write(f"  Avg Salary (USD)        : ${kpis['avg_salary_usd']:,}\n")
        f.write(f"  Median Salary (USD)     : ${kpis['median_salary_usd']:,}\n")
        f.write(f"  P90 Salary (USD)        : ${kpis['p90_salary_usd']:,}\n")
        f.write(f"  Highest Paying Role     : {kpis['highest_paying_role']}\n")
        f.write(f"  Highest Paying Industry : {kpis['highest_paying_industry']}\n")
        f.write(f"  Most Demanded Role      : {kpis['most_demanded_role']}\n")
        f.write(f"  Top 5 Skills            : {', '.join(kpis['top_5_skills'])}\n")
        f.write(f"  Avg Company Rating      : {kpis['avg_company_rating']} / 5.0\n")
        f.write(f"  Avg Applications/Job    : {kpis['avg_applications_per_job']}\n\n")

        f.write("SALARY BY EXPERIENCE LEVEL\n" + "─" * 40 + "\n")
        for lvl, sal in sorted(kpis["salary_by_exp"].items(),
                               key=lambda x: x[1], reverse=True):
            f.write(f"  {lvl:<22} ${sal:>8,}\n")
        f.write("\n")

        f.write("TOP COMPANIES HIRING\n" + "─" * 40 + "\n")
        for co, cnt in kpis["top_companies_hiring"].items():
            f.write(f"  {co:<25} {cnt:>4} listings\n")
        f.write("\n")

        f.write("ANOMALY DETECTION RESULTS\n" + "─" * 40 + "\n")
        for _, row in anomalies.iterrows():
            f.write(f"  {row['check']:<35} count={row['count']:>4}  action={row['action_taken']}\n")
        f.write("\n")

        f.write("OUTPUT ARTEFACTS\n" + "─" * 40 + "\n")
        artefacts = [
            "job_market_cleaned_auto.csv",
            "kpi_summary.json",
            "salary_aggregation.csv",
            "skills_frequency.csv",
            "anomaly_report.csv",
            "pipeline_summary_report.txt",
            "pipeline.log",
        ]
        for art in artefacts:
            f.write(f"  • {art}\n")
        f.write("\n" + "=" * 68 + "\n")

    logger.info(f"       Summary TXT  → {summary_path.name}")
    logger.info(f"       All artefacts saved to: {output_dir}")

# ─────────────────────────────────────────────────────────────────────────────
# 10. MAIN PIPELINE ORCHESTRATOR
# ─────────────────────────────────────────────────────────────────────────────
def run_pipeline():
    args       = parse_args()
    log_path   = str(Path(args.output) / "pipeline.log")
    Path(args.output).mkdir(parents=True, exist_ok=True)
    logger     = setup_logger(log_path)

    logger.info("=" * 55)
    logger.info("  JOB MARKET 2025 — AUTOMATION PIPELINE STARTED")
    logger.info(f"  Input  : {args.input}")
    logger.info(f"  Output : {args.output}")
    logger.info("=" * 55)

    total_start = time.perf_counter()

    # Pipeline stages
    df_raw        = ingest(args.input)
    audit_report  = audit(df_raw, args.alert_null_pct)
    df_clean      = clean(df_raw.copy(), args.threshold_salary_max, args.threshold_yoe_max)
    kpis          = compute_kpis(df_clean)
    anomalies     = detect_anomalies(df_clean)
    _             = simulate_incremental(df_clean)
    generate_report(df_clean, kpis, audit_report, anomalies, args.output)

    total_elapsed = time.perf_counter() - total_start
    logger.info("=" * 55)
    logger.info(f"  PIPELINE COMPLETE  |  Total time: {total_elapsed:.2f}s")
    logger.info("=" * 55)


if __name__ == "__main__":
    run_pipeline()
