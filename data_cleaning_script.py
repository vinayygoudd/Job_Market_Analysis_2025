"""
Job Market 2025 — Data Cleaning Script
Transforms messy raw CSV → clean, analysis-ready CSV
"""

import pandas as pd
import numpy as np
import re

# ── 1. Load ──────────────────────────────────────────────────────────────────
df = pd.read_csv("/mnt/user-data/outputs/job_market_messy_2025.csv", dtype=str)
print(f"Raw shape: {df.shape}")

# ── 2. Drop fully empty rows ──────────────────────────────────────────────────
df.replace(r'^\s*$', np.nan, regex=True, inplace=True)
df.dropna(how='all', inplace=True)

# ── 3. Remove duplicates ─────────────────────────────────────────────────────
before_dup = len(df)
df.drop_duplicates(inplace=True)
print(f"Dropped duplicates: {before_dup - len(df)}")

# ── 4. Standardise JOB TITLE ─────────────────────────────────────────────────
title_map = {
    r"data\s*scientist|data sci": "Data Scientist",
    r"software\s*eng|swe\b": "Software Engineer",
    r"\bdata\s*analyst|\bda\b": "Data Analyst",
    r"ml\s*eng|machine\s*learn\s*eng": "ML Engineer",
    r"backend\s*dev|back.?end": "Backend Developer",
    r"frontend\s*dev|front.?end|front\s*dev": "Frontend Developer",
    r"devops": "DevOps Engineer",
    r"cloud\s*arch": "Cloud Architect",
    r"product\s*man|\bpm\b": "Product Manager",
    r"data\s*eng": "Data Engineer",
    r"ai\s*eng": "AI Engineer",
    r"cyber|security\s*anal": "Cybersecurity Analyst",
    r"full.?stack|fullstack": "Full Stack Developer",
    r"business\s*intel|bi\s*anal": "BI Analyst",
}

def clean_title(val):
    if pd.isna(val):
        return "Unknown"
    v = str(val).strip().lower()
    for pat, label in title_map.items():
        if re.search(pat, v):
            return label
    return str(val).strip().title()

df["job_title"] = df["job_title"].apply(clean_title)

# ── 5. Standardise COMPANY NAME ──────────────────────────────────────────────
company_map = {
    r"^googl?e?$|googl\b": "Google",
    r"microsoft|microso": "Microsoft",
    r"amazon|aws\b": "Amazon",
    r"meta\b|facebook": "Meta",
    r"apple": "Apple",
    r"netflix": "Netflix",
    r"uber": "Uber",
    r"airbnb": "Airbnb",
    r"salesforce": "Salesforce",
    r"\bibm\b|i\.b\.m": "IBM",
    r"oracle": "Oracle",
    r"infosys": "Infosys",
    r"tcs|tata\s*consult": "TCS",
    r"wipro": "Wipro",
    r"accenture": "Accenture",
}

def clean_company(val):
    if pd.isna(val):
        return "Unknown"
    v = str(val).strip().lower()
    for pat, label in company_map.items():
        if re.search(pat, v):
            return label
    return str(val).strip().title()

df["company_name"] = df["company_name"].apply(clean_company)

# ── 6. Standardise LOCATION ──────────────────────────────────────────────────
location_map = {
    r"san\s*fran|sf,?\s*ca": "San Francisco, CA",
    r"new\s*york|nyc": "New York, NY",
    r"seattle": "Seattle, WA",
    r"austin": "Austin, TX",
    r"chicago": "Chicago, IL",
    r"boston": "Boston, MA",
    r"remote|work\s*from\s*home|wfh": "Remote",
    r"bangal|bengal": "Bangalore, India",
    r"hyderab": "Hyderabad, India",
    r"london": "London, UK",
    r"berlin": "Berlin, Germany",
    r"toronto": "Toronto, Canada",
}

def clean_location(val):
    if pd.isna(val):
        return "Unknown"
    v = str(val).strip().lower()
    for pat, label in location_map.items():
        if re.search(pat, v):
            return label
    return str(val).strip().title()

df["location"] = df["location"].apply(clean_location)

# ── 7. Clean SALARY → numeric USD ────────────────────────────────────────────
def clean_salary(val):
    if pd.isna(val):
        return np.nan
    v = str(val).strip().replace(",", "").replace("$", "").replace("USD", "").replace("INR","").replace("GBP","").replace("EUR","").strip()
    # Handle "120k"
    if v.lower().endswith("k"):
        try:
            return float(v[:-1]) * 1000
        except:
            return np.nan
    try:
        num = float(v)
        if num <= 0 or num > 1_000_000:   # outliers / bad values
            return np.nan
        return num
    except:
        return np.nan

df["salary_usd"] = df["salary"].apply(clean_salary)
# Impute missing salaries with median per job title
df["salary_usd"] = df.groupby("job_title")["salary_usd"].transform(
    lambda x: x.fillna(x.median())
)
df["salary_usd"] = df["salary_usd"].fillna(df["salary_usd"].median()).round(0).astype(int)

# ── 8. Clean EXPERIENCE LEVEL ────────────────────────────────────────────────
exp_map = {
    r"entry|junior|jr\b": "Entry Level",
    r"mid|intermediate|middle": "Mid Level",
    r"senior|sr\b": "Senior",
    r"lead|principal": "Lead/Principal",
    r"manager|mgr": "Manager",
    r"director": "Director",
    r"executive|vp\b|c-level|cto|cpo": "Executive",
}

def clean_exp(val):
    if pd.isna(val):
        return "Unknown"
    v = str(val).strip().lower()
    for pat, label in exp_map.items():
        if re.search(pat, v):
            return label
    return "Unknown"

df["experience_level"] = df["experience_level"].apply(clean_exp)

# ── 9. Clean YEARS OF EXPERIENCE ─────────────────────────────────────────────
def clean_yoe(val):
    if pd.isna(val):
        return np.nan
    v = str(val).strip().lower().replace("years","").replace("yrs","").strip()
    try:
        n = float(v)
        if n < 0 or n > 45:
            return np.nan
        return round(n)
    except:
        return np.nan

df["years_of_exp"] = df["years_of_exp"].apply(clean_yoe)
df["years_of_exp"] = df.groupby("experience_level")["years_of_exp"].transform(
    lambda x: x.fillna(x.median())
)
df["years_of_exp"] = df["years_of_exp"].fillna(df["years_of_exp"].median()).astype(int)

# ── 10. Clean EDUCATION ──────────────────────────────────────────────────────
edu_map = {
    r"phd|ph\.d|doctorate|doctor": "PhD",
    r"master|msc?|m\.s": "Master's",
    r"bachelor|b\.s|^bs$|^ba$": "Bachelor's",
    r"associate": "Associate's",
    r"high\s*school|hs\b|12th": "High School",
    r"bootcamp|boot\s*camp|coding": "Bootcamp",
}

def clean_edu(val):
    if pd.isna(val):
        return "Unknown"
    v = str(val).strip().lower()
    for pat, label in edu_map.items():
        if re.search(pat, v):
            return label
    return "Unknown"

df["education"] = df["education"].apply(clean_edu)

# ── 11. Clean EMPLOYMENT TYPE ────────────────────────────────────────────────
emp_map = {
    r"full.?time|^ft$|fulltime": "Full-Time",
    r"part.?time|^pt$": "Part-Time",
    r"contract|contractor": "Contract",
    r"freelance|gig": "Freelance",
    r"intern": "Internship",
}

def clean_emp(val):
    if pd.isna(val):
        return "Unknown"
    v = str(val).strip().lower()
    for pat, label in emp_map.items():
        if re.search(pat, v):
            return label
    return "Unknown"

df["employment_type"] = df["employment_type"].apply(clean_emp)

# ── 12. Clean INDUSTRY ───────────────────────────────────────────────────────
ind_map = {
    r"tech|it\b|information\s*tech|software": "Technology",
    r"financ|fintech|banking|bank": "Finance",
    r"health|med": "Healthcare",
    r"e.?comm|retail": "E-Commerce/Retail",
    r"edu|edtech": "Education",
    r"consult": "Consulting",
    r"telecom|telco": "Telecommunications",
    r"gam|game": "Gaming",
    r"auto|ev\b|electric\s*vehicle": "Automotive",
    r"media|entertainment": "Media/Entertainment",
}

def clean_industry(val):
    if pd.isna(val):
        return "Unknown"
    v = str(val).strip().lower()
    for pat, label in ind_map.items():
        if re.search(pat, v):
            return label
    return "Unknown"

df["industry"] = df["industry"].apply(clean_industry)

# ── 13. Clean REMOTE WORK ────────────────────────────────────────────────────
def clean_remote(val):
    if pd.isna(val):
        return "Unknown"
    v = str(val).strip().lower()
    if v in ["yes","1","true","remote"]:
        return "Remote"
    if v in ["no","0","false","onsite","on-site"]:
        return "On-Site"
    if v in ["hybrid","maybe","partial"]:
        return "Hybrid"
    return "Unknown"

df["remote_work"] = df["remote_work"].apply(clean_remote)

# ── 14. Clean DATE ───────────────────────────────────────────────────────────
def clean_date(val):
    if pd.isna(val):
        return pd.NaT
    v = str(val).strip()
    for fmt in ["%m/%d/%Y","%d-%m-%Y","%Y-%m-%d","%m/%d/%Y","%d.%m.%Y"]:
        try:
            return pd.to_datetime(v, format=fmt)
        except:
            continue
    try:
        return pd.to_datetime(v, infer_datetime_format=True)
    except:
        return pd.NaT

df["job_posted_date"] = df["job_posted_date"].apply(clean_date)
df["job_posted_date"] = df["job_posted_date"].fillna(pd.Timestamp("2025-01-01"))
df["post_month"] = df["job_posted_date"].dt.to_period("M").astype(str)
df["post_year"]  = df["job_posted_date"].dt.year

# ── 15. Clean COMPANY RATING ─────────────────────────────────────────────────
def clean_rating(val):
    if pd.isna(val):
        return np.nan
    v = str(val).strip().lower()
    if v in ["n/a","na","none","excellent","good","average",""]:
        return np.nan
    v = v.replace("/5","").replace("/10","")
    try:
        n = float(v)
        if n > 5:        # 10-scale → normalise
            n = n / 2
        if 1 <= n <= 5:
            return round(n, 1)
        return np.nan
    except:
        return np.nan

df["company_rating"] = df["company_rating"].apply(clean_rating)
df["company_rating"] = df["company_rating"].fillna(df["company_rating"].median())

# ── 16. Clean APPLICATIONS ───────────────────────────────────────────────────
def clean_apps(val):
    if pd.isna(val):
        return np.nan
    v = str(val).strip().lower().replace("+","").replace(",","")
    if v in ["many","several","few"]:
        return np.nan
    try:
        n = float(v)
        if n < 0:
            return np.nan
        return int(n)
    except:
        return np.nan

df["applications"] = df["applications"].apply(clean_apps)
df["applications"] = df["applications"].fillna(df["applications"].median())
df["applications"] = df["applications"].fillna(0).astype(int)

# ── 17. Clean HIRING STATUS ──────────────────────────────────────────────────
def clean_status(val):
    if pd.isna(val):
        return "Unknown"
    v = str(val).strip().lower()
    if v in ["open","active"]:
        return "Open"
    if v in ["closed","filled"]:
        return "Closed"
    if v in ["on hold"]:
        return "On Hold"
    return "Unknown"

df["hiring_status"] = df["hiring_status"].apply(clean_status)

# ── 18. Clean SKILLS → normalise separators ───────────────────────────────────
def clean_skills(val):
    if pd.isna(val):
        return ""
    v = str(val).strip()
    v = v.strip("[]'\"")
    v = re.sub(r"[|/;]", ",", v)
    parts = [s.strip().strip("'\"").strip() for s in v.split(",") if s.strip()]
    # Keep unique, title-case
    seen = set()
    clean = []
    for p in parts:
        pk = p.lower()
        if pk not in seen and p:
            seen.add(pk)
            clean.append(p.title())
    return ", ".join(clean)

df["skills"] = df["skills"].apply(clean_skills)

# ── 19. Drop original messy salary & currency; clean up ──────────────────────
df.drop(columns=["salary", "currency", "job_description"], inplace=True, errors="ignore")

# ── 20. Salary bands ─────────────────────────────────────────────────────────
def salary_band(s):
    if s < 50000:  return "< $50K"
    if s < 80000:  return "$50K–$80K"
    if s < 120000: return "$80K–$120K"
    if s < 180000: return "$120K–$180K"
    return "$180K+"

df["salary_band"] = df["salary_usd"].apply(salary_band)

# ── 21. Final column order ────────────────────────────────────────────────────
cols = [
    "job_id","job_title","company_name","location","industry",
    "experience_level","years_of_exp","education","employment_type",
    "remote_work","salary_usd","salary_band","skills",
    "company_rating","applications","hiring_status",
    "job_posted_date","post_month","post_year","benefits",
]
df = df[[c for c in cols if c in df.columns]]

# Fill remaining NaN strings
for col in df.select_dtypes(include="object").columns:
    df[col] = df[col].fillna("Unknown")

df.reset_index(drop=True, inplace=True)

df.to_csv("/mnt/user-data/outputs/job_market_cleaned_2025.csv", index=False)
print(f"\nCleaned shape: {df.shape}")
print("\nNull counts after cleaning:")
print(df.isnull().sum())
print("\nSample:\n", df.head(3).to_string())
