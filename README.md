# 📊 Job Market Analysis 2025

> An end-to-end data analytics project examining tech hiring trends, salary benchmarks, skills demand, and remote work patterns across 1,397 global job listings.

---

## 🎯 Problem Statement

The tech job market in 2025 is noisy, fragmented, and hard to read — job boards present inconsistent data, salary figures are unreliable, and it is difficult for candidates, recruiters, and business leaders to identify real hiring trends from raw listings.

This project answers four key questions:

1. **Which roles and skills command the highest salaries?**
2. **Where is hiring demand actually concentrated — by role, industry, and location?**
3. **How has remote work reshaped the tech labour market?**
4. **What does the competitive landscape look like for candidates applying in 2025?**

---

## 📁 Dataset

| Property | Detail |
|---|---|
| **Source** | Synthetic-realistic dataset generated to mirror real-world job board data |
| **Raw Records** | 1,500 job listings |
| **Cleaned Records** | 1,397 (after deduplication and empty-row removal) |
| **Features** | 20 columns across role, company, location, salary, skills, dates, status |
| **Date Range** | January 2024 – December 2025 |
| **Geographic Coverage** | 12 markets: US cities, Remote, UK, Germany, Canada, India |
| **Industries** | 11 sectors including Technology, Finance, Healthcare, Gaming, Automotive |

### Raw Data Issues Introduced
The raw dataset (`job_market_messy_2025.csv`) was deliberately constructed with real-world messiness:

- Mixed date formats (`MM/DD/YYYY`, `DD-MM-YYYY`, `DD.MM.YYYY`)
- Salary values as strings (`"$85,000"`, `"120k"`, `"150000 USD"`)
- Inconsistent casing and aliases (`"data scientist"`, `"DATA SCIENTIST"`, `"Data Sci"`)
- ~15–40% null values per column
- 73 exact duplicate rows
- Outlier salaries (negative values, values > $1M)
- Numeric fields stored as strings (`"5 years"`, `"10yrs"`)
- Company rating on mixed scales (1–5 and 1–10)

---

## 🛠️ Tools Used

| Layer | Tool | Purpose |
|---|---|---|
| **Data Generation** | Python · NumPy | Synthetic messy dataset creation |
| **Data Cleaning** | Python · Pandas · Regex | Standardisation, imputation, deduplication |
| **EDA** | Python · Matplotlib | Exploratory analysis + 11-panel visual dashboard |
| **Automation** | Python · Logging · Argparse | End-to-end pipeline with 7 orchestrated stages |
| **SQL Analysis** | PostgreSQL-compatible SQL | Aggregation, window functions, KPI views |
| **Business Modelling** | Excel · OpenPyXL | What-if salary models, hiring funnel, skills matrix |
| **Reporting** | Word (docx-js) | Professional insights report with embedded visuals |
| **Dashboard** | Power BI *(ready)* | Cleaned CSV pre-formatted for direct import |

---

## 🧹 Data Cleaning

All cleaning logic lives in [`data_cleaning_script.py`](data_cleaning_script.py) and is re-used inside the automation pipeline ([`python_2_automation.py`](python_2_automation.py)).

### Cleaning Steps

**1. Load & Initial Sanitisation**
- Replace blank strings with `NaN`
- Drop fully empty rows (30 removed)

**2. Deduplication**
- Remove exact duplicate rows → 73 duplicates dropped

**3. Categorical Standardisation (Regex Mapping)**

| Column | Example Raw Values | Cleaned To |
|---|---|---|
| `job_title` | `"data scientist"`, `"DATA SCI"`, `"DS"` | `Data Scientist` |
| `company_name` | `"google"`, `"GOOGLE"`, `"Googl"` | `Google` |
| `location` | `"SF, CA"`, `"san francisco"` | `San Francisco, CA` |
| `experience_level` | `"sr"`, `"Senior"`, `"SR."` | `Senior` |
| `education` | `"BS"`, `"Bachelors"`, `"B.S."` | `Bachelor's` |
| `employment_type` | `"FT"`, `"full time"`, `"FULL-TIME"` | `Full-Time` |
| `industry` | `"tech"`, `"IT"`, `"information technology"` | `Technology` |
| `remote_work` | `"1"`, `"yes"`, `"TRUE"`, `"WFH"` | `Remote` |

**4. Salary Normalisation**
- Strip currency symbols and text (`$`, `USD`, `INR`)
- Expand `k` suffix (`120k` → `120000`)
- Clamp to valid range: `$0 < salary ≤ $600,000`
- Impute nulls using **group median by job_title**, then global median

**5. Numeric Fields**
- `years_of_exp`: strip text suffixes, clamp to `[0, 45]`, impute by experience level median
- `company_rating`: normalise 10-point scale to 5-point (`÷ 2`), impute with global median
- `applications`: strip `+` and text values, impute with median

**6. Date Parsing**
- Try 5 date format patterns; fallback to `pd.to_datetime` inference
- Impute unparseable dates to `2025-01-01`
- Derive `post_month` and `post_year` columns

**7. Derived Features**
- `salary_band`: `< $50K` / `$50K–$80K` / `$80K–$120K` / `$120K–$180K` / `$180K+`
- `skill_count`: count of comma-separated skills per listing

**8. Final Output**
- 1,397 rows × 20 columns
- Zero nulls remaining (categorical → `"Unknown"`, numeric → group median)

---

## 🗄️ SQL Analysis

Full query file: [`job_market_analysis.sql`](job_market_analysis.sql)

### Structure (9 Sections)

```
Section 1 — Table creation & CSV import
Section 2 — Data overview & null audit
Section 3 — Salary analysis (overall, by role, by exp, by location, by industry)
Section 4 — Job demand analysis (role volumes, employment type, education)
Section 5 — Skills analysis via UNNEST (top skills, high-salary skills)
Section 6 — Time-series & trend analysis (monthly trend, MoM growth, rolling avg)
Section 7 — Advanced analytics (percentile ranking, gap analysis, pivot, outliers)
Section 8 — KPI summary VIEW
Section 9 — Indexes + stored procedure for scheduled refresh
```

### Highlighted Queries

**Salary premium: Senior vs Entry by role**
```sql
WITH salary_by_level AS (
    SELECT job_title,
           MAX(CASE WHEN experience_level = 'Senior'      THEN salary_usd END) AS senior_med,
           MAX(CASE WHEN experience_level = 'Entry Level' THEN salary_usd END) AS entry_med
    FROM job_market
    GROUP BY job_title
)
SELECT job_title,
       ROUND(senior_med - entry_med, 0)                              AS salary_premium,
       ROUND((senior_med - entry_med) / NULLIF(entry_med,0)*100, 1) AS premium_pct
FROM salary_by_level
ORDER BY salary_premium DESC;
```

**Month-over-month growth (window function)**
```sql
WITH monthly AS (SELECT post_month, COUNT(*) AS jobs FROM job_market GROUP BY post_month),
     with_prev AS (SELECT *, LAG(jobs) OVER (ORDER BY post_month) AS prev FROM monthly)
SELECT post_month, jobs,
       ROUND((jobs - prev) * 100.0 / NULLIF(prev, 0), 2) AS mom_growth_pct
FROM with_prev;
```

**Role demand vs supply gap**
```sql
SELECT job_title, open_now, competition_ratio,
       CASE
           WHEN open_now > 30 AND competition_ratio < 200 THEN 'High Demand, Low Competition'
           WHEN open_now > 30 AND competition_ratio >= 200 THEN 'High Demand, High Competition'
           ...
       END AS market_segment
FROM demand ORDER BY open_now DESC;
```

---

## 📈 Dashboard

The Excel workbook ([`job_market_excel_analysis.xlsx`](job_market_excel_analysis.xlsx)) contains 5 sheets:

| Sheet | Content |
|---|---|
| **Dashboard** | KPI cards, salary-by-role bar chart, remote work pie chart, industry overview table |
| **Salary What-If** | Blue assumption cells (base salary, exp multiplier, remote bonus, equity %) → live formula outputs. 5-scenario comparison table + line chart |
| **Hiring Scenarios** | Hiring funnel (8 stages), cost-per-hire model, pipeline & attrition KPIs |
| **Skills Matrix** | Heat-map of 14 skills × 8 roles (colour-coded by demand strength) |
| **Data Sample** | First 200 rows of cleaned dataset |

### Power BI Setup
Load `job_market_cleaned_2025.csv` directly. Recommended visuals:

- **KPI cards**: `salary_usd` (median), `total count`, `open_pct`, `remote_pct`
- **Bar chart**: Median salary by `job_title`
- **Line chart**: Job postings by `post_month`
- **Treemap**: Count by `industry`
- **Donut**: `remote_work` distribution
- **Matrix**: `experience_level` × `industry` salary pivot
- **Slicer**: `hiring_status`, `employment_type`, `education`

---

## 💡 Key Insights

### Salary
- **Median salary: $129,681** — consistent across the market with low compression between bands
- **Cloud Architects** earn the highest median ($192,533), followed by Full Stack Developers ($170,216) and Data Scientists ($152,498)
- The gap between Entry Level and Director median is only **~$12,000** — foundational technical skills command strong pay even at junior levels
- **Education premium is minimal**: PhD holders earn just $3,174 more than Bootcamp graduates (median), validating practical certification pathways
- Remote roles carry **no salary penalty** — Remote median ($131K) is on par with On-Site ($124K)

### Demand & Hiring
- **32.8% of positions are actively open** — genuine unmet demand in the market
- **ML Engineer** is the most-listed role (92 postings, 6.6% share) across all categories
- **Technology** dominates industry distribution (25.6%), but Healthcare (11%) and Gaming (9.2%) are rising tech-talent verticals
- Only **21.2% of roles are full-time** — over 50% are contract, freelance, or internship, signalling a shift toward non-traditional workforce models
- Average **511 applications per posting** — competition is intense, particularly for cloud and AI roles

### Skills
- Top skills by mention: **PyTorch (13.7%), Git (13.0%), Docker (12.9%), Tableau (12.9%), Java (12.7%)**
- Three dominant skill clusters: **(1) ML/AI stack**, **(2) Cloud/DevOps**, **(3) Data & Visualisation**
- Candidates spanning two or more clusters command measurably higher salaries
- Average **4.5 skills listed per job posting** — breadth is expected, depth is rewarded

### Remote Work
- **32.6% fully remote + 16.5% hybrid = 49.1%** of the market offers location flexibility
- Remote prevalence is highest for AI Engineers (~45%) and Data Scientists (~40%)
- On-site roles cluster in Healthcare, Gaming, and infrastructure-heavy positions

### Companies
- **Amazon and Google** alone post 214 listings (15.3% of the market)
- Top 5 companies account for **35%** of all listings
- Average company rating is **3.55/5.0** with minimal variance — salary and role quality are stronger candidate differentiators than brand perception

---

## 📂 Project File Structure

```
job-market-analysis-2025/
│
├── data/
│   ├── job_market_messy_2025.csv          ← Raw messy input (1,500 rows)
│   └── job_market_cleaned_2025.csv        ← Cleaned output (1,397 rows, 20 cols)
│
├── python/
│   ├── data_cleaning_script.py            ← Standalone cleaning pipeline
│   ├── python_1_eda.py                    ← EDA + 11-panel visualisation dashboard
│   └── python_2_automation.py            ← Full automation pipeline (7 stages)
│
├── sql/
│   └── job_market_analysis.sql            ← 9-section SQL analysis (PostgreSQL)
│
├── excel/
│   └── job_market_excel_analysis.xlsx     ← 5-sheet workbook with What-If models
│
├── reports/
│   ├── job_market_insights_report.docx    ← 10-page professional Word report
│   ├── eda_dashboard.png                  ← 11-panel EDA visualisation
│   ├── eda_report.txt                     ← Full EDA text output
│   └── pipeline_summary_report.txt        ← Automation pipeline summary
│
└── README.md
```

---

## 🚀 How to Run

### 1. Data Cleaning
```bash
python data_cleaning_script.py
# Input:  job_market_messy_2025.csv
# Output: job_market_cleaned_2025.csv
```

### 2. Exploratory Data Analysis
```bash
python python_1_eda.py
# Outputs: eda_report.txt, eda_dashboard.png
```

### 3. Full Automation Pipeline
```bash
python python_2_automation.py \
  --input  data/job_market_messy_2025.csv \
  --output outputs/
# Outputs: cleaned CSV, KPI JSON, salary aggregation, skills frequency,
#          anomaly report, pipeline log, summary report
```

### 4. SQL Analysis
```sql
-- In PostgreSQL:
\i job_market_analysis.sql
-- Or run individual sections in any SQL client
```

### 5. Excel Model
```
Open job_market_excel_analysis.xlsx
→ Salary What-If: change blue cells (Base Salary, Multipliers, Bonuses)
→ Hiring Scenarios: adjust headcount, cost-per-hire, funnel rates
→ All outputs update automatically via live formulas
```

---

## 📦 Dependencies

```txt
pandas>=2.0
numpy>=1.24
matplotlib>=3.7
openpyxl>=3.1
```

Install with:
```bash
pip install pandas numpy matplotlib openpyxl
```

---

*Built as a portfolio data analytics project demonstrating end-to-end skills across Python, SQL, Excel, and business reporting.*
