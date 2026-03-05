"""
╔══════════════════════════════════════════════════════════════════╗
║   Job Market Analysis 2025 — Exploratory Data Analysis (EDA)   ║
║   Script 1 of 2 | Tech: Python · Pandas · Matplotlib           ║
╚══════════════════════════════════════════════════════════════════╝

Purpose : Deep-dive EDA on the cleaned job market dataset.
Outputs : job_market_eda_report.txt  (printed summary)
          [figures printed to console / save-ready]
"""

import pandas as pd
import numpy as np
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
from matplotlib.gridspec import GridSpec
import warnings
warnings.filterwarnings("ignore")

# ─────────────────────────────────────────────────────────────────────────────
# 0. CONFIG
# ─────────────────────────────────────────────────────────────────────────────
DATA_PATH   = "/mnt/user-data/outputs/job_market_cleaned_2025.csv"
OUTPUT_DIR  = "/mnt/user-data/outputs/"
REPORT_PATH = OUTPUT_DIR + "eda_report.txt"

PALETTE = {
    "primary"  : "#2563EB",
    "secondary": "#7C3AED",
    "accent"   : "#059669",
    "warning"  : "#D97706",
    "danger"   : "#DC2626",
    "light"    : "#F1F5F9",
    "dark"     : "#1E293B",
    "muted"    : "#94A3B8",
}

COLOR_SEQ = ["#2563EB","#7C3AED","#059669","#D97706","#DC2626",
             "#0891B2","#DB2777","#65A30D","#EA580C","#7E22CE"]

plt.rcParams.update({
    "font.family"      : "DejaVu Sans",
    "font.size"        : 10,
    "axes.spines.top"  : False,
    "axes.spines.right": False,
    "axes.grid"        : True,
    "grid.alpha"       : 0.3,
    "figure.dpi"       : 120,
})

lines = []
def log(msg=""):
    print(msg)
    lines.append(str(msg))

# ─────────────────────────────────────────────────────────────────────────────
# 1. LOAD DATA
# ─────────────────────────────────────────────────────────────────────────────
df = pd.read_csv(DATA_PATH, parse_dates=["job_posted_date"])

log("=" * 66)
log("  JOB MARKET ANALYSIS 2025 — EDA REPORT")
log("=" * 66)

log(f"\n{'DATASET OVERVIEW':─<60}")
log(f"  Total Records   : {len(df):,}")
log(f"  Total Features  : {df.shape[1]}")
log(f"  Date Range      : {df['job_posted_date'].min().date()} → {df['job_posted_date'].max().date()}")
log(f"  Memory Usage    : {df.memory_usage(deep=True).sum() / 1024:.1f} KB")

log(f"\n  DATA TYPES:")
for col, dtype in df.dtypes.items():
    log(f"    {col:<22} {str(dtype)}")

# ─────────────────────────────────────────────────────────────────────────────
# 2. SALARY ANALYSIS
# ─────────────────────────────────────────────────────────────────────────────
log(f"\n{'SALARY ANALYSIS':─<60}")
s = df["salary_usd"]
log(f"  Min     : ${s.min():>10,.0f}")
log(f"  Max     : ${s.max():>10,.0f}")
log(f"  Mean    : ${s.mean():>10,.0f}")
log(f"  Median  : ${s.median():>10,.0f}")
log(f"  Std Dev : ${s.std():>10,.0f}")
log(f"  Q1 (25%): ${s.quantile(0.25):>10,.0f}")
log(f"  Q3 (75%): ${s.quantile(0.75):>10,.0f}")
log(f"  IQR     : ${s.quantile(0.75)-s.quantile(0.25):>10,.0f}")

log(f"\n  SALARY BY JOB TITLE (Top 10):")
sal_title = df.groupby("job_title")["salary_usd"].agg(["median","mean","count"]).sort_values("median", ascending=False).head(10)
for idx, row in sal_title.iterrows():
    log(f"    {idx:<30} Median=${row['median']:>8,.0f}  Mean=${row['mean']:>8,.0f}  n={int(row['count'])}")

log(f"\n  SALARY BY EXPERIENCE LEVEL:")
sal_exp = df.groupby("experience_level")["salary_usd"].agg(["median","mean","count"]).sort_values("median", ascending=False)
for idx, row in sal_exp.iterrows():
    log(f"    {idx:<20} Median=${row['median']:>8,.0f}  Mean=${row['mean']:>8,.0f}  n={int(row['count'])}")

log(f"\n  SALARY BY LOCATION (Top 8):")
sal_loc = df.groupby("location")["salary_usd"].median().sort_values(ascending=False).head(8)
for loc, val in sal_loc.items():
    log(f"    {loc:<30} ${val:>8,.0f}")

log(f"\n  SALARY BANDS:")
band_counts = df["salary_band"].value_counts()
for band, cnt in band_counts.items():
    pct = cnt / len(df) * 100
    log(f"    {band:<15} {cnt:>5,}  ({pct:.1f}%)")

# ─────────────────────────────────────────────────────────────────────────────
# 3. JOB DISTRIBUTION ANALYSIS
# ─────────────────────────────────────────────────────────────────────────────
log(f"\n{'JOB DISTRIBUTION':─<60}")

log(f"\n  TOP JOB TITLES:")
for title, cnt in df["job_title"].value_counts().head(10).items():
    pct = cnt / len(df) * 100
    bar = "█" * int(pct / 1.5)
    log(f"    {title:<30} {cnt:>4} ({pct:5.1f}%)  {bar}")

log(f"\n  BY EXPERIENCE LEVEL:")
for lvl, cnt in df["experience_level"].value_counts().items():
    pct = cnt / len(df) * 100
    log(f"    {lvl:<20} {cnt:>4} ({pct:5.1f}%)")

log(f"\n  BY EMPLOYMENT TYPE:")
for etype, cnt in df["employment_type"].value_counts().items():
    pct = cnt / len(df) * 100
    log(f"    {etype:<20} {cnt:>4} ({pct:5.1f}%)")

log(f"\n  BY INDUSTRY:")
for ind, cnt in df["industry"].value_counts().items():
    pct = cnt / len(df) * 100
    log(f"    {ind:<30} {cnt:>4} ({pct:5.1f}%)")

log(f"\n  REMOTE WORK:")
for rw, cnt in df["remote_work"].value_counts().items():
    pct = cnt / len(df) * 100
    log(f"    {rw:<15} {cnt:>4} ({pct:5.1f}%)")

log(f"\n  TOP HIRING COMPANIES:")
for comp, cnt in df["company_name"].value_counts().head(10).items():
    log(f"    {comp:<25} {cnt:>4}")

# ─────────────────────────────────────────────────────────────────────────────
# 4. SKILLS ANALYSIS
# ─────────────────────────────────────────────────────────────────────────────
log(f"\n{'SKILLS ANALYSIS':─<60}")

all_skills = []
for row in df["skills"]:
    if row and row != "Unknown":
        all_skills.extend([s.strip() for s in str(row).split(",") if s.strip()])

from collections import Counter
skill_counts = Counter(all_skills)
top_skills = skill_counts.most_common(20)

log(f"\n  TOTAL SKILL MENTIONS  : {len(all_skills):,}")
log(f"  UNIQUE SKILLS         : {len(skill_counts):,}")
log(f"\n  TOP 20 IN-DEMAND SKILLS:")
for i, (skill, cnt) in enumerate(top_skills, 1):
    pct = cnt / len(df) * 100
    bar = "█" * int(pct / 1.5)
    log(f"    {i:>2}. {skill:<30} {cnt:>4} ({pct:5.1f}%)  {bar}")

log(f"\n  AVG SKILLS PER JOB POSTING:")
df["skill_count"] = df["skills"].apply(lambda x: len([s for s in str(x).split(",") if s.strip()]) if x != "Unknown" else 0)
log(f"    Mean   : {df['skill_count'].mean():.2f}")
log(f"    Median : {df['skill_count'].median():.1f}")
log(f"    Max    : {df['skill_count'].max()}")

# ─────────────────────────────────────────────────────────────────────────────
# 5. CORRELATION ANALYSIS
# ─────────────────────────────────────────────────────────────────────────────
log(f"\n{'CORRELATION ANALYSIS':─<60}")
corr = df[["salary_usd","years_of_exp","company_rating","applications","skill_count"]].corr()
log(f"\n  PEARSON CORRELATION MATRIX:")
log(f"  {corr.to_string()}")

key_corr = [
    ("salary_usd","years_of_exp"),
    ("salary_usd","company_rating"),
    ("salary_usd","applications"),
    ("years_of_exp","applications"),
]
log(f"\n  KEY CORRELATIONS:")
for a, b in key_corr:
    r = df[a].corr(df[b])
    strength = "Strong" if abs(r) > 0.5 else "Moderate" if abs(r) > 0.3 else "Weak"
    direction = "Positive" if r > 0 else "Negative"
    log(f"    {a:<20} ↔ {b:<20}  r={r:+.4f}  ({strength} {direction})")

# ─────────────────────────────────────────────────────────────────────────────
# 6. TIME SERIES ANALYSIS
# ─────────────────────────────────────────────────────────────────────────────
log(f"\n{'TIME-SERIES ANALYSIS':─<60}")
monthly = df.groupby("post_month").agg(
    jobs_posted=("job_title","count"),
    avg_salary=("salary_usd","mean"),
    avg_rating=("company_rating","mean")
).reset_index().sort_values("post_month")

log(f"\n  MONTHLY JOB POSTINGS:")
for _, row in monthly.iterrows():
    bar = "█" * int(row["jobs_posted"] / 8)
    log(f"    {row['post_month']}  {row['jobs_posted']:>4} jobs  ${row['avg_salary']:>8,.0f} avg  {bar}")

# ─────────────────────────────────────────────────────────────────────────────
# 7. HIRING STATUS KPIs
# ─────────────────────────────────────────────────────────────────────────────
log(f"\n{'HIRING STATUS KPIs':─<60}")
status = df["hiring_status"].value_counts()
for st, cnt in status.items():
    pct = cnt / len(df) * 100
    log(f"    {st:<15} {cnt:>4} ({pct:5.1f}%)")

open_jobs = df[df["hiring_status"] == "Open"]
log(f"\n  OPEN JOBS INSIGHTS:")
log(f"    Count                 : {len(open_jobs):,}")
log(f"    Avg Salary (Open)     : ${open_jobs['salary_usd'].mean():,.0f}")
log(f"    Avg Applications      : {open_jobs['applications'].mean():.0f}")
log(f"    Top Role (Open)       : {open_jobs['job_title'].mode()[0]}")
log(f"    Top Location (Open)   : {open_jobs['location'].mode()[0]}")

# ─────────────────────────────────────────────────────────────────────────────
# 8. EDUCATION vs SALARY
# ─────────────────────────────────────────────────────────────────────────────
log(f"\n{'EDUCATION vs SALARY':─<60}")
edu_sal = df.groupby("education")["salary_usd"].agg(["median","count"]).sort_values("median", ascending=False)
for edu, row in edu_sal.iterrows():
    log(f"    {edu:<20} Median=${row['median']:>8,.0f}  n={int(row['count'])}")

# ─────────────────────────────────────────────────────────────────────────────
# 9. SAVE REPORT
# ─────────────────────────────────────────────────────────────────────────────
with open(REPORT_PATH, "w") as f:
    f.write("\n".join(lines))
log(f"\n  ✔ EDA report saved → {REPORT_PATH}")

# ─────────────────────────────────────────────────────────────────────────────
# 10. VISUALISATIONS  (single multi-panel figure)
# ─────────────────────────────────────────────────────────────────────────────
fig = plt.figure(figsize=(22, 28), facecolor=PALETTE["light"])
fig.suptitle("Job Market Analysis 2025 — EDA Dashboard",
             fontsize=18, fontweight="bold", color=PALETTE["dark"], y=0.98)

gs = GridSpec(4, 3, figure=fig, hspace=0.50, wspace=0.35)

# ── Panel 1: Salary Distribution ─────────────────────────────────────────────
ax1 = fig.add_subplot(gs[0, 0])
ax1.hist(df["salary_usd"] / 1000, bins=40, color=PALETTE["primary"], edgecolor="white", linewidth=0.5)
ax1.axvline(df["salary_usd"].median() / 1000, color=PALETTE["danger"], linestyle="--", linewidth=1.5, label=f"Median ${df['salary_usd'].median()/1000:.0f}K")
ax1.axvline(df["salary_usd"].mean()   / 1000, color=PALETTE["warning"], linestyle="-.", linewidth=1.5, label=f"Mean ${df['salary_usd'].mean()/1000:.0f}K")
ax1.set_title("Salary Distribution", fontweight="bold", color=PALETTE["dark"])
ax1.set_xlabel("Salary (USD $K)"); ax1.set_ylabel("Count")
ax1.legend(fontsize=8)

# ── Panel 2: Salary by Experience Level ──────────────────────────────────────
ax2 = fig.add_subplot(gs[0, 1])
exp_order = ["Entry Level","Mid Level","Senior","Lead/Principal","Manager","Director","Executive","Unknown"]
exp_data   = [df[df["experience_level"] == e]["salary_usd"].median() / 1000 for e in exp_order]
bars = ax2.barh(exp_order, exp_data, color=COLOR_SEQ[:len(exp_order)])
for bar, val in zip(bars, exp_data):
    ax2.text(bar.get_width() + 1, bar.get_y() + bar.get_height()/2,
             f"${val:.0f}K", va="center", fontsize=8)
ax2.set_title("Median Salary by Experience", fontweight="bold", color=PALETTE["dark"])
ax2.set_xlabel("Median Salary ($K)")

# ── Panel 3: Top 10 Job Titles ────────────────────────────────────────────────
ax3 = fig.add_subplot(gs[0, 2])
top_titles = df["job_title"].value_counts().head(10)
bars = ax3.barh(top_titles.index[::-1], top_titles.values[::-1], color=PALETTE["secondary"])
for bar, val in zip(bars, top_titles.values[::-1]):
    ax3.text(bar.get_width() + 2, bar.get_y() + bar.get_height()/2,
             str(val), va="center", fontsize=8)
ax3.set_title("Top 10 Job Titles", fontweight="bold", color=PALETTE["dark"])
ax3.set_xlabel("Count")

# ── Panel 4: Remote Work Distribution ────────────────────────────────────────
ax4 = fig.add_subplot(gs[1, 0])
remote_counts = df["remote_work"].value_counts()
wedge_colors = [PALETTE["accent"], PALETTE["primary"], PALETTE["warning"], PALETTE["muted"]]
wedges, texts, autotexts = ax4.pie(
    remote_counts.values, labels=remote_counts.index,
    autopct="%1.1f%%", colors=wedge_colors[:len(remote_counts)],
    startangle=90, wedgeprops={"edgecolor":"white","linewidth":1.5}
)
for at in autotexts: at.set_fontsize(8)
ax4.set_title("Remote Work Distribution", fontweight="bold", color=PALETTE["dark"])

# ── Panel 5: Industry Distribution ───────────────────────────────────────────
ax5 = fig.add_subplot(gs[1, 1])
ind_counts = df["industry"].value_counts().head(8)
bars = ax5.barh(ind_counts.index[::-1], ind_counts.values[::-1], color=PALETTE["accent"])
for bar, val in zip(bars, ind_counts.values[::-1]):
    ax5.text(bar.get_width() + 1, bar.get_y() + bar.get_height()/2,
             str(val), va="center", fontsize=8)
ax5.set_title("Jobs by Industry", fontweight="bold", color=PALETTE["dark"])
ax5.set_xlabel("Count")

# ── Panel 6: Top 10 Skills Heatbar ───────────────────────────────────────────
ax6 = fig.add_subplot(gs[1, 2])
top10_skills = [s for s, _ in top_skills[:10]]
top10_counts = [c for _, c in top_skills[:10]]
bars = ax6.barh(top10_skills[::-1], top10_counts[::-1],
                color=[COLOR_SEQ[i % len(COLOR_SEQ)] for i in range(10)])
for bar, val in zip(bars, top10_counts[::-1]):
    ax6.text(bar.get_width() + 1, bar.get_y() + bar.get_height()/2,
             str(val), va="center", fontsize=8)
ax6.set_title("Top 10 In-Demand Skills", fontweight="bold", color=PALETTE["dark"])
ax6.set_xlabel("Mentions")

# ── Panel 7: Monthly Job Postings Trend ──────────────────────────────────────
ax7 = fig.add_subplot(gs[2, :2])
monthly_sorted = monthly.sort_values("post_month")
ax7.fill_between(range(len(monthly_sorted)), monthly_sorted["jobs_posted"],
                 alpha=0.25, color=PALETTE["primary"])
ax7.plot(range(len(monthly_sorted)), monthly_sorted["jobs_posted"],
         color=PALETTE["primary"], linewidth=2.5, marker="o", markersize=5)
ax7.set_xticks(range(len(monthly_sorted)))
ax7.set_xticklabels(monthly_sorted["post_month"], rotation=45, ha="right", fontsize=8)
ax7.set_title("Monthly Job Postings Trend", fontweight="bold", color=PALETTE["dark"])
ax7.set_ylabel("Jobs Posted")

# ── Panel 8: Salary by Industry (boxplot proxy = bar + std) ──────────────────
ax8 = fig.add_subplot(gs[2, 2])
ind_sal = df.groupby("industry")["salary_usd"].median().sort_values(ascending=False).head(7) / 1000
colors_8 = COLOR_SEQ[:len(ind_sal)]
bars = ax8.bar(range(len(ind_sal)), ind_sal.values, color=colors_8)
ax8.set_xticks(range(len(ind_sal)))
ax8.set_xticklabels([x.replace("/", "\n") for x in ind_sal.index], rotation=45, ha="right", fontsize=7)
ax8.set_title("Median Salary by Industry ($K)", fontweight="bold", color=PALETTE["dark"])
ax8.set_ylabel("Median Salary ($K)")

# ── Panel 9: Education Breakdown ─────────────────────────────────────────────
ax9 = fig.add_subplot(gs[3, 0])
edu_counts = df["education"].value_counts()
ax9.pie(edu_counts.values, labels=edu_counts.index,
        autopct="%1.1f%%", colors=COLOR_SEQ[:len(edu_counts)],
        startangle=90, wedgeprops={"edgecolor":"white","linewidth":1.5})
ax9.set_title("Education Level Distribution", fontweight="bold", color=PALETTE["dark"])

# ── Panel 10: Salary by Education ────────────────────────────────────────────
ax10 = fig.add_subplot(gs[3, 1])
edu_sal2 = df.groupby("education")["salary_usd"].median().sort_values(ascending=False) / 1000
bars = ax10.bar(range(len(edu_sal2)), edu_sal2.values,
                color=COLOR_SEQ[:len(edu_sal2)])
ax10.set_xticks(range(len(edu_sal2)))
ax10.set_xticklabels(edu_sal2.index, rotation=45, ha="right", fontsize=8)
ax10.set_title("Median Salary by Education ($K)", fontweight="bold", color=PALETTE["dark"])
ax10.set_ylabel("$K")
for i, val in enumerate(edu_sal2.values):
    ax10.text(i, val + 1, f"${val:.0f}K", ha="center", fontsize=7)

# ── Panel 11: Hiring Status ───────────────────────────────────────────────────
ax11 = fig.add_subplot(gs[3, 2])
status_counts = df["hiring_status"].value_counts()
status_colors = [PALETTE["accent"] if s == "Open" else PALETTE["danger"] if s == "Closed"
                 else PALETTE["warning"] if s == "On Hold" else PALETTE["muted"]
                 for s in status_counts.index]
wedges2, texts2, autotexts2 = ax11.pie(
    status_counts.values, labels=status_counts.index,
    autopct="%1.1f%%", colors=status_colors,
    startangle=90, wedgeprops={"edgecolor":"white","linewidth":1.5}
)
for at in autotexts2: at.set_fontsize(8)
ax11.set_title("Hiring Status", fontweight="bold", color=PALETTE["dark"])

plt.savefig(OUTPUT_DIR + "eda_dashboard.png", dpi=130, bbox_inches="tight",
            facecolor=PALETTE["light"])
print(f"\n  ✔ EDA Dashboard saved → {OUTPUT_DIR}eda_dashboard.png")

# ─────────────────────────────────────────────────────────────────────────────
# 11. FINAL KPI SUMMARY
# ─────────────────────────────────────────────────────────────────────────────
log("\n" + "=" * 66)
log("  KEY PERFORMANCE INDICATORS — EXECUTIVE SUMMARY")
log("=" * 66)
log(f"  Total Job Listings     : {len(df):,}")
log(f"  Avg Salary (USD)       : ${df['salary_usd'].mean():,.0f}")
log(f"  Median Salary (USD)    : ${df['salary_usd'].median():,.0f}")
log(f"  Highest Paying Role    : {df.groupby('job_title')['salary_usd'].median().idxmax()}")
log(f"  Most In-Demand Role    : {df['job_title'].mode()[0]}")
log(f"  Top Hiring Company     : {df['company_name'].value_counts().index[0]}")
log(f"  Most Common Skill      : {top_skills[0][0]}")
log(f"  % Remote Jobs          : {(df['remote_work']=='Remote').sum()/len(df)*100:.1f}%")
log(f"  % Open Positions       : {(df['hiring_status']=='Open').sum()/len(df)*100:.1f}%")
log(f"  Avg Company Rating     : {df['company_rating'].mean():.2f} / 5.0")
log(f"  Avg Applications/Job   : {df['applications'].mean():.0f}")
log("=" * 66)

print("\n✅ EDA Complete! All outputs saved.")
