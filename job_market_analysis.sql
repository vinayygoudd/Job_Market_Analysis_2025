-- ============================================================
--  JOB MARKET ANALYSIS 2025 — SQL SCRIPT
--  Database: PostgreSQL / MySQL / SQLite compatible
--  Source  : job_market_cleaned_2025.csv
--  Purpose : Data aggregation, KPI queries, trend analysis
-- ============================================================


-- ============================================================
-- SECTION 0 : TABLE CREATION & DATA LOAD
-- ============================================================

CREATE TABLE IF NOT EXISTS job_market (
    job_id            VARCHAR(20),
    job_title         VARCHAR(100)    NOT NULL,
    company_name      VARCHAR(100),
    location          VARCHAR(100),
    industry          VARCHAR(100),
    experience_level  VARCHAR(50),
    years_of_exp      INTEGER,
    education         VARCHAR(50),
    employment_type   VARCHAR(50),
    remote_work       VARCHAR(20),
    salary_usd        INTEGER,
    salary_band       VARCHAR(20),
    skills            TEXT,
    company_rating    DECIMAL(3,1),
    applications      INTEGER,
    hiring_status     VARCHAR(20),
    job_posted_date   DATE,
    post_month        VARCHAR(10),
    post_year         INTEGER,
    benefits          VARCHAR(200)
);

-- To import from CSV (PostgreSQL):
-- COPY job_market FROM '/path/to/job_market_cleaned_2025.csv'
-- DELIMITER ',' CSV HEADER;

-- ============================================================
-- SECTION 1 : DATA OVERVIEW & HEALTH CHECK
-- ============================================================

-- 1.1 Row count and basic stats
SELECT
    COUNT(*)                                        AS total_records,
    COUNT(DISTINCT job_title)                       AS unique_job_titles,
    COUNT(DISTINCT company_name)                    AS unique_companies,
    COUNT(DISTINCT location)                        AS unique_locations,
    COUNT(DISTINCT industry)                        AS unique_industries,
    MIN(job_posted_date)                            AS earliest_posting,
    MAX(job_posted_date)                            AS latest_posting
FROM job_market;

-- 1.2 Null / missing value counts per column
SELECT
    SUM(CASE WHEN job_id           IS NULL OR job_id = 'Unknown' THEN 1 ELSE 0 END)  AS null_job_id,
    SUM(CASE WHEN job_title        IS NULL OR job_title = 'Unknown' THEN 1 ELSE 0 END) AS null_job_title,
    SUM(CASE WHEN company_name     IS NULL OR company_name = 'Unknown' THEN 1 ELSE 0 END) AS null_company,
    SUM(CASE WHEN location         IS NULL OR location = 'Unknown' THEN 1 ELSE 0 END) AS null_location,
    SUM(CASE WHEN salary_usd       IS NULL THEN 1 ELSE 0 END)                        AS null_salary,
    SUM(CASE WHEN experience_level IS NULL OR experience_level = 'Unknown' THEN 1 ELSE 0 END) AS null_exp_level,
    SUM(CASE WHEN skills           IS NULL OR skills = '' THEN 1 ELSE 0 END)         AS null_skills,
    COUNT(*)                                                                          AS total_rows
FROM job_market;

-- 1.3 Distribution of hiring status
SELECT
    hiring_status,
    COUNT(*)                                                    AS count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2)         AS pct
FROM job_market
GROUP BY hiring_status
ORDER BY count DESC;


-- ============================================================
-- SECTION 2 : SALARY ANALYSIS
-- ============================================================

-- 2.1 Overall salary statistics
SELECT
    ROUND(AVG(salary_usd), 0)            AS avg_salary,
    PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY salary_usd) AS median_salary,
    MIN(salary_usd)                      AS min_salary,
    MAX(salary_usd)                      AS max_salary,
    ROUND(STDDEV(salary_usd), 0)         AS std_dev,
    PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY salary_usd) AS p25_salary,
    PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY salary_usd) AS p75_salary,
    PERCENTILE_CONT(0.90) WITHIN GROUP (ORDER BY salary_usd) AS p90_salary
FROM job_market
WHERE salary_usd IS NOT NULL;

-- 2.2 Salary by job title (top 15, ranked)
SELECT
    job_title,
    COUNT(*)                                                    AS job_count,
    ROUND(AVG(salary_usd), 0)                                   AS avg_salary,
    PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY salary_usd)   AS median_salary,
    MIN(salary_usd)                                             AS min_salary,
    MAX(salary_usd)                                             AS max_salary,
    RANK() OVER (ORDER BY AVG(salary_usd) DESC)                 AS salary_rank
FROM job_market
WHERE job_title <> 'Unknown'
GROUP BY job_title
ORDER BY median_salary DESC
LIMIT 15;

-- 2.3 Salary by experience level
SELECT
    experience_level,
    COUNT(*)                                                    AS count,
    ROUND(AVG(salary_usd), 0)                                   AS avg_salary,
    PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY salary_usd)   AS median_salary,
    ROUND(MIN(salary_usd), 0)                                   AS min_salary,
    ROUND(MAX(salary_usd), 0)                                   AS max_salary
FROM job_market
WHERE experience_level <> 'Unknown'
GROUP BY experience_level
ORDER BY median_salary DESC;

-- 2.4 Salary by location (top 10)
SELECT
    location,
    COUNT(*)                                                    AS jobs,
    ROUND(AVG(salary_usd), 0)                                   AS avg_salary,
    PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY salary_usd)   AS median_salary
FROM job_market
WHERE location <> 'Unknown'
GROUP BY location
ORDER BY median_salary DESC
LIMIT 10;

-- 2.5 Salary bands distribution
SELECT
    salary_band,
    COUNT(*)                                                    AS count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2)         AS pct,
    ROUND(AVG(salary_usd), 0)                                   AS avg_salary
FROM job_market
GROUP BY salary_band
ORDER BY avg_salary;

-- 2.6 Salary by industry
SELECT
    industry,
    COUNT(*)                                                    AS job_count,
    ROUND(AVG(salary_usd), 0)                                   AS avg_salary,
    PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY salary_usd)   AS median_salary,
    ROUND(MAX(salary_usd), 0)                                   AS max_salary
FROM job_market
WHERE industry <> 'Unknown'
GROUP BY industry
ORDER BY median_salary DESC;

-- 2.7 Salary by remote work type
SELECT
    remote_work,
    COUNT(*)                                                    AS count,
    ROUND(AVG(salary_usd), 0)                                   AS avg_salary,
    PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY salary_usd)   AS median_salary
FROM job_market
GROUP BY remote_work
ORDER BY median_salary DESC;

-- 2.8 Salary premium: Senior vs Entry level by role
WITH salary_by_level AS (
    SELECT
        job_title,
        MAX(CASE WHEN experience_level = 'Senior'     THEN salary_usd END) AS senior_med,
        MAX(CASE WHEN experience_level = 'Entry Level' THEN salary_usd END) AS entry_med
    FROM job_market
    WHERE job_title <> 'Unknown'
    GROUP BY job_title
)
SELECT
    job_title,
    ROUND(senior_med, 0)                                        AS senior_median_salary,
    ROUND(entry_med, 0)                                         AS entry_median_salary,
    ROUND(senior_med - entry_med, 0)                            AS salary_premium,
    ROUND((senior_med - entry_med) / NULLIF(entry_med, 0) * 100, 1) AS premium_pct
FROM salary_by_level
WHERE senior_med IS NOT NULL AND entry_med IS NOT NULL
ORDER BY salary_premium DESC;


-- ============================================================
-- SECTION 3 : JOB DEMAND ANALYSIS
-- ============================================================

-- 3.1 Most in-demand job titles
SELECT
    job_title,
    COUNT(*)                                                    AS postings,
    ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM job_market), 2) AS market_share_pct,
    ROUND(AVG(salary_usd), 0)                                   AS avg_salary,
    ROUND(AVG(applications), 0)                                 AS avg_applications,
    RANK() OVER (ORDER BY COUNT(*) DESC)                        AS demand_rank
FROM job_market
WHERE job_title <> 'Unknown'
GROUP BY job_title
ORDER BY postings DESC;

-- 3.2 Jobs by employment type
SELECT
    employment_type,
    COUNT(*)                                                    AS count,
    ROUND(AVG(salary_usd), 0)                                   AS avg_salary,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2)         AS pct
FROM job_market
WHERE employment_type <> 'Unknown'
GROUP BY employment_type
ORDER BY count DESC;

-- 3.3 Education requirements distribution
SELECT
    education,
    COUNT(*)                                                    AS count,
    ROUND(AVG(salary_usd), 0)                                   AS avg_salary,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2)         AS pct
FROM job_market
WHERE education <> 'Unknown'
GROUP BY education
ORDER BY avg_salary DESC;

-- 3.4 Competition index: avg applications per job by title
SELECT
    job_title,
    COUNT(*)                                                    AS postings,
    ROUND(AVG(applications), 0)                                 AS avg_applications,
    ROUND(AVG(salary_usd), 0)                                   AS avg_salary,
    ROUND(AVG(company_rating), 2)                               AS avg_company_rating
FROM job_market
WHERE job_title <> 'Unknown'
GROUP BY job_title
HAVING COUNT(*) >= 10
ORDER BY avg_applications DESC
LIMIT 15;

-- 3.5 Top hiring companies
SELECT
    company_name,
    COUNT(*)                                                    AS total_postings,
    COUNT(CASE WHEN hiring_status = 'Open' THEN 1 END)          AS open_postings,
    ROUND(AVG(salary_usd), 0)                                   AS avg_salary,
    ROUND(AVG(company_rating), 2)                               AS avg_rating
FROM job_market
WHERE company_name <> 'Unknown'
GROUP BY company_name
ORDER BY total_postings DESC
LIMIT 15;


-- ============================================================
-- SECTION 4 : SKILLS ANALYSIS
-- ============================================================
-- Note: skills column = comma-separated, parsed via CTE / UNNEST

-- 4.1 Top skills for open positions
-- PostgreSQL UNNEST approach (for other DBs use application-side processing)
WITH skill_split AS (
    SELECT
        TRIM(UNNEST(STRING_TO_ARRAY(skills, ','))) AS skill,
        salary_usd,
        experience_level,
        industry
    FROM job_market
    WHERE skills <> '' AND skills <> 'Unknown'
      AND hiring_status = 'Open'
)
SELECT
    skill,
    COUNT(*)                                                    AS mention_count,
    ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM job_market), 2) AS pct_of_jobs,
    ROUND(AVG(salary_usd), 0)                                   AS avg_salary_when_required,
    RANK() OVER (ORDER BY COUNT(*) DESC)                        AS skill_rank
FROM skill_split
WHERE skill <> ''
GROUP BY skill
ORDER BY mention_count DESC
LIMIT 20;

-- 4.2 High-salary skills (skills associated with salaries > $150K)
WITH skill_split AS (
    SELECT
        TRIM(UNNEST(STRING_TO_ARRAY(skills, ','))) AS skill,
        salary_usd
    FROM job_market
    WHERE skills <> '' AND skills <> 'Unknown'
)
SELECT
    skill,
    COUNT(*)                                                    AS total_mentions,
    SUM(CASE WHEN salary_usd > 150000 THEN 1 ELSE 0 END)       AS high_salary_jobs,
    ROUND(AVG(salary_usd), 0)                                   AS avg_salary,
    ROUND(SUM(CASE WHEN salary_usd > 150000 THEN 1 ELSE 0 END)
          * 100.0 / COUNT(*), 1)                                AS pct_high_salary
FROM skill_split
WHERE skill <> ''
GROUP BY skill
HAVING COUNT(*) >= 15
ORDER BY avg_salary DESC
LIMIT 15;


-- ============================================================
-- SECTION 5 : TIME SERIES & TREND ANALYSIS
-- ============================================================

-- 5.1 Monthly job postings trend
SELECT
    post_month,
    COUNT(*)                                                    AS jobs_posted,
    ROUND(AVG(salary_usd), 0)                                   AS avg_salary,
    SUM(applications)                                           AS total_applications,
    COUNT(CASE WHEN hiring_status = 'Open' THEN 1 END)          AS open_jobs,
    COUNT(CASE WHEN remote_work = 'Remote' THEN 1 END)          AS remote_jobs
FROM job_market
GROUP BY post_month
ORDER BY post_month;

-- 5.2 Month-over-month growth rate
WITH monthly_counts AS (
    SELECT
        post_month,
        COUNT(*) AS jobs_count
    FROM job_market
    GROUP BY post_month
),
with_prev AS (
    SELECT
        post_month,
        jobs_count,
        LAG(jobs_count) OVER (ORDER BY post_month) AS prev_month_count
    FROM monthly_counts
)
SELECT
    post_month,
    jobs_count,
    prev_month_count,
    ROUND((jobs_count - prev_month_count) * 100.0
          / NULLIF(prev_month_count, 0), 2)                     AS mom_growth_pct
FROM with_prev
ORDER BY post_month;

-- 5.3 Rolling 3-month avg salary
WITH monthly_sal AS (
    SELECT
        post_month,
        ROUND(AVG(salary_usd), 0) AS avg_salary
    FROM job_market
    GROUP BY post_month
)
SELECT
    post_month,
    avg_salary,
    ROUND(AVG(avg_salary) OVER (
        ORDER BY post_month
        ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
    ), 0)                                                       AS rolling_3m_avg_salary
FROM monthly_sal
ORDER BY post_month;


-- ============================================================
-- SECTION 6 : ADVANCED ANALYTICS (CTEs + WINDOW FUNCTIONS)
-- ============================================================

-- 6.1 Percentile ranking of companies by avg salary
WITH company_stats AS (
    SELECT
        company_name,
        COUNT(*)                    AS postings,
        ROUND(AVG(salary_usd), 0)   AS avg_salary,
        ROUND(AVG(company_rating), 2) AS avg_rating
    FROM job_market
    WHERE company_name <> 'Unknown'
    GROUP BY company_name
    HAVING COUNT(*) >= 5
)
SELECT
    company_name,
    postings,
    avg_salary,
    avg_rating,
    PERCENT_RANK() OVER (ORDER BY avg_salary)   AS salary_percentile,
    NTILE(4)       OVER (ORDER BY avg_salary)   AS salary_quartile
FROM company_stats
ORDER BY avg_salary DESC;

-- 6.2 Role demand vs supply gap analysis
WITH demand AS (
    SELECT
        job_title,
        COUNT(*)                        AS total_listings,
        SUM(applications)               AS total_applications,
        COUNT(CASE WHEN hiring_status = 'Open' THEN 1 END) AS open_now
    FROM job_market
    WHERE job_title <> 'Unknown'
    GROUP BY job_title
)
SELECT
    job_title,
    total_listings,
    total_applications,
    open_now,
    ROUND(total_applications * 1.0 / NULLIF(total_listings, 0), 1)  AS competition_ratio,
    CASE
        WHEN open_now > 30 AND total_applications / NULLIF(total_listings,0) < 200 THEN 'High Demand, Low Competition'
        WHEN open_now > 30 AND total_applications / NULLIF(total_listings,0) >= 200 THEN 'High Demand, High Competition'
        WHEN open_now <= 30 AND total_applications / NULLIF(total_listings,0) < 200 THEN 'Low Demand, Low Competition'
        ELSE 'Low Demand, High Competition'
    END                                                              AS market_segment
FROM demand
ORDER BY open_now DESC;

-- 6.3 Top roles by industry — rank within industry
WITH role_industry AS (
    SELECT
        industry,
        job_title,
        COUNT(*)                        AS postings,
        ROUND(AVG(salary_usd), 0)       AS avg_salary,
        RANK() OVER (
            PARTITION BY industry
            ORDER BY COUNT(*) DESC
        )                               AS rank_in_industry
    FROM job_market
    WHERE industry <> 'Unknown' AND job_title <> 'Unknown'
    GROUP BY industry, job_title
)
SELECT *
FROM role_industry
WHERE rank_in_industry <= 3
ORDER BY industry, rank_in_industry;

-- 6.4 Salary quartiles by experience × industry pivot
SELECT
    experience_level,
    ROUND(AVG(CASE WHEN industry = 'Technology'          THEN salary_usd END), 0) AS tech_avg_sal,
    ROUND(AVG(CASE WHEN industry = 'Finance'             THEN salary_usd END), 0) AS finance_avg_sal,
    ROUND(AVG(CASE WHEN industry = 'Healthcare'          THEN salary_usd END), 0) AS healthcare_avg_sal,
    ROUND(AVG(CASE WHEN industry = 'E-Commerce/Retail'   THEN salary_usd END), 0) AS ecomm_avg_sal,
    ROUND(AVG(CASE WHEN industry = 'Consulting'          THEN salary_usd END), 0) AS consulting_avg_sal,
    ROUND(AVG(salary_usd), 0)                                                      AS overall_avg_sal
FROM job_market
WHERE experience_level <> 'Unknown'
GROUP BY experience_level
ORDER BY overall_avg_sal DESC;

-- 6.5 Outlier detection: salaries > 3 standard deviations from mean
WITH stats AS (
    SELECT
        AVG(salary_usd)    AS mean_sal,
        STDDEV(salary_usd) AS std_sal
    FROM job_market
)
SELECT
    job_id,
    job_title,
    company_name,
    location,
    salary_usd,
    ROUND((salary_usd - stats.mean_sal) / NULLIF(stats.std_sal, 0), 2) AS z_score
FROM job_market
CROSS JOIN stats
WHERE ABS((salary_usd - stats.mean_sal) / NULLIF(stats.std_sal, 0)) > 3
ORDER BY z_score DESC;


-- ============================================================
-- SECTION 7 : KPI SUMMARY VIEW
-- ============================================================

CREATE OR REPLACE VIEW v_job_market_kpis AS
SELECT
    COUNT(*)                                                        AS total_listings,
    COUNT(CASE WHEN hiring_status = 'Open' THEN 1 END)             AS open_positions,
    ROUND(COUNT(CASE WHEN hiring_status = 'Open' THEN 1 END)
          * 100.0 / COUNT(*), 1)                                   AS open_pct,
    ROUND(COUNT(CASE WHEN remote_work = 'Remote' THEN 1 END)
          * 100.0 / COUNT(*), 1)                                   AS remote_pct,
    ROUND(AVG(salary_usd), 0)                                      AS avg_salary_usd,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY salary_usd)        AS median_salary_usd,
    ROUND(AVG(company_rating), 2)                                  AS avg_company_rating,
    ROUND(AVG(applications), 0)                                    AS avg_applications,
    ROUND(AVG(years_of_exp), 1)                                    AS avg_years_exp,
    COUNT(DISTINCT company_name)                                   AS unique_companies,
    COUNT(DISTINCT location)                                       AS unique_locations
FROM job_market;

-- Query the KPI view
SELECT * FROM v_job_market_kpis;


-- ============================================================
-- SECTION 8 : INDEXING FOR PERFORMANCE
-- ============================================================

CREATE INDEX IF NOT EXISTS idx_job_title        ON job_market(job_title);
CREATE INDEX IF NOT EXISTS idx_experience_level ON job_market(experience_level);
CREATE INDEX IF NOT EXISTS idx_industry         ON job_market(industry);
CREATE INDEX IF NOT EXISTS idx_location         ON job_market(location);
CREATE INDEX IF NOT EXISTS idx_hiring_status    ON job_market(hiring_status);
CREATE INDEX IF NOT EXISTS idx_post_month       ON job_market(post_month);
CREATE INDEX IF NOT EXISTS idx_salary           ON job_market(salary_usd);
CREATE INDEX IF NOT EXISTS idx_company          ON job_market(company_name);

-- ============================================================
-- SECTION 9 : STORED PROCEDURE — REFRESH KPI SUMMARY TABLE
-- ============================================================

-- PostgreSQL stored procedure to refresh summary stats
CREATE OR REPLACE PROCEDURE sp_refresh_kpi_summary()
LANGUAGE plpgsql AS $$
BEGIN
    -- Drop and recreate KPI summary table
    DROP TABLE IF EXISTS job_market_kpi_summary;

    CREATE TABLE job_market_kpi_summary AS
    SELECT
        post_month                                              AS period,
        COUNT(*)                                               AS total_jobs,
        COUNT(CASE WHEN hiring_status = 'Open' THEN 1 END)    AS open_jobs,
        ROUND(AVG(salary_usd), 0)                              AS avg_salary,
        ROUND(AVG(company_rating), 2)                          AS avg_rating,
        ROUND(AVG(applications), 0)                            AS avg_apps,
        COUNT(CASE WHEN remote_work = 'Remote' THEN 1 END)    AS remote_count
    FROM job_market
    GROUP BY post_month
    ORDER BY post_month;

    RAISE NOTICE 'KPI summary table refreshed at %', NOW();
END;
$$;

-- Call it:
-- CALL sp_refresh_kpi_summary();

-- ============================================================
-- END OF SCRIPT
-- ============================================================
