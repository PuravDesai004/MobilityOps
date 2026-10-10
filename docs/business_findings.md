# Three measured findings from Yellow Taxi records

Scope: all three **verified source files, January–March 2026**, queried read-only from the
existing real DuckDB database on 10 October 2026. No full preparation was rerun.
The [export receipt](../reports/analysis/real/business_insights.json) records generation time,
exact artifact IDs, query hashes, verification inputs, definitions and result checksums.
These are accepted source records, not globally deduplicated physical journeys.

## A. Pickup activity concentrates in a few zones and hours

**Measured:** Upper East Side South has **479,826 accepted pickups**, **4.33%** of all
11,077,201 accepted records. Its busiest hour is **17:00–18:00 New York wall clock**, with
**38,469 records** across the three files, or **8.02%** of that zone's pickups. Its signed
recorded charge is **$10,422,280.56**, and weighted mean duration is **12.29 minutes**.
Upper East Side North follows with 449,683 pickups (peak 15:00–16:00); Midtown Center has
447,264 (peak 18:00–19:00). Peak-hour ties select the earliest hour.

**Evidence:** [pickup_zone_hour.sql](../sql/analysis/pickup_zone_hour.sql) and its
[CSV result](../reports/analysis/real/pickup_zone_hour.csv). All zone rows together reconcile
to the verified accepted count and $330,026,139.73 charge, including out-of-partition dates.

**Interpretation:** These concentrations could inform where and when an operator investigates
vehicle positioning or staffing. Test a planning decision with current operations data.

**Limitation:** Observed Yellow Taxi activity does not measure total transportation demand,
unserved requests, vehicle supply, waiting time, profit or staffing effectiveness. Combining
three files hides weekday and seasonal variation. Naive wall-clock durations have DST limits.

## B. February's lower monthly count hides a higher daily rate

**Measured:** February has **325,024 fewer accepted records than January (−8.73%)**, but
its in-partition pickups per calendar day rise **1.05%**. March's monthly accepted count rises
**16.25%**, while its comparable daily rate rises **5.00%** from February.

| Source-file month | Accepted records, all dates | Signed charge USD | Average charge USD | Days / observed days | In-month trips/day | Count MoM | Daily-rate MoM |
| --- | ---: | ---: | ---: | --- | ---: | ---: | ---: |
| January 2026 | 3,724,888 | 108,686,701.73 | 29.18 | 31 / 31 | 120,157.45 | unavailable | unavailable |
| February 2026 | 3,399,864 | 102,381,010.56 | 30.11 | 28 / 28 | 121,423.14 | −8.73% | +1.05% |
| March 2026 | 3,952,449 | 118,958,427.44 | 30.10 | 31 / 31 | 127,497.74 | +16.25% | +5.00% |

Full-file charge changes are −$6,305,691.17 (−5.80%) in February and +$16,577,416.88
(+16.19%) in March. Average-charge changes are +$0.9347511 (+3.20%) and −$0.0158693
(−0.05%), respectively. CSV/JSON preserve calculation precision; table display is rounded.

**Evidence:** [monthly_changes.sql](../sql/analysis/monthly_changes.sql),
[monthly CSV](../reports/analysis/real/monthly_changes.csv),
[pickup_month_coverage.sql](../sql/analysis/pickup_month_coverage.sql) and its
[coverage CSV](../reports/analysis/real/pickup_month_coverage.csv).

**Definitions and exclusions:** Full-file counts/charges reconcile without exclusions.
For per-day comparison only, a pickup must fall within its own source-file month:

| Source month | Excluded accepted records | Excluded signed charge USD | In-partition records |
| --- | ---: | ---: | ---: |
| January | 7 | 285.04 | 3,724,881 |
| February | 16 | 330.79 | 3,399,848 |
| March | 19 | 575.43 | 3,952,430 |
| Total | 42 | 1,191.26 | 11,077,159 |

The corresponding in-partition charge is **$330,024,948.47**. Thus the difference from the
headline count/charge is fully explained by 42 retained records; nothing was deleted.
Actual accepted dates span **2008-12-31–2026-04-01**, while source-file months are Jan–Mar
2026. The coverage cross-tab keeps these two meanings separate, even for anomalous dates.

Calendar-day rate = in-partition accepted count / 31, 28 or 31 days. Observed-day rate uses
distinct observed in-month dates; the two rates happen to match here. This is not a check
that every day was reported completely. MoM compares adjacent source months only;
`NULLIF` protects zero denominators and unavailable comparisons remain null/blank.

**Interpretation:** Normalize month length before discussing changes in operational workload.
A lower monthly count need not mean lower daily activity.

**Limitation:** Per-day normalization does not adjust weekday mix, holidays, weather,
reporting changes or other causes. These records alone do not establish why activity changed.
Signed recorded charge includes adjustments and is neither profit nor complete revenue.

## C. Warnings are common; quarantines are rare under this contract

**Measured:** **3,186,377 distinct accepted records (28.77%)** have at least one warning.
Only **5 of 11,077,206 source records (0.00004514%)** are quarantined; all five have
`DROPOFF_BEFORE_PICKUP`.

| Source month | Distinct accepted with warnings | Accepted denominator | Warning rate | Quarantined / source denominator |
| --- | ---: | ---: | ---: | --- |
| January | 1,142,828 | 3,724,888 | 30.68% | 1 / 3,724,889 |
| February | 1,063,507 | 3,399,864 | 31.28% | 2 / 3,399,866 |
| March | 980,042 | 3,952,449 | 24.80% | 2 / 3,952,451 |

Missing passenger count and unknown payment code each occur in **3,057,120** accepted
records. Their individual counts can overlap; equal counts alone do not establish that they
affect the same records. Code totals must never be added to estimate distinct warned trips.

Among zone/month groups with at least **10,000 accepted records**, January Astoria has the
highest warning rate: **8,593 / 10,044 = 85.55%**, versus January's **30.68%** overall rate.
January Alphabet City has 12,682 / 16,613 = 76.34%. All groups, including small denominators,
remain in the CSV; an elevated flag requires both sufficient volume and a rate above its
source-month baseline. Quarantine flags separately require at least 10,000 source records.

**Evidence:** [quality_month.sql](../sql/analysis/quality_month.sql) →
[monthly quality CSV](../reports/analysis/real/quality_month.csv);
[warning_codes.sql](../sql/analysis/warning_codes.sql) →
[warning CSV](../reports/analysis/real/warning_codes.csv);
[quality_zones.sql](../sql/analysis/quality_zones.sql) →
[zone CSV](../reports/analysis/real/quality_zones.csv);
[quarantine_codes.sql](../sql/analysis/quarantine_codes.sql) →
[reason CSV](../reports/analysis/real/quarantine_codes.csv).

**Interpretation:** Investigate optional-field coverage in high-volume, high-warning groups
before relying on passenger or payment segmentation. Keep useful accepted measurements
available with their warnings.

**Limitation:** Warnings mean missing/unusual fields under this project's policy, not proven
incorrect journeys. The 10,000-record threshold is a transparent screening choice, not a
statistical significance test. Low quarantine rate does not prove complete source accuracy.
Unresolved key 0 is retained when present; real data has no pickup rows in that bucket.
Official keys 264 (N/A; borough Unknown) and 265 (Outside of NYC; borough N/A) remain valid,
distinct members. The offline tests also exercise an unresolved bucket.

## How to read the SQL

- **CTE / `WITH`:** names an intermediate result, such as hourly zone counts, so the next
  step is easy to read. It does not create a permanent table here.
- **Aggregation:** `GROUP BY`, `COUNT` and `SUM` reduce records into a stated grain.
- **Join:** attaches zone names from a dimension; key 0 and official Unknown members survive.
- **Window function:** `ROW_NUMBER` ranks zones/hours without collapsing their rows;
  `LAG` looks at the previous source month; partitioned sums provide month baselines.
- **Safe division:** `NULLIF(denominator, 0)` produces an unavailable value instead of a
  divide-by-zero failure or an invented percentage.
- **Weighted mean:** summed duration / summed record count. Averaging zone averages would
  give a tiny group as much influence as a busy one.
- **`UNNEST`:** expands an array into one row per code. Distinct records per code prevent
  duplicate entries within a record from inflating that code, while different codes overlap.

The [DuckDB window-function reference](https://duckdb.org/docs/current/sql/functions/window_functions)
and [list-function reference](https://duckdb.org/docs/current/sql/functions/list) were reviewed
for the installed SQL engine. Local tests execute these exact files on small synthetic tables.

## Reproduce without rerunning preparation

```powershell
Set-Location 'C:\Users\purav\OneDrive\Desktop\Data Projects\MobilityOps'
$env:PYTHONPATH = "$PWD\src"
.\.venv\Scripts\python.exe scripts/run_analysis.py --snapshot demo/published
.\.venv\Scripts\python.exe -m mobilityops.cli validate-snapshot --path demo/published
```

The first command requires the existing private real database and passed per-artifact
reports. It opens the database read-only, validates scope/live counts/charges, saves seven
small CSVs and JSON, then attaches the JSON to a new snapshot generation. Original three
snapshot files retain their exact bytes. The app reads the fourth file only through the
manifest and validator. A source/pipeline snapshot rerun must be followed by a fresh analysis
attachment. A new clone can inspect committed results and launch the demo without that DB.
