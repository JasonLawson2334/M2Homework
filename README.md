# M2Homework

Deterministic survey workflow to rank MAcc programs/courses using the anonymized exit survey dataset.

## Project structure

- `data/` — dataset file used by the workflow
- `src/` — analysis script
- `outputs/` — generated results for grading/checking
- `.github/workflows/` — GitHub Actions workflow
- `requirements.txt` — dependency contract for CI

## Dataset

- `data/Grad Program Exit Survey Data.xlsx`

## What the workflow does

1. Reads the Excel workbook **without external Python dependencies** (ZIP/XML parsing from standard library).
2. Cleans the export by:
   - using row 1 headers,
   - skipping metadata row 2,
   - filtering to completed responses (`Finished = 1`),
   - extracting only course ranking columns.
3. Reshapes to tidy long format: `response_id`, `course`, `bucket`, `rank`.
4. Computes deterministic course ranking score:

   ```text
   net_preference_score = (Most Beneficial count - Least Beneficial count) / Taken count
   ```

5. Produces CSV/markdown/figure outputs in `outputs/`.

## Run locally

```bash
python src/build_rankings.py
```

## Outputs

- `outputs/course_rankings.csv` — all courses ranked (includes `overall_rank` and `eligible_rank`).
- `outputs/course_rankings_min10.csv` — only courses with >=10 taken responses, using contiguous `eligible_rank`.
- `outputs/tidy_course_rank_data.csv` — cleaned + reshaped long-form ranking data.
- `outputs/ranking_summary.md` — readable top ranking table.
- `outputs/course_rankings.svg` — figure of top eligible courses by net preference.
- `outputs/course_rankings_most_to_least.svg` — ordered bar chart from most liked to least liked classes.
- `outputs/most_vs_least_liked.md` — quick top-5 and bottom-5 table for eligible courses.
- `outputs/workflow_validation.json` — deterministic validation checks for ranking order and rank consistency.

## GitHub Actions

Workflow: `.github/workflows/course-ranking.yml`

- Runs on manual dispatch and when dataset/script/workflow/dependency file changes.
- Installs from `requirements.txt`.
- Executes `python src/build_rankings.py`.
- Uploads `outputs/` as artifact `course-ranking-outputs`.
