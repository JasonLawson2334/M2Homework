#!/usr/bin/env python3
"""Deterministic workflow to rank MAcc courses from an exit survey workbook.

Uses only Python standard library so it can run in minimal CI environments.
"""

from __future__ import annotations

import csv
import json
import re
import statistics
import zipfile
from collections import defaultdict
from pathlib import Path
from typing import Dict, List, Tuple
import xml.etree.ElementTree as ET

WORKBOOK_PATH = Path("data/Grad Program Exit Survey Data.xlsx")
OUTPUT_DIR = Path("outputs")

NS = {"a": "http://schemas.openxmlformats.org/spreadsheetml/2006/main"}
RANK_COLUMN_PATTERN = re.compile(
    r" - Ranks - (?P<bucket>Most Beneficial|Neutral|Least Beneficial|Did not take) - (?P<course>.+) - Rank$"
)


def _column_to_index(col: str) -> int:
    idx = 0
    for ch in col:
        idx = idx * 26 + (ord(ch) - ord("A") + 1)
    return idx - 1


def read_xlsx_rows(path: Path) -> List[List[str]]:
    with zipfile.ZipFile(path) as zf:
        shared_strings: List[str] = []
        if "xl/sharedStrings.xml" in zf.namelist():
            shared_root = ET.fromstring(zf.read("xl/sharedStrings.xml"))
            for si in shared_root.findall("a:si", NS):
                text_parts = [t.text or "" for t in si.findall(".//a:t", NS)]
                shared_strings.append("".join(text_parts))

        workbook = ET.fromstring(zf.read("xl/workbook.xml"))
        first_sheet = workbook.find("a:sheets/a:sheet", NS)
        relationship_id = first_sheet.attrib[
            "{http://schemas.openxmlformats.org/officeDocument/2006/relationships}id"
        ]

        rels_root = ET.fromstring(zf.read("xl/_rels/workbook.xml.rels"))
        target = None
        for rel in rels_root:
            if rel.attrib.get("Id") == relationship_id:
                target = rel.attrib.get("Target")
                break
        if not target:
            raise RuntimeError("Could not find worksheet target in workbook relationships.")

        if not target.startswith("xl/"):
            target = f"xl/{target}"

        sheet_root = ET.fromstring(zf.read(target))

    rows: List[List[str]] = []
    for row in sheet_root.findall(".//a:sheetData/a:row", NS):
        row_values: Dict[int, str] = {}
        max_idx = -1
        for cell in row.findall("a:c", NS):
            ref = cell.attrib["r"]
            col = re.match(r"[A-Z]+", ref).group(0)
            idx = _column_to_index(col)
            max_idx = max(max_idx, idx)

            cell_type = cell.attrib.get("t")
            value_node = cell.find("a:v", NS)
            if value_node is None:
                inline = cell.find("a:is/a:t", NS)
                value = (inline.text if inline is not None else "") or ""
            else:
                raw = value_node.text or ""
                if cell_type == "s":
                    value = shared_strings[int(raw)]
                else:
                    value = raw
            row_values[idx] = value.strip()

        if max_idx >= 0:
            rows.append([row_values.get(i, "") for i in range(max_idx + 1)])

    return rows


def normalize_course_name(course: str) -> str:
    return re.sub(r"\s+", " ", course).strip()


def as_int(value: str) -> int | None:
    if value == "":
        return None
    try:
        return int(float(value))
    except ValueError:
        return None


def build_long_rank_data(headers: List[str], response_rows: List[List[str]]) -> List[dict]:
    rank_columns: List[Tuple[int, str, str]] = []
    for idx, header in enumerate(headers):
        match = RANK_COLUMN_PATTERN.search(header)
        if match:
            rank_columns.append((idx, match.group("bucket"), normalize_course_name(match.group("course"))))

    finished_idx = headers.index("Finished") if "Finished" in headers else None
    response_id_idx = headers.index("Response ID") if "Response ID" in headers else None

    long_rows: List[dict] = []
    for row in response_rows:
        if response_id_idx is not None and (response_id_idx >= len(row) or row[response_id_idx] == ""):
            continue
        if finished_idx is not None:
            finished = as_int(row[finished_idx] if finished_idx < len(row) else "")
            if finished != 1:
                continue

        response_id = row[response_id_idx] if response_id_idx is not None and response_id_idx < len(row) else ""

        for col_idx, bucket, course in rank_columns:
            if col_idx >= len(row):
                continue
            rank = as_int(row[col_idx])
            if rank is None:
                continue
            long_rows.append(
                {
                    "response_id": response_id,
                    "course": course,
                    "bucket": bucket,
                    "rank": rank,
                }
            )

    return long_rows


def summarize_rankings(long_rows: List[dict]) -> List[dict]:
    by_course: Dict[str, dict] = defaultdict(
        lambda: {
            "course": "",
            "most_count": 0,
            "neutral_count": 0,
            "least_count": 0,
            "did_not_take_count": 0,
            "taken_count": 0,
            "rank_values": [],
        }
    )

    for item in long_rows:
        course = item["course"]
        bucket = item["bucket"]
        rank = item["rank"]
        entry = by_course[course]
        entry["course"] = course
        entry["rank_values"].append(rank)

        if bucket == "Most Beneficial":
            entry["most_count"] += 1
            entry["taken_count"] += 1
        elif bucket == "Neutral":
            entry["neutral_count"] += 1
            entry["taken_count"] += 1
        elif bucket == "Least Beneficial":
            entry["least_count"] += 1
            entry["taken_count"] += 1
        else:
            entry["did_not_take_count"] += 1

    summary: List[dict] = []
    for course, entry in by_course.items():
        taken = entry["taken_count"]
        most = entry["most_count"]
        least = entry["least_count"]
        neutral = entry["neutral_count"]

        if taken > 0:
            net_preference = (most - least) / taken
            most_share = most / taken
            least_share = least / taken
            avg_rank = statistics.mean(entry["rank_values"])
        else:
            net_preference = 0.0
            most_share = 0.0
            least_share = 0.0
            avg_rank = 0.0

        summary.append(
            {
                "course": course,
                "most_count": most,
                "neutral_count": neutral,
                "least_count": least,
                "did_not_take_count": entry["did_not_take_count"],
                "taken_count": taken,
                "net_preference_score": round(net_preference, 6),
                "most_share": round(most_share, 6),
                "least_share": round(least_share, 6),
                "average_rank_value": round(avg_rank, 6),
            }
        )

    summary.sort(
        key=lambda r: (
            -r["net_preference_score"],
            -r["most_share"],
            r["least_share"],
            -r["taken_count"],
            r["course"],
        )
    )

    for idx, row in enumerate(summary, start=1):
        row["overall_rank"] = idx

    eligible_counter = 0
    for row in summary:
        if row["taken_count"] >= 10:
            eligible_counter += 1
            row["eligible_rank"] = eligible_counter
        else:
            row["eligible_rank"] = ""

    return summary


def write_csv(path: Path, rows: List[dict], fieldnames: List[str]) -> None:
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def write_markdown_summary(path: Path, summary_rows: List[dict], top_n: int = 10, min_taken: int = 10) -> None:
    eligible = [row for row in summary_rows if row["taken_count"] >= min_taken]
    lines = [
        "# Course Ranking Summary",
        "",
        "Ranking logic: courses are ordered by **net preference score = (Most Beneficial - Least Beneficial) / Taken**.",
        f"Only courses with at least **{min_taken}** taken responses are included in the primary ranking table.",
        "",
        f"Top {min(top_n, len(eligible))} eligible courses:",
        "",
        "Full ordered bar chart file: `outputs/course_rankings_most_to_least.svg`.",
        "",
        "| Rank | Course | Net Preference | Most | Neutral | Least | Taken |",
        "|---:|---|---:|---:|---:|---:|---:|",
    ]
    for row in eligible[:top_n]:
        lines.append(
            f"| {row['eligible_rank']} | {row['course']} | {row['net_preference_score']:.3f} | {row['most_count']} | {row['neutral_count']} | {row['least_count']} | {row['taken_count']} |"
        )

    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def write_svg_chart(path: Path, summary_rows: List[dict], max_bars: int = 12) -> None:
    rows = summary_rows[:max_bars]
    width, height = 1100, 650
    left_margin, right_margin, top_margin, bottom_margin = 420, 80, 80, 60
    plot_width = width - left_margin - right_margin
    plot_height = height - top_margin - bottom_margin
    bar_gap = 8
    bar_height = int((plot_height - (len(rows) - 1) * bar_gap) / max(1, len(rows)))

    def x_pos(value: float) -> float:
        clamped = max(-1.0, min(1.0, value))
        return left_margin + (clamped + 1) / 2 * plot_width

    zero_x = x_pos(0)

    svg_lines = [
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" viewBox="0 0 {width} {height}">',
        '<style>text{font-family:Arial,sans-serif;} .title{font-size:24px;font-weight:bold;} .subtitle{font-size:14px;fill:#333;} .label{font-size:14px;} .axis{font-size:12px;fill:#444;} </style>',
        f'<text x="{left_margin}" y="40" class="title">Top Courses by Net Preference Score</text>',
        f'<text x="{left_margin}" y="62" class="subtitle">Score = (Most Beneficial - Least Beneficial) / Taken responses</text>',
        f'<line x1="{left_margin}" y1="{top_margin}" x2="{width-right_margin}" y2="{top_margin}" stroke="#ddd"/>',
        f'<line x1="{left_margin}" y1="{height-bottom_margin}" x2="{width-right_margin}" y2="{height-bottom_margin}" stroke="#ddd"/>',
        f'<line x1="{zero_x}" y1="{top_margin}" x2="{zero_x}" y2="{height-bottom_margin}" stroke="#888" stroke-dasharray="4,4"/>',
    ]

    for tick in [-1, -0.5, 0, 0.5, 1]:
        x = x_pos(tick)
        svg_lines.append(f'<line x1="{x}" y1="{height-bottom_margin}" x2="{x}" y2="{height-bottom_margin+6}" stroke="#999"/>')
        svg_lines.append(f'<text x="{x}" y="{height-bottom_margin+24}" text-anchor="middle" class="axis">{tick:g}</text>')

    for i, row in enumerate(rows):
        y = top_margin + i * (bar_height + bar_gap)
        y_mid = y + bar_height / 2
        score = row["net_preference_score"]
        x1 = min(zero_x, x_pos(score))
        x2 = max(zero_x, x_pos(score))
        bar_color = "#1f77b4" if score >= 0 else "#d62728"

        svg_lines.append(
            f'<rect x="{x1}" y="{y}" width="{max(1, x2-x1)}" height="{bar_height}" fill="{bar_color}" opacity="0.9"/>'
        )
        svg_lines.append(
            f'<text x="{left_margin-10}" y="{y_mid+5}" text-anchor="end" class="label">{row["eligible_rank"]}. {row["course"]}</text>'
        )
        label_x = x2 + 8 if score >= 0 else x1 - 8
        anchor = "start" if score >= 0 else "end"
        svg_lines.append(
            f'<text x="{label_x}" y="{y_mid+5}" text-anchor="{anchor}" class="label">{score:.3f}</text>'
        )

    svg_lines.append("</svg>")
    path.write_text("\n".join(svg_lines) + "\n", encoding="utf-8")




def write_ordered_bar_chart(path: Path, summary_rows: List[dict]) -> None:
    """Write a full ordered bar chart (most liked to least liked) for eligible courses."""
    rows = list(summary_rows)
    width = 1200
    top_margin, bottom_margin = 80, 60
    left_margin, right_margin = 460, 90
    bar_gap = 8
    bar_height = 26
    plot_height = max(140, len(rows) * (bar_height + bar_gap) - bar_gap)
    height = top_margin + plot_height + bottom_margin
    plot_width = width - left_margin - right_margin

    def x_pos(value: float) -> float:
        clamped = max(-1.0, min(1.0, value))
        return left_margin + (clamped + 1) / 2 * plot_width

    zero_x = x_pos(0)

    svg_lines = [
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" viewBox="0 0 {width} {height}">',
        '<style>text{font-family:Arial,sans-serif;} .title{font-size:24px;font-weight:bold;} .subtitle{font-size:14px;fill:#333;} .label{font-size:14px;} .axis{font-size:12px;fill:#444;} </style>',
        f'<text x="{left_margin}" y="40" class="title">Course Ranking: Most Liked to Least Liked</text>',
        f'<text x="{left_margin}" y="62" class="subtitle">Ordered by net preference score = (Most Beneficial - Least Beneficial) / Taken</text>',
        f'<line x1="{left_margin}" y1="{top_margin}" x2="{width-right_margin}" y2="{top_margin}" stroke="#ddd"/>',
        f'<line x1="{left_margin}" y1="{height-bottom_margin}" x2="{width-right_margin}" y2="{height-bottom_margin}" stroke="#ddd"/>',
        f'<line x1="{zero_x}" y1="{top_margin}" x2="{zero_x}" y2="{height-bottom_margin}" stroke="#888" stroke-dasharray="4,4"/>',
    ]

    for tick in [-1, -0.5, 0, 0.5, 1]:
        x = x_pos(tick)
        svg_lines.append(f'<line x1="{x}" y1="{height-bottom_margin}" x2="{x}" y2="{height-bottom_margin+6}" stroke="#999"/>')
        svg_lines.append(f'<text x="{x}" y="{height-bottom_margin+24}" text-anchor="middle" class="axis">{tick:g}</text>')

    for i, row in enumerate(rows):
        y = top_margin + i * (bar_height + bar_gap)
        y_mid = y + bar_height / 2
        score = row["net_preference_score"]
        x1 = min(zero_x, x_pos(score))
        x2 = max(zero_x, x_pos(score))
        bar_color = "#1f77b4" if score >= 0 else "#d62728"

        svg_lines.append(
            f'<rect x="{x1}" y="{y}" width="{max(1, x2-x1)}" height="{bar_height}" fill="{bar_color}" opacity="0.9"/>'
        )
        svg_lines.append(
            f'<text x="{left_margin-10}" y="{y_mid+5}" text-anchor="end" class="label">{row["eligible_rank"]}. {row["course"]}</text>'
        )
        label_x = x2 + 8 if score >= 0 else x1 - 8
        anchor = "start" if score >= 0 else "end"
        svg_lines.append(
            f'<text x="{label_x}" y="{y_mid+5}" text-anchor="{anchor}" class="label">{score:.3f}</text>'
        )

    svg_lines.append("</svg>")
    path.write_text("\n".join(svg_lines) + "\n", encoding="utf-8")



def write_json(path: Path, payload: dict) -> None:
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def validate_rankings(summary_rows: List[dict], min_taken: int) -> dict:
    eligible = [row for row in summary_rows if row["taken_count"] >= min_taken]

    sorted_desc = True
    for i in range(len(summary_rows) - 1):
        if summary_rows[i]["net_preference_score"] < summary_rows[i + 1]["net_preference_score"]:
            sorted_desc = False
            break

    contiguous_eligible = True
    for idx, row in enumerate(eligible, start=1):
        if row.get("eligible_rank") != idx:
            contiguous_eligible = False
            break

    missing_eligible_rank = any((row["taken_count"] >= min_taken and row.get("eligible_rank") == "") for row in summary_rows)

    return {
        "min_taken_threshold": min_taken,
        "num_ranked_courses": len(summary_rows),
        "num_eligible_courses": len(eligible),
        "is_sorted_descending": sorted_desc,
        "has_contiguous_eligible_ranks": contiguous_eligible,
        "has_missing_eligible_rank": missing_eligible_rank,
        "top_course": eligible[0]["course"] if eligible else "",
        "bottom_course": eligible[-1]["course"] if eligible else "",
    }


def write_rank_extremes(path: Path, summary_rows: List[dict], min_taken: int, n: int = 5) -> None:
    eligible = [row for row in summary_rows if row["taken_count"] >= min_taken]
    top_rows = eligible[:n]
    bottom_rows = list(reversed(eligible[-n:]))

    lines = [
        "# Most and Least Liked Courses",
        "",
        f"Threshold: courses with at least **{min_taken}** taken responses.",
        "",
        f"## Top {len(top_rows)} most liked",
        "",
        "| Rank | Course | Net Preference | Taken |",
        "|---:|---|---:|---:|",
    ]
    for row in top_rows:
        lines.append(f"| {row['eligible_rank']} | {row['course']} | {row['net_preference_score']:.3f} | {row['taken_count']} |")

    lines.extend(["", f"## Bottom {len(bottom_rows)} least liked", "", "| Rank | Course | Net Preference | Taken |", "|---:|---|---:|---:|"])
    for row in bottom_rows:
        lines.append(f"| {row['eligible_rank']} | {row['course']} | {row['net_preference_score']:.3f} | {row['taken_count']} |")

    path.write_text("\n".join(lines) + "\n", encoding="utf-8")

def main() -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    rows = read_xlsx_rows(WORKBOOK_PATH)
    if len(rows) < 3:
        raise RuntimeError("Workbook does not include expected header + metadata + response rows.")

    headers = rows[0]
    data_rows = rows[2:]

    long_rows = build_long_rank_data(headers, data_rows)
    summary = summarize_rankings(long_rows)

    write_csv(
        OUTPUT_DIR / "course_rankings.csv",
        summary,
        [
            "overall_rank",
            "course",
            "eligible_rank",
            "net_preference_score",
            "most_share",
            "least_share",
            "most_count",
            "neutral_count",
            "least_count",
            "did_not_take_count",
            "taken_count",
            "average_rank_value",
        ],
    )

    min_taken_threshold = 10
    summary_min_taken = [row for row in summary if row["taken_count"] >= min_taken_threshold]
    write_csv(
        OUTPUT_DIR / f"course_rankings_min{min_taken_threshold}.csv",
        summary_min_taken,
        [
            "overall_rank",
            "course",
            "eligible_rank",
            "net_preference_score",
            "most_share",
            "least_share",
            "most_count",
            "neutral_count",
            "least_count",
            "did_not_take_count",
            "taken_count",
            "average_rank_value",
        ],
    )

    write_csv(
        OUTPUT_DIR / "tidy_course_rank_data.csv",
        long_rows,
        ["response_id", "course", "bucket", "rank"],
    )

    write_markdown_summary(
        OUTPUT_DIR / "ranking_summary.md",
        summary,
        top_n=10,
        min_taken=min_taken_threshold,
    )
    write_rank_extremes(OUTPUT_DIR / "most_vs_least_liked.md", summary, min_taken=min_taken_threshold, n=5)
    write_svg_chart(OUTPUT_DIR / "course_rankings.svg", summary_min_taken, max_bars=12)
    write_ordered_bar_chart(OUTPUT_DIR / "course_rankings_most_to_least.svg", summary_min_taken)

    validation = validate_rankings(summary, min_taken=min_taken_threshold)
    write_json(OUTPUT_DIR / "workflow_validation.json", validation)

    print(f"Processed {len(data_rows)} responses into {len(long_rows)} tidy ranking rows.")
    print(f"Ranked {len(summary)} courses. Outputs saved to: {OUTPUT_DIR.resolve()}")


if __name__ == "__main__":
    main()
