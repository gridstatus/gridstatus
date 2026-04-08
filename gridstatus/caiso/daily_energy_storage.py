from __future__ import annotations

import ast
from typing import Any

import pandas as pd
import requests

from gridstatus import utils
from gridstatus.gs_logging import logger


def _report_day_start(date: str | pd.Timestamp, tz: str) -> pd.Timestamp:
    d = utils._handle_date(date, tz=tz)
    if not isinstance(d, pd.Timestamp):
        d = pd.Timestamp(d)
    return d.tz_convert(tz).normalize()


def load_daily_energy_storage_report(
    date: str | pd.Timestamp,
    tz: str,
    verbose: bool = False,
) -> tuple[str, pd.Timestamp]:
    html = _fetch_daily_energy_storage_html(date, tz, verbose)
    report_start = _report_day_start(date, tz)
    return html, report_start


def _fetch_daily_energy_storage_html(
    date: str | pd.Timestamp,
    tz: str,
    verbose: bool = False,
) -> str:
    day = _report_day_start(date, tz)
    slug = day.strftime("%b-%d-%Y").lower()
    primary_url = (
        f"https://www.caiso.com/documents/daily-energy-storage-report-{slug}.html"
    )
    if verbose:
        logger.info(f"Fetching URL: {primary_url}")
    response = requests.get(primary_url, timeout=60)
    if response.status_code != 200:
        corrected_url = (
            f"https://www.caiso.com/documents/"
            f"daily-energy-storage-report-{slug}-corrected.html"
        )
        if verbose:
            logger.info(f"Fetching URL: {corrected_url}")
        response = requests.get(corrected_url, timeout=60)
    if response.status_code != 200:
        from gridstatus.base import NoDataFoundException

        raise NoDataFoundException(
            f"No Daily Energy Storage report for {day.strftime('%Y-%m-%d')}: "
            f"HTTP {response.status_code}",
        )
    body: bytes = response.content
    return body.decode("utf-8")


def _extract_js_array_literal(html: str, var_name: str) -> str | None:
    assign_needle = f"{var_name} = ["
    pos = html.find(assign_needle)
    if pos == -1:
        assign_needle = f"var {var_name} = ["
        pos = html.find(assign_needle)
    if pos == -1:
        return None
    i = pos + len(assign_needle) - 1
    if i < 0 or html[i] != "[":
        return None
    depth = 0
    start = i
    while i < len(html):
        c = html[i]
        if c == "[":
            depth += 1
        elif c == "]":
            depth -= 1
            if depth == 0:
                return html[start : i + 1]
        i += 1
    return None


def _parse_js_array(html: str, var_name: str) -> list[Any]:
    raw = _extract_js_array_literal(html, var_name)
    if raw is None:
        return []
    try:
        val = ast.literal_eval(raw)
    except (SyntaxError, ValueError):
        return []
    if isinstance(val, list):
        return val
    return []


def _interval_index(
    report_start: pd.Timestamp,
    n: int,
    minutes: int,
) -> tuple[pd.Series, pd.Series]:
    if n <= 0:
        return (
            pd.Series(dtype="datetime64[ns, US/Pacific]"),
            pd.Series(dtype="datetime64[ns, US/Pacific]"),
        )
    starts = pd.date_range(
        start=report_start,
        periods=n,
        freq=f"{minutes}min",
        tz=report_start.tz,
    )
    ends = starts + pd.Timedelta(minutes=minutes)
    return starts, ends


def _long_energy_awards(
    report_start: pd.Timestamp,
    values_standalone: list[Any],
    values_hybrid: list[Any],
    minutes: int,
    product: str,
) -> pd.DataFrame:
    n = min(len(values_standalone), len(values_hybrid))
    if n == 0:
        return pd.DataFrame(
            columns=[
                "Interval Start",
                "Interval End",
                "Product",
                "Type",
                "MW",
            ],
        )
    starts, ends = _interval_index(report_start, n, minutes)
    s_rows = pd.DataFrame(
        {
            "Interval Start": starts,
            "Interval End": ends,
            "Product": product,
            "Type": "Standalone",
            "MW": values_standalone[:n],
        },
    )
    h_rows = pd.DataFrame(
        {
            "Interval Start": starts,
            "Interval End": ends,
            "Product": product,
            "Type": "Hybrid",
            "MW": values_hybrid[:n],
        },
    )
    return pd.concat([s_rows, h_rows], ignore_index=True)


def _long_as_awards(
    report_start: pd.Timestamp,
    series_map: dict[str, tuple[str, str]],
    minutes: int,
    html: str,
) -> pd.DataFrame:
    parts: list[pd.DataFrame] = []
    for var_name, (product, type_label) in series_map.items():
        vals = _parse_js_array(html, var_name)
        n = len(vals)
        if n == 0:
            continue
        starts, ends = _interval_index(report_start, n, minutes)
        parts.append(
            pd.DataFrame(
                {
                    "Interval Start": starts,
                    "Interval End": ends,
                    "Product": product,
                    "Type": type_label,
                    "MW": vals,
                },
            ),
        )
    if not parts:
        return pd.DataFrame(
            columns=[
                "Interval Start",
                "Interval End",
                "Product",
                "Type",
                "MW",
            ],
        )
    return pd.concat(parts, ignore_index=True)


def _hybrid_bid_var_names(order: list[tuple[str, str]]) -> list[tuple[str, str]]:
    out: list[tuple[str, str]] = []
    for var, label in order:
        if var.endswith("_ss"):
            base = var[:-3]
            out.append((f"{base}_hybrid_ss", label))
        else:
            head, idx = var.rsplit("_", 1)
            out.append((f"{head}_hybrid_{idx}", label))
    return out


BID_IFM_RANGE_ORDER: list[tuple[str, str]] = [
    ("bid_ifm_pos_ss", "Self Schedule"),
    ("bid_ifm_pos_1", "[-$150,-$100]"),
    ("bid_ifm_pos_2", "(-$100,-$50]"),
    ("bid_ifm_pos_3", "(-$50,-$15]"),
    ("bid_ifm_pos_4", "(-$15, $0]"),
    ("bid_ifm_pos_5", "($0, $15]"),
    ("bid_ifm_pos_6", "($15, $50]"),
    ("bid_ifm_pos_7", "($50, $100]"),
    ("bid_ifm_pos_8", "($100, $200]"),
    ("bid_ifm_pos_9", "($200, $500]"),
    ("bid_ifm_pos_10", "($500, $1000]"),
    ("bid_ifm_pos_11", "($1000, $2000]"),
]

BID_IFM_NEG_ORDER: list[tuple[str, str]] = [
    ("bid_ifm_neg_ss", "Self Schedule"),
    ("bid_ifm_neg_1", "[-$150,-$100]"),
    ("bid_ifm_neg_2", "(-$100,-$50]"),
    ("bid_ifm_neg_3", "(-$50,-$15]"),
    ("bid_ifm_neg_4", "(-$15, $0]"),
    ("bid_ifm_neg_5", "($0, $15]"),
    ("bid_ifm_neg_6", "($15, $50]"),
    ("bid_ifm_neg_7", "($50, $100]"),
    ("bid_ifm_neg_8", "($100, $200]"),
    ("bid_ifm_neg_9", "($200, $500]"),
    ("bid_ifm_neg_10", "($500, $1000]"),
    ("bid_ifm_neg_11", "($1000, $2000]"),
]

BID_RTPD_RANGE_ORDER: list[tuple[str, str]] = [
    ("bid_rtpd_pos_ss", "Self Schedule"),
    ("bid_rtpd_pos_1", "[-$150,-$100]"),
    ("bid_rtpd_pos_2", "(-$100,-$50]"),
    ("bid_rtpd_pos_3", "(-$50,-$15]"),
    ("bid_rtpd_pos_4", "(-$15, $0]"),
    ("bid_rtpd_pos_5", "($0, $15]"),
    ("bid_rtpd_pos_6", "($15, $50]"),
    ("bid_rtpd_pos_7", "($50, $100]"),
    ("bid_rtpd_pos_8", "($100, $200]"),
    ("bid_rtpd_pos_9", "($200, $500]"),
    ("bid_rtpd_pos_10", "($500, $1000]"),
    ("bid_rtpd_pos_11", "($1000, $2000]"),
]

BID_RTPD_NEG_ORDER: list[tuple[str, str]] = [
    ("bid_rtpd_neg_ss", "Self Schedule"),
    ("bid_rtpd_neg_1", "[-$150,-$100]"),
    ("bid_rtpd_neg_2", "(-$100,-$50]"),
    ("bid_rtpd_neg_3", "(-$50,-$15]"),
    ("bid_rtpd_neg_4", "(-$15, $0]"),
    ("bid_rtpd_neg_5", "($0, $15]"),
    ("bid_rtpd_neg_6", "($15, $50]"),
    ("bid_rtpd_neg_7", "($50, $100]"),
    ("bid_rtpd_neg_8", "($100, $200]"),
    ("bid_rtpd_neg_9", "($200, $500]"),
    ("bid_rtpd_neg_10", "($500, $1000]"),
    ("bid_rtpd_neg_11", "($1000, $2000]"),
]


def _bid_stack_to_df(
    report_start: pd.Timestamp,
    html: str,
    var_order: list[tuple[str, str]],
    minutes: int,
    operation: str,
    type_label: str,
) -> pd.DataFrame:
    lengths: list[int] = []
    for var_name, _ in var_order:
        vals = _parse_js_array(html, var_name)
        lengths.append(len(vals))
    n = min(lengths) if lengths else 0
    if n == 0:
        return pd.DataFrame(
            columns=[
                "Interval Start",
                "Interval End",
                "Bid Range",
                "Operation",
                "Type",
                "MW",
            ],
        )
    starts, ends = _interval_index(report_start, n, minutes)
    rows = []
    for var_name, bid_range in var_order:
        vals = _parse_js_array(html, var_name)
        rows.append(
            pd.DataFrame(
                {
                    "Interval Start": starts,
                    "Interval End": ends,
                    "Bid Range": bid_range,
                    "Operation": operation,
                    "Type": type_label,
                    "MW": vals[:n],
                },
            ),
        )
    if not rows:
        return pd.DataFrame(
            columns=[
                "Interval Start",
                "Interval End",
                "Bid Range",
                "Operation",
                "Type",
                "MW",
            ],
        )
    return pd.concat(rows, ignore_index=True)


def _downsample_5min_to_15min(values: list[Any]) -> list[Any]:
    if len(values) % 3 != 0 or len(values) == 0:
        return values
    out: list[Any] = []
    for i in range(0, len(values), 3):
        chunk = values[i : i + 3]
        out.append(sum(chunk) / len(chunk))
    return out


def _downsample_5min_to_60min(values: list[Any]) -> list[Any]:
    if len(values) % 12 != 0 or len(values) == 0:
        return values
    out: list[Any] = []
    for i in range(0, len(values), 12):
        chunk = values[i : i + 12]
        out.append(sum(chunk) / len(chunk))
    return out


def build_storage_awards_fmm(
    html: str,
    report_start: pd.Timestamp,
) -> pd.DataFrame:
    e_s = _downsample_5min_to_15min(_parse_js_array(html, "tot_energy_rtpd"))
    e_h = _downsample_5min_to_15min(_parse_js_array(html, "tot_energy_hybrid_rtpd"))
    energy = _long_energy_awards(
        report_start,
        e_s,
        e_h,
        15,
        "Energy",
    )
    as_standalone = _long_as_awards(
        report_start,
        {
            "as_ru_rtpd": ("Reg Up", "Standalone"),
            "as_rd_rtpd": ("Reg Down", "Standalone"),
            "as_sr_rtpd": ("Spin", "Standalone"),
            "as_nr_rtpd": ("Non Spin", "Standalone"),
        },
        15,
        html,
    )
    as_hybrid = _long_as_awards(
        report_start,
        {
            "as_ru_hybrid_rtpd": ("Reg Up", "Hybrid"),
            "as_rd_hybrid_rtpd": ("Reg Down", "Hybrid"),
            "as_sr_hybrid_rtpd": ("Spin", "Hybrid"),
            "as_nr_hybrid_rtpd": ("Non Spin", "Hybrid"),
        },
        15,
        html,
    )
    return pd.concat([energy, as_standalone, as_hybrid], ignore_index=True).sort_values(
        ["Interval Start", "Product", "Type"],
    )


def build_storage_awards_ifm(
    html: str,
    report_start: pd.Timestamp,
) -> pd.DataFrame:
    e_s = _downsample_5min_to_60min(_parse_js_array(html, "tot_energy_ifm"))
    e_h = _downsample_5min_to_60min(_parse_js_array(html, "tot_energy_hybrid_ifm"))
    energy = _long_energy_awards(
        report_start,
        e_s,
        e_h,
        60,
        "Energy",
    )
    as_standalone = _long_as_awards(
        report_start,
        {
            "as_ru_ifm": ("Reg Up", "Standalone"),
            "as_rd_ifm": ("Reg Down", "Standalone"),
            "as_sr_ifm": ("Spin", "Standalone"),
            "as_nr_ifm": ("Non Spin", "Standalone"),
        },
        60,
        html,
    )
    as_hybrid = _long_as_awards(
        report_start,
        {
            "as_ru_hybrid_ifm": ("Reg Up", "Hybrid"),
            "as_rd_hybrid_ifm": ("Reg Down", "Hybrid"),
            "as_sr_hybrid_ifm": ("Spin", "Hybrid"),
            "as_nr_hybrid_ifm": ("Non Spin", "Hybrid"),
        },
        60,
        html,
    )
    return pd.concat([energy, as_standalone, as_hybrid], ignore_index=True).sort_values(
        ["Interval Start", "Product", "Type"],
    )


def build_storage_awards_rtd(
    html: str,
    report_start: pd.Timestamp,
) -> pd.DataFrame:
    return _long_energy_awards(
        report_start,
        _parse_js_array(html, "tot_energy_rtd"),
        _parse_js_array(html, "tot_energy_hybrid_rtd"),
        5,
        "Energy",
    ).sort_values(["Interval Start", "Type"])


def build_storage_energy_awards_ruc(
    html: str,
    report_start: pd.Timestamp,
) -> pd.DataFrame:
    return _long_energy_awards(
        report_start,
        _parse_js_array(html, "tot_energy_ruc"),
        _parse_js_array(html, "tot_energy_hybrid_ruc"),
        5,
        "Energy",
    ).sort_values(["Interval Start", "Type"])


def build_storage_soc_hourly(
    html: str,
    report_start: pd.Timestamp,
) -> pd.DataFrame:
    ifm = _parse_js_array(html, "tot_charge_ifm")
    ruc = _parse_js_array(html, "tot_charge_ruc")
    n = min(len(ifm), len(ruc))
    if n == 0:
        return pd.DataFrame(
            columns=[
                "Interval Start",
                "Interval End",
                "Schedule",
                "SOC",
            ],
        )
    starts, ends = _interval_index(report_start, n, 5)
    df_ifm = pd.DataFrame(
        {
            "Interval Start": starts,
            "Interval End": ends,
            "Schedule": "IFM",
            "SOC": ifm[:n],
        },
    )
    df_ruc = pd.DataFrame(
        {
            "Interval Start": starts,
            "Interval End": ends,
            "Schedule": "RUC",
            "SOC": ruc[:n],
        },
    )
    return pd.concat([df_ifm, df_ruc], ignore_index=True).sort_values(
        ["Interval Start", "Schedule"],
    )


def build_storage_soc_fmm(
    html: str,
    report_start: pd.Timestamp,
) -> pd.DataFrame:
    s = _parse_js_array(html, "tot_charge_rtpd")
    n = len(s)
    if n == 0:
        return pd.DataFrame(
            columns=[
                "Interval Start",
                "Interval End",
                "Type",
                "SOC",
            ],
        )
    starts, ends = _interval_index(report_start, n, 5)
    return pd.DataFrame(
        {
            "Interval Start": starts,
            "Interval End": ends,
            "Type": "Standalone",
            "SOC": s,
        },
    ).sort_values(["Interval Start", "Type"])


def build_storage_soc_rtd(
    html: str,
    report_start: pd.Timestamp,
) -> pd.DataFrame:
    s = _parse_js_array(html, "tot_charge_rtd")
    n = len(s)
    if n == 0:
        return pd.DataFrame(
            columns=[
                "Interval Start",
                "Interval End",
                "Type",
                "SOC",
            ],
        )
    starts, ends = _interval_index(report_start, n, 5)
    return pd.DataFrame(
        {
            "Interval Start": starts,
            "Interval End": ends,
            "Type": "Standalone",
            "SOC": s,
        },
    ).sort_values(["Interval Start", "Type"])


def build_storage_energy_bids_fmm(
    html: str,
    report_start: pd.Timestamp,
) -> pd.DataFrame:
    hybrid_pos = _hybrid_bid_var_names(BID_RTPD_RANGE_ORDER)
    hybrid_neg = _hybrid_bid_var_names(BID_RTPD_NEG_ORDER)
    parts: list[pd.DataFrame] = [
        _bid_stack_to_df(
            report_start,
            html,
            BID_RTPD_RANGE_ORDER,
            15,
            "Charge",
            "Standalone",
        ),
        _bid_stack_to_df(
            report_start,
            html,
            BID_RTPD_NEG_ORDER,
            15,
            "Discharge",
            "Standalone",
        ),
        _bid_stack_to_df(
            report_start,
            html,
            hybrid_pos,
            15,
            "Charge",
            "Hybrid",
        ),
        _bid_stack_to_df(
            report_start,
            html,
            hybrid_neg,
            15,
            "Discharge",
            "Hybrid",
        ),
    ]
    return pd.concat(parts, ignore_index=True).sort_values(
        ["Interval Start", "Bid Range", "Operation", "Type"],
    )


def build_storage_energy_bids_ifm(
    html: str,
    report_start: pd.Timestamp,
) -> pd.DataFrame:
    hybrid_pos = _hybrid_bid_var_names(BID_IFM_RANGE_ORDER)
    hybrid_neg = _hybrid_bid_var_names(BID_IFM_NEG_ORDER)
    parts: list[pd.DataFrame] = [
        _bid_stack_to_df(
            report_start,
            html,
            BID_IFM_RANGE_ORDER,
            60,
            "Charge",
            "Standalone",
        ),
        _bid_stack_to_df(
            report_start,
            html,
            BID_IFM_NEG_ORDER,
            60,
            "Discharge",
            "Standalone",
        ),
        _bid_stack_to_df(
            report_start,
            html,
            hybrid_pos,
            60,
            "Charge",
            "Hybrid",
        ),
        _bid_stack_to_df(
            report_start,
            html,
            hybrid_neg,
            60,
            "Discharge",
            "Hybrid",
        ),
    ]
    return pd.concat(parts, ignore_index=True).sort_values(
        ["Interval Start", "Bid Range", "Operation", "Type"],
    )
