"""
tally_sheets_mcp — MCP server exposing Tally-on-Google-Sheets data to Claude.

Design principle (critical for large data):
    The Google Sheet has 50k+ rows. Claude's context cannot hold them all.
    So every tool here reads rows from the Apps Script web app *server-side*,
    aggregates/summarizes, and returns only small results to Claude.
    No tool ever dumps the full raw sheet into the model.

Data source:
    A Google Apps Script web app (see apps_script_Code.gs) that returns JSON.
    The server pages through its `action=data` endpoint and computes summaries.

Environment variables (set these on Railway / Render):
    TALLY_WEBAPP_URL   -> your Apps Script /exec URL
    TALLY_WEBAPP_KEY   -> the SECRET you set in the Apps Script
    PORT               -> provided automatically by the host (default 8000)

Run locally (stdio):      python server.py
Run hosted (HTTP):        TRANSPORT=http python server.py
"""

import os
import json
import asyncio
from enum import Enum
from collections import defaultdict
from datetime import datetime, date
from typing import Optional, List, Dict, Any

import httpx
from pydantic import BaseModel, Field, ConfigDict
from mcp.server.fastmcp import FastMCP

# --------------------------------------------------------------------------- #
# Configuration
# --------------------------------------------------------------------------- #

WEBAPP_URL = os.environ.get("TALLY_WEBAPP_URL", "").strip()
WEBAPP_KEY = os.environ.get("TALLY_WEBAPP_KEY", "").strip()

PAGE_SIZE = 2000          # rows fetched per request to Apps Script
MAX_ROWS_SCAN = 200_000   # safety ceiling on how many rows we will scan
HTTP_TIMEOUT = 60.0       # seconds

mcp = FastMCP("tally_sheets_mcp")


# --------------------------------------------------------------------------- #
# Shared helpers
# --------------------------------------------------------------------------- #

def _config_error() -> Optional[str]:
    """Return an actionable error string if the server is misconfigured."""
    if not WEBAPP_URL:
        return ("Error: TALLY_WEBAPP_URL is not set. Set it to your Apps Script "
                "web app /exec URL in the host's environment variables.")
    if not WEBAPP_KEY:
        return ("Error: TALLY_WEBAPP_KEY is not set. Set it to the SECRET value "
                "from your Apps Script in the host's environment variables.")
    return None


async def _call_webapp(params: Dict[str, Any]) -> Any:
    """Call the Apps Script web app with the secret key and return parsed JSON."""
    q = dict(params)
    q["key"] = WEBAPP_KEY
    async with httpx.AsyncClient(timeout=HTTP_TIMEOUT, follow_redirects=True) as client:
        resp = await client.get(WEBAPP_URL, params=q)
        resp.raise_for_status()
        text = resp.text.strip()
        try:
            return json.loads(text)
        except json.JSONDecodeError:
            raise ValueError(
                "Apps Script did not return JSON. Check the URL is the /exec "
                "deployment and the key is correct. First 200 chars: "
                + text[:200]
            )


def _handle_error(e: Exception) -> str:
    """Consistent, actionable error formatting."""
    if isinstance(e, httpx.HTTPStatusError):
        code = e.response.status_code
        if code == 401 or code == 403:
            return ("Error: Unauthorized. The TALLY_WEBAPP_KEY likely does not "
                    "match the SECRET in your Apps Script.")
        if code == 404:
            return "Error: Web app URL not found (404). Check TALLY_WEBAPP_URL."
        return f"Error: Web app request failed with HTTP {code}."
    if isinstance(e, httpx.TimeoutException):
        return "Error: Request to the web app timed out. The sheet may be large; try again."
    return f"Error: {type(e).__name__}: {e}"


def _find_col(headers: List[str], *candidates: str) -> Optional[int]:
    """Find a column index by trying candidate names (case-insensitive, contains)."""
    low = [str(h).strip().lower() for h in headers]
    # exact match first
    for cand in candidates:
        c = cand.lower()
        if c in low:
            return low.index(c)
    # then 'contains'
    for cand in candidates:
        c = cand.lower()
        for i, h in enumerate(low):
            if c in h:
                return i
    return None


def _to_number(val: Any) -> float:
    """Best-effort parse of a cell into a float (handles commas, blanks, ₹)."""
    if val is None or val == "":
        return 0.0
    if isinstance(val, (int, float)):
        return float(val)
    s = str(val).replace(",", "").replace("\u20b9", "").replace("Rs.", "").strip()
    try:
        return float(s)
    except ValueError:
        return 0.0


def _month_key(val: Any) -> Optional[str]:
    """Extract a 'YYYY-MM' month key from a date-like cell, if possible."""
    if isinstance(val, (datetime, date)):
        return val.strftime("%Y-%m")
    s = str(val).strip()
    for fmt in ("%Y-%m-%d", "%d-%m-%Y", "%d/%m/%Y", "%Y/%m/%d",
                "%d-%b-%Y", "%d-%b-%y", "%m/%d/%Y"):
        try:
            return datetime.strptime(s[:19].split("T")[0], fmt).strftime("%Y-%m")
        except ValueError:
            continue
    # ISO-ish fallback
    if len(s) >= 7 and s[4] == "-":
        return s[:7]
    return None


async def _iter_rows(tab: str, max_rows: int = MAX_ROWS_SCAN):
    """Yield (headers, rows_batch) pages from the web app until exhausted."""
    offset = 0
    headers: List[str] = []
    while True:
        page = await _call_webapp(
            {"action": "data", "tab": tab, "offset": offset, "limit": PAGE_SIZE}
        )
        if isinstance(page, dict) and page.get("error"):
            raise ValueError(f"Web app error for tab '{tab}': {page['error']}")
        headers = page.get("headers", headers)
        rows = page.get("rows", [])
        if not rows:
            break
        yield headers, rows
        offset += len(rows)
        if not page.get("has_more") or offset >= max_rows:
            break


# --------------------------------------------------------------------------- #
# Input models
# --------------------------------------------------------------------------- #

class ResponseFormat(str, Enum):
    MARKDOWN = "markdown"
    JSON = "json"


class EmptyInput(BaseModel):
    model_config = ConfigDict(extra="forbid")


class TabInput(BaseModel):
    model_config = ConfigDict(str_strip_whitespace=True, extra="forbid")
    tab: str = Field(..., description="Exact tab/sheet name, e.g. 'Sales'", min_length=1)


class RangeInput(BaseModel):
    model_config = ConfigDict(str_strip_whitespace=True, extra="forbid")
    tab: str = Field(..., description="Exact tab/sheet name", min_length=1)
    offset: int = Field(default=0, description="Rows to skip (0-based)", ge=0)
    limit: int = Field(default=50, description="Rows to return (max 200)", ge=1, le=200)


class GroupSumInput(BaseModel):
    model_config = ConfigDict(str_strip_whitespace=True, extra="forbid")
    tab: str = Field(..., description="Exact tab/sheet name", min_length=1)
    group_by: str = Field(..., description="Column to group by, e.g. 'Party' or 'Item'", min_length=1)
    sum_column: str = Field(..., description="Numeric column to total, e.g. 'Amount'", min_length=1)
    limit: int = Field(default=20, description="Top N groups to return", ge=1, le=100)


class MonthlyInput(BaseModel):
    model_config = ConfigDict(str_strip_whitespace=True, extra="forbid")
    tab: str = Field(..., description="Exact tab/sheet name", min_length=1)
    date_column: str = Field(..., description="Date column name, e.g. 'Date'", min_length=1)
    sum_column: str = Field(..., description="Numeric column to total, e.g. 'Amount'", min_length=1)


# --------------------------------------------------------------------------- #
# Tools
# --------------------------------------------------------------------------- #

@mcp.tool(
    name="tally_list_tabs",
    annotations={"title": "List sheet tabs", "readOnlyHint": True,
                 "destructiveHint": False, "idempotentHint": True, "openWorldHint": True},
)
async def tally_list_tabs(params: EmptyInput) -> str:
    """List all tab (sheet) names in the connected Tally Google Sheet.

    Returns:
        str: JSON array of tab names, e.g. ["Sales","Ledger","Stock"].
    """
    err = _config_error()
    if err:
        return err
    try:
        tabs = await _call_webapp({"action": "tabs"})
        return json.dumps({"tabs": tabs}, indent=2)
    except Exception as e:  # noqa: BLE001
        return _handle_error(e)


@mcp.tool(
    name="tally_row_counts",
    annotations={"title": "Row counts per tab", "readOnlyHint": True,
                 "destructiveHint": False, "idempotentHint": True, "openWorldHint": True},
)
async def tally_row_counts(params: EmptyInput) -> str:
    """Return the number of data rows (excluding header) in each tab.

    Use this first to understand the size of each tab before analysing.

    Returns:
        str: JSON object mapping tab name -> row count.
    """
    err = _config_error()
    if err:
        return err
    try:
        counts = await _call_webapp({"action": "rowcount"})
        return json.dumps({"row_counts": counts}, indent=2)
    except Exception as e:  # noqa: BLE001
        return _handle_error(e)


@mcp.tool(
    name="tally_get_headers",
    annotations={"title": "Get column headers", "readOnlyHint": True,
                 "destructiveHint": False, "idempotentHint": True, "openWorldHint": True},
)
async def tally_get_headers(params: TabInput) -> str:
    """Return the column header names for a given tab.

    Call this to discover which columns exist before grouping or summing.

    Args:
        params.tab (str): Exact tab name.

    Returns:
        str: JSON array of header strings.
    """
    err = _config_error()
    if err:
        return err
    try:
        headers = await _call_webapp({"action": "headers", "tab": params.tab})
        return json.dumps({"tab": params.tab, "headers": headers}, indent=2)
    except Exception as e:  # noqa: BLE001
        return _handle_error(e)


@mcp.tool(
    name="tally_preview_rows",
    annotations={"title": "Preview a few rows", "readOnlyHint": True,
                 "destructiveHint": False, "idempotentHint": True, "openWorldHint": True},
)
async def tally_preview_rows(params: RangeInput) -> str:
    """Return a small slice of raw rows from a tab (max 200) for inspection.

    Use this to eyeball sample data, NOT to read the whole sheet. For totals
    and analysis use the aggregation tools instead.

    Args:
        params.tab (str): Exact tab name.
        params.offset (int): Rows to skip.
        params.limit (int): Rows to return (<=200).

    Returns:
        str: JSON with headers and the requested rows.
    """
    err = _config_error()
    if err:
        return err
    try:
        page = await _call_webapp(
            {"action": "data", "tab": params.tab,
             "offset": params.offset, "limit": params.limit}
        )
        return json.dumps(page, indent=2, default=str)
    except Exception as e:  # noqa: BLE001
        return _handle_error(e)


@mcp.tool(
    name="tally_sum_by_group",
    annotations={"title": "Total a column grouped by another", "readOnlyHint": True,
                 "destructiveHint": False, "idempotentHint": True, "openWorldHint": True},
)
async def tally_sum_by_group(params: GroupSumInput) -> str:
    """Sum a numeric column grouped by another column, across ALL rows.

    Reads every row server-side and returns only the top groups, so it is
    safe on very large tabs. Example: total 'Amount' by 'Party' to find the
    biggest customers/suppliers.

    Args:
        params.tab (str): Exact tab name.
        params.group_by (str): Column to group by (e.g. 'Party').
        params.sum_column (str): Numeric column to total (e.g. 'Amount').
        params.limit (int): How many top groups to return.

    Returns:
        str: JSON with grand_total, rows_scanned, and top groups
             [{"group": name, "total": number, "count": n}, ...].
    """
    err = _config_error()
    if err:
        return err
    try:
        totals: Dict[str, float] = defaultdict(float)
        counts: Dict[str, int] = defaultdict(int)
        grand_total = 0.0
        rows_scanned = 0
        gi = si = None

        async for headers, rows in _iter_rows(params.tab):
            if gi is None:
                gi = _find_col(headers, params.group_by)
                si = _find_col(headers, params.sum_column)
                if gi is None:
                    return (f"Error: group_by column '{params.group_by}' not found. "
                            f"Available: {headers}")
                if si is None:
                    return (f"Error: sum_column '{params.sum_column}' not found. "
                            f"Available: {headers}")
            for r in rows:
                if gi >= len(r):
                    continue
                key = str(r[gi]).strip() or "(blank)"
                val = _to_number(r[si]) if si < len(r) else 0.0
                totals[key] += val
                counts[key] += 1
                grand_total += val
                rows_scanned += 1

        top = sorted(totals.items(), key=lambda kv: kv[1], reverse=True)[: params.limit]
        result = {
            "tab": params.tab,
            "group_by": params.group_by,
            "sum_column": params.sum_column,
            "rows_scanned": rows_scanned,
            "distinct_groups": len(totals),
            "grand_total": round(grand_total, 2),
            "top_groups": [
                {"group": k, "total": round(v, 2), "count": counts[k]} for k, v in top
            ],
        }
        return json.dumps(result, indent=2)
    except Exception as e:  # noqa: BLE001
        return _handle_error(e)


@mcp.tool(
    name="tally_monthly_summary",
    annotations={"title": "Monthly totals", "readOnlyHint": True,
                 "destructiveHint": False, "idempotentHint": True, "openWorldHint": True},
)
async def tally_monthly_summary(params: MonthlyInput) -> str:
    """Total a numeric column by month, across ALL rows.

    Reads every row server-side, groups by YYYY-MM from the date column, and
    returns one figure per month — safe on very large tabs. Good for sales or
    purchase trends.

    Args:
        params.tab (str): Exact tab name.
        params.date_column (str): Date column (e.g. 'Date').
        params.sum_column (str): Numeric column to total (e.g. 'Amount').

    Returns:
        str: JSON with grand_total and per-month totals sorted chronologically.
    """
    err = _config_error()
    if err:
        return err
    try:
        totals: Dict[str, float] = defaultdict(float)
        counts: Dict[str, int] = defaultdict(int)
        grand_total = 0.0
        rows_scanned = 0
        unparsed = 0
        di = si = None

        async for headers, rows in _iter_rows(params.tab):
            if di is None:
                di = _find_col(headers, params.date_column)
                si = _find_col(headers, params.sum_column)
                if di is None:
                    return (f"Error: date_column '{params.date_column}' not found. "
                            f"Available: {headers}")
                if si is None:
                    return (f"Error: sum_column '{params.sum_column}' not found. "
                            f"Available: {headers}")
            for r in rows:
                rows_scanned += 1
                mk = _month_key(r[di]) if di < len(r) else None
                if mk is None:
                    unparsed += 1
                    continue
                val = _to_number(r[si]) if si < len(r) else 0.0
                totals[mk] += val
                counts[mk] += 1
                grand_total += val

        months = sorted(totals.keys())
        result = {
            "tab": params.tab,
            "date_column": params.date_column,
            "sum_column": params.sum_column,
            "rows_scanned": rows_scanned,
            "rows_with_unparsable_date": unparsed,
            "grand_total": round(grand_total, 2),
            "monthly": [
                {"month": m, "total": round(totals[m], 2), "count": counts[m]}
                for m in months
            ],
        }
        return json.dumps(result, indent=2)
    except Exception as e:  # noqa: BLE001
        return _handle_error(e)


# --------------------------------------------------------------------------- #
# Entry point
# --------------------------------------------------------------------------- #

if __name__ == "__main__":
    transport = os.environ.get("TRANSPORT", "stdio").lower()
    if transport in ("http", "streamable_http", "streamable-http"):
        port = int(os.environ.get("PORT", "8000"))
        mcp.settings.host = "0.0.0.0"
        mcp.settings.port = port
        mcp.run(transport="streamable-http")
    else:
        mcp.run()
