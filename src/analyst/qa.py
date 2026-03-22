"""QA (Quality Assurance) layer for analyst reports.

Runs between report generation and sending. Three checks:
1. Completeness Gate  — all URGENT facts must appear in the report
2. Freshness Validator — detect stale dynamic data repeated across cycles
3. validate_report()  — orchestrator that returns QAResult

Design principles:
- NO LLM calls — only pattern matching and heuristics (runs in seconds)
- NEVER blocks sending — if QA crashes, report goes out with a warning
- Supplement is a separate text message (no DOCX regeneration)
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field

from loguru import logger


# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------

@dataclass
class QAResult:
    passed: bool
    completeness: dict  # from check_completeness
    freshness: dict     # from check_freshness
    supplement: str | None  # text to append if completeness failed
    warnings: list[str] = field(default_factory=list)


# ---------------------------------------------------------------------------
# 1. Completeness Gate
# ---------------------------------------------------------------------------

async def check_completeness(
    report_text: str,
    urgent_facts: list[dict],
    memory,                       # unused for now; reserved for future semantic search
) -> dict:
    """Check that all URGENT-classified facts from this cycle appear in the report.

    Returns:
        {"passed": bool, "missing": list[str], "supplement": str | None}
    """
    missing: list[str] = []
    covered: list[str] = []

    for fact in urgent_facts:
        fact_text = fact.get("memory", fact.get("data", fact.get("text", "")))
        if not fact_text:
            continue

        if _fact_in_report(fact_text, report_text):
            covered.append(fact_text)
        else:
            missing.append(fact_text)

    supplement: str | None = None
    if missing:
        lines = [
            "## Дополнение: пропущенные URGENT события",
            "Следующие события были классифицированы как URGENT, "
            "но не вошли в основной анализ:",
        ]
        for fact_text in missing:
            # Trim very long facts for readability
            short = fact_text[:500] + ("..." if len(fact_text) > 500 else "")
            lines.append(f"- {short}")
        supplement = "\n".join(lines)

    return {
        "passed": len(missing) == 0,
        "total_urgent": len(urgent_facts),
        "covered": len(covered),
        "missing": missing,
        "supplement": supplement,
    }


def _fact_in_report(fact_text: str, report_text: str) -> bool:
    """Check if a fact is covered in the report using keyword matching.

    A fact is considered covered when >= 60 % of its key terms
    (numbers, tickers, proper nouns) appear somewhere in the report.
    """
    key_terms = _extract_key_terms(fact_text)
    if not key_terms:
        # No identifiable key terms — assume covered to avoid false positives
        return True

    report_lower = report_text.lower()
    matches = sum(1 for t in key_terms if t.lower() in report_lower)
    return matches / len(key_terms) >= 0.6


def _extract_key_terms(text: str) -> list[str]:
    """Extract key terms from a fact: numbers, dollar amounts, tickers, names.

    Returns a list of short strings that should be findable in the report
    if the fact was covered.
    """
    terms: list[str] = []

    # 1. Dollar amounts  e.g. "$69,000", "$1.2B"
    for m in re.finditer(r"\$[\d,]+(?:\.\d+)?[BMKbmk]?", text):
        terms.append(m.group())

    # 2. Percentages  e.g. "30%", "+13.5%"
    for m in re.finditer(r"[+-]?\d+(?:\.\d+)?%", text):
        terms.append(m.group())

    # 3. Tickers  e.g. $BTC, $ETH (with dollar sign)
    for m in re.finditer(r"\$([A-Z]{2,10})\b", text):
        terms.append(m.group(1))

    # 4. Standalone all-caps tokens (likely tickers / acronyms): BTC, ETH, SOL
    for m in re.finditer(r"\b([A-Z]{2,10})\b", text):
        word = m.group(1)
        # Skip very common English words
        if word not in _NOISE_CAPS:
            terms.append(word)

    # 5. Large standalone numbers (prices, liquidation amounts, etc.)
    for m in re.finditer(r"\b(\d[\d,]{2,}(?:\.\d+)?)\b", text):
        # Keep raw number including commas — also try without commas
        raw = m.group(1)
        terms.append(raw)
        no_comma = raw.replace(",", "")
        if no_comma != raw:
            terms.append(no_comma)

    # 6. Capitalised multi-word names (e.g. "Bessent", "Kharg Island")
    for m in re.finditer(r"[A-Z][a-z]{2,}(?:\s+[A-Z][a-z]+)*", text):
        name = m.group()
        if name not in _NOISE_NAMES:
            terms.append(name)

    # Deduplicate while preserving order
    seen: set[str] = set()
    unique: list[str] = []
    for t in terms:
        if t not in seen:
            seen.add(t)
            unique.append(t)

    return unique


_NOISE_CAPS = frozenset({
    "THE", "AND", "FOR", "BUT", "NOT", "ALL", "ARE", "WAS", "HAS",
    "HAD", "HIS", "HER", "ITS", "OUR", "NEW", "OLD", "NOW", "HOW",
    "WHO", "WHY", "UTC", "USD", "BREAKING", "URGENT", "VIA", "THIS",
    "THAT", "WITH", "FROM", "JUST", "ALSO", "INTO", "OVER", "WILL",
})

_NOISE_NAMES = frozenset({
    "The", "This", "That", "These", "Those", "Here", "There",
    "January", "February", "March", "April", "May", "June",
    "July", "August", "September", "October", "November", "December",
    "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday",
})


# ---------------------------------------------------------------------------
# 2. Freshness Validator
# ---------------------------------------------------------------------------

async def check_freshness(
    report_text: str,
    previous_reports: list[str],
) -> dict:
    """Detect stale dynamic data that hasn't been updated across cycles.

    Compares numeric claims (prices, percentages, probabilities) between
    the current report and the most recent previous report.  If the same
    value appears in 2+ consecutive reports it is flagged as potentially stale.

    Returns:
        {"passed": bool, "stale_items": list[dict], "warnings": list[str]}
    """
    if not previous_reports:
        return {"passed": True, "stale_items": [], "warnings": []}

    current_claims = _extract_numeric_claims(report_text)
    prev_claims = _extract_numeric_claims(previous_reports[-1])

    stale_items: list[dict] = []
    for label, value in current_claims.items():
        if label in prev_claims and prev_claims[label] == value:
            stale_items.append({
                "label": label,
                "value": value,
                "note": "Identical in current and previous report — may be stale",
            })

    warnings: list[str] = []
    for item in stale_items:
        warnings.append(
            f"Stale data: \"{item['label']}\" = {item['value']} "
            f"(unchanged from previous report)"
        )

    return {
        "passed": len(stale_items) == 0,
        "stale_items": stale_items,
        "warnings": warnings,
    }


def _extract_numeric_claims(text: str) -> dict[str, str]:
    """Extract numeric claims from report text.

    Returns a dict mapping a normalised label to its value string, e.g.:
        {"polymarket ... ": "57%", "btc ...": "$69,000"}
    """
    claims: dict[str, str] = {}

    patterns: list[tuple[str, re.Pattern[str]]] = [
        # entity $price  (e.g.  "BTC $87,500",  "ETH — $3,200")
        ("price", re.compile(
            r"([A-Za-z][A-Za-z0-9 ]{0,30}?)\s*[-—:~]\s*\$\s*([\d,]+(?:\.\d+)?)"
        )),
        # $price entity  (e.g.  "$87,500 BTC")
        ("price_rev", re.compile(
            r"\$\s*([\d,]+(?:\.\d+)?)\s+([\w]+)"
        )),
        # entity: percent  (e.g.  "safe-haven 30%",  "Polymarket: 57%")
        ("percent", re.compile(
            r"([A-Za-z][A-Za-z0-9 \-]{0,30}?)\s*[-—:~]\s*([+-]?\d+(?:\.\d+)?)\s*%"
        )),
        # Polymarket-specific  (e.g.  "Polymarket ... 57%")
        ("polymarket", re.compile(
            r"(Polymarket[^.]{0,60}?)\s+(\d+(?:\.\d+)?)\s*%",
            re.IGNORECASE,
        )),
        # Fear & Greed index  (e.g.  "Fear & Greed Index: 72")
        ("fear_greed", re.compile(
            r"((?:Fear|Greed)[^.]{0,30}?)\s*[-—:]\s*(\d+)",
            re.IGNORECASE,
        )),
        # Dominance  (e.g.  "BTC dominance 54.2%")
        ("dominance", re.compile(
            r"((?:BTC|Bitcoin)\s+dominance[^.]{0,20}?)\s*[-—:~]?\s*(\d+(?:\.\d+)?)\s*%",
            re.IGNORECASE,
        )),
        # Funding rate  (e.g.  "funding rate -0.01%")
        ("funding", re.compile(
            r"(funding\s+rate[^.]{0,20}?)\s*[-—:~]?\s*([+-]?\d+(?:\.\d+)?)\s*%?",
            re.IGNORECASE,
        )),
    ]

    for name, pattern in patterns:
        for m in pattern.finditer(text):
            if name == "price_rev":
                label = m.group(2).strip().lower()
                value = "$" + m.group(1).strip()
            else:
                label = m.group(1).strip().lower()
                value = m.group(2).strip()
                if name in ("percent", "polymarket", "dominance", "funding"):
                    value = value + "%"
                elif name == "price":
                    value = "$" + value

            # Normalise whitespace in label
            label = re.sub(r"\s+", " ", label)

            # Skip very short / generic labels
            if len(label) < 3:
                continue

            # Use first occurrence only (avoids overwriting with same-page duplicates)
            if label not in claims:
                claims[label] = value

    return claims


# ---------------------------------------------------------------------------
# 3. Orchestrator
# ---------------------------------------------------------------------------

async def validate_report(
    report_text: str,
    cycle_id: str,
    memory,
    previous_report: str | None = None,
    urgent_facts: list[dict] | None = None,
) -> QAResult:
    """Run all QA checks on a report.

    Parameters
    ----------
    report_text : str
        The generated report markdown.
    cycle_id : str
        E.g. "20260320_0600".
    memory
        Memory instance (passed to completeness check).
    previous_report : str | None
        Plain text of the immediately preceding report (for freshness).
    urgent_facts : list[dict] | None
        URGENT facts for this cycle window (for completeness).

    Returns
    -------
    QAResult with overall verdict and details from each check.
    """
    all_warnings: list[str] = []

    # --- Completeness ---
    if urgent_facts is None:
        urgent_facts = []

    try:
        completeness = await check_completeness(report_text, urgent_facts, memory)
    except Exception as e:
        logger.error(f"QA completeness check failed: {e}")
        completeness = {"passed": True, "missing": [], "supplement": None,
                        "total_urgent": 0, "covered": 0}
        all_warnings.append(f"Completeness check error: {e}")

    # --- Freshness ---
    previous_reports = [previous_report] if previous_report else []
    try:
        freshness = await check_freshness(report_text, previous_reports)
    except Exception as e:
        logger.error(f"QA freshness check failed: {e}")
        freshness = {"passed": True, "stale_items": [], "warnings": []}
        all_warnings.append(f"Freshness check error: {e}")

    all_warnings.extend(freshness.get("warnings", []))

    # --- Missing URGENT warnings ---
    missing = completeness.get("missing", [])
    if missing:
        all_warnings.append(
            f"Completeness: {len(missing)} URGENT fact(s) missing from report"
        )

    # --- Overall verdict ---
    passed = completeness.get("passed", True) and freshness.get("passed", True)
    supplement = completeness.get("supplement")

    logger.info(
        f"QA │ completeness={'PASS' if completeness.get('passed') else 'FAIL'} "
        f"({completeness.get('covered', 0)}/{completeness.get('total_urgent', 0)} URGENT covered), "
        f"freshness={'PASS' if freshness.get('passed') else 'FAIL'} "
        f"({len(freshness.get('stale_items', []))} stale)"
    )

    return QAResult(
        passed=passed,
        completeness=completeness,
        freshness=freshness,
        supplement=supplement,
        warnings=all_warnings,
    )
