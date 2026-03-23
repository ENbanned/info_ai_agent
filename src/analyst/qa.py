from __future__ import annotations

import re
from dataclasses import dataclass, field

from loguru import logger


@dataclass
class QAResult:
    passed: bool
    completeness: dict
    freshness: dict
    supplement: str | None
    warnings: list[str] = field(default_factory=list)


async def check_completeness(
    report_text: str,
    urgent_facts: list[dict],
    memory,
) -> dict:
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
    key_terms = _extract_key_terms(fact_text)
    if not key_terms:
        return True

    report_lower = report_text.lower()
    matches = sum(1 for t in key_terms if t.lower() in report_lower)
    return matches / len(key_terms) >= 0.6


def _extract_key_terms(text: str) -> list[str]:
    terms: list[str] = []

    for m in re.finditer(r"\$[\d,]+(?:\.\d+)?[BMKbmk]?", text):
        terms.append(m.group())

    for m in re.finditer(r"[+-]?\d+(?:\.\d+)?%", text):
        terms.append(m.group())

    for m in re.finditer(r"\$([A-Z]{2,10})\b", text):
        terms.append(m.group(1))

    for m in re.finditer(r"\b([A-Z]{2,10})\b", text):
        word = m.group(1)
        if word not in _NOISE_CAPS:
            terms.append(word)

    for m in re.finditer(r"\b(\d[\d,]{2,}(?:\.\d+)?)\b", text):
        raw = m.group(1)
        terms.append(raw)
        no_comma = raw.replace(",", "")
        if no_comma != raw:
            terms.append(no_comma)

    for m in re.finditer(r"[A-Z][a-z]{2,}(?:\s+[A-Z][a-z]+)*", text):
        name = m.group()
        if name not in _NOISE_NAMES:
            terms.append(name)

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


async def check_freshness(
    report_text: str,
    previous_reports: list[str],
) -> dict:
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
    claims: dict[str, str] = {}

    patterns: list[tuple[str, re.Pattern[str]]] = [
        ("price", re.compile(
            r"([A-Za-z][A-Za-z0-9 ]{0,30}?)\s*[-—:~]\s*\$\s*([\d,]+(?:\.\d+)?)"
        )),
        ("price_rev", re.compile(
            r"\$\s*([\d,]+(?:\.\d+)?)\s+([\w]+)"
        )),
        ("percent", re.compile(
            r"([A-Za-z][A-Za-z0-9 \-]{0,30}?)\s*[-—:~]\s*([+-]?\d+(?:\.\d+)?)\s*%"
        )),
        ("polymarket", re.compile(
            r"(Polymarket[^.]{0,60}?)\s+(\d+(?:\.\d+)?)\s*%",
            re.IGNORECASE,
        )),
        ("fear_greed", re.compile(
            r"((?:Fear|Greed)[^.]{0,30}?)\s*[-—:]\s*(\d+)",
            re.IGNORECASE,
        )),
        ("dominance", re.compile(
            r"((?:BTC|Bitcoin)\s+dominance[^.]{0,20}?)\s*[-—:~]?\s*(\d+(?:\.\d+)?)\s*%",
            re.IGNORECASE,
        )),
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

            label = re.sub(r"\s+", " ", label)

            if len(label) < 3:
                continue

            if label not in claims:
                claims[label] = value

    return claims


async def validate_report(
    report_text: str,
    cycle_id: str,
    memory,
    previous_report: str | None = None,
    urgent_facts: list[dict] | None = None,
) -> QAResult:
    all_warnings: list[str] = []

    if urgent_facts is None:
        urgent_facts = []

    try:
        completeness = await check_completeness(report_text, urgent_facts, memory)
    except Exception as e:
        logger.error(f"QA completeness check failed: {e}")
        completeness = {"passed": True, "missing": [], "supplement": None,
                        "total_urgent": 0, "covered": 0}
        all_warnings.append(f"Completeness check error: {e}")

    previous_reports = [previous_report] if previous_report else []
    try:
        freshness = await check_freshness(report_text, previous_reports)
    except Exception as e:
        logger.error(f"QA freshness check failed: {e}")
        freshness = {"passed": True, "stale_items": [], "warnings": []}
        all_warnings.append(f"Freshness check error: {e}")

    all_warnings.extend(freshness.get("warnings", []))

    missing = completeness.get("missing", [])
    if missing:
        all_warnings.append(
            f"Completeness: {len(missing)} URGENT fact(s) missing from report"
        )

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
