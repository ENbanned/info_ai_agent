"""Convert Markdown text (from Opus responses) to beautiful Telegram HTML.

Telegram Bot API HTML mode supports:
  <b>, <i>, <u>, <s>, <code>, <pre>, <a>, <blockquote>, <blockquote expandable>,
  <tg-spoiler>, <tg-emoji>, <tg-time>

This module converts Opus Markdown output into visually polished Telegram messages.
"""

import re
from html import escape as _html_escape


# ---------------------------------------------------------------------------
# Section emoji mapping — auto-decorates headings
# ---------------------------------------------------------------------------

_SECTION_EMOJI = {
    # Russian keywords
    "вывод": "💡", "выводы": "💡", "итог": "💡", "итоги": "💡",
    "заключение": "💡", "резюме": "💡",
    "риск": "⚠️", "риски": "⚠️", "предупреждение": "⚠️",
    "анализ": "🔍", "разбор": "🔍", "обзор": "🔍",
    "рекомен": "🎯", "стратеги": "🎯", "тактик": "🎯", "действи": "🎯",
    "прогноз": "📊", "данные": "📊", "метрик": "📊", "статистик": "📊",
    "срочно": "⚡", "urgent": "⚡", "breaking": "⚡",
    "тезис": "📌", "thesis": "📌",
    "сценарий": "📋", "сценарии": "📋",
    "источник": "🔗", "ссылк": "🔗",
    "главн": "🏆", "ключев": "🏆",
    # English keywords
    "summary": "💡", "conclusion": "💡", "takeaway": "💡",
    "risk": "⚠️", "warning": "⚠️", "caution": "⚠️",
    "analysis": "🔍", "overview": "🔍", "review": "🔍",
    "recommendation": "🎯", "strategy": "🎯", "action": "🎯",
    "forecast": "📊", "data": "📊", "metrics": "📊",
    "scenario": "📋",
}


def _pick_section_emoji(heading_text: str) -> str:
    """Pick a contextual emoji for a section heading."""
    lower = heading_text.lower()
    for keyword, emoji in _SECTION_EMOJI.items():
        if keyword in lower:
            return emoji
    return "▸"


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def markdown_to_telegram_html(text: str) -> str:
    """Convert Markdown produced by Opus into beautiful Telegram HTML."""
    text = text.replace("\r\n", "\n")

    # Step 1: Stash fenced code blocks
    code_blocks: list[str] = []
    def _stash_code(m: re.Match) -> str:
        lang = m.group(1) or ""
        code = _html_escape(m.group(2).strip("\n"))
        if lang:
            block = f'<pre><code class="language-{_html_escape(lang)}">{code}</code></pre>'
        else:
            block = f"<pre>{code}</pre>"
        code_blocks.append(block)
        return f"\x00CB{len(code_blocks)-1}\x00"

    text = re.sub(r"```(\w*)\n([\s\S]*?)```", _stash_code, text)

    # Step 2: Stash inline code
    inline_codes: list[str] = []
    def _stash_inline(m: re.Match) -> str:
        inline_codes.append(f"<code>{_html_escape(m.group(1))}</code>")
        return f"\x00IC{len(inline_codes)-1}\x00"

    text = re.sub(r"`([^`\n]+)`", _stash_inline, text)

    # Step 3: Escape HTML in remaining text
    text = _html_escape(text)

    # Step 4: Tables → beautiful monospace blocks
    text = _convert_tables(text)

    # Step 5: Headings → bold with emoji decoration
    def _heading_replace(m: re.Match) -> str:
        level = len(m.group(1))
        content = m.group(2).strip()
        # Remove any existing emoji at start to avoid doubling
        clean = re.sub(r"^[\U0001F300-\U0001FAFF\U00002702-\U000027B0\U0000FE00-\U0000FE0F\u200d]+\s*", "", content)
        emoji = _pick_section_emoji(clean)
        if level == 1:
            return f"\n{emoji} <b>{clean}</b>\n{'━' * 20}"
        else:
            return f"\n{emoji} <b>{clean}</b>"

    text = re.sub(r"^(#{1,6})\s+(.+)$", _heading_replace, text, flags=re.MULTILINE)

    # Step 6: Bold
    text = re.sub(r"\*\*(.+?)\*\*", r"<b>\1</b>", text)
    text = re.sub(r"__(.+?)__", r"<b>\1</b>", text)

    # Step 7: Italic
    text = re.sub(r"(?<!\w)\*([^*\n]+?)\*(?!\w)", r"<i>\1</i>", text)
    text = re.sub(r"(?<!\w)_([^_\n]+?)_(?!\w)", r"<i>\1</i>", text)

    # Step 8: Strikethrough
    text = re.sub(r"~~(.+?)~~", r"<s>\1</s>", text)

    # Step 9: Links
    text = re.sub(r"\[([^\]]+)\]\(([^)]+)\)", r'<a href="\2">\1</a>', text)

    # Step 10: Blockquotes → Telegram blockquote (beautiful)
    text = _convert_blockquotes(text)

    # Step 11: Horizontal rules → clean separator
    text = re.sub(r"^[-*_]{3,}\s*$", "━━━━━━━━━━━━━━━━━━━━━", text, flags=re.MULTILINE)

    # Step 12: Bullet lists with better bullets
    text = re.sub(r"^(\s*)[-*]\s+", lambda m: m.group(1) + "▸ ", text, flags=re.MULTILINE)

    # Step 13: Collapse excessive blank lines
    text = re.sub(r"\n{4,}", "\n\n\n", text)

    # Step 14: Restore stashed code
    for i, block in enumerate(code_blocks):
        text = text.replace(f"\x00CB{i}\x00", block)
    for i, code in enumerate(inline_codes):
        text = text.replace(f"\x00IC{i}\x00", code)

    return text.strip()


# ---------------------------------------------------------------------------
# Table conversion → aligned monospace
# ---------------------------------------------------------------------------

def _convert_tables(text: str) -> str:
    """Convert Markdown tables to aligned <pre> blocks."""
    lines = text.split("\n")
    result: list[str] = []
    table_lines: list[str] = []
    in_table = False

    for line in lines:
        stripped = line.strip()
        if re.match(r"^\|.*\|$", stripped):
            if not in_table:
                in_table = True
                table_lines = []
            table_lines.append(stripped)
        else:
            if in_table:
                result.append(_render_table(table_lines))
                table_lines = []
                in_table = False
            result.append(line)

    if in_table:
        result.append(_render_table(table_lines))

    return "\n".join(result)


def _render_table(lines: list[str]) -> str:
    """Render table as a clean monospace block."""
    if not lines:
        return ""

    rows: list[list[str]] = []
    for line in lines:
        cells = [c.strip() for c in line.strip("|").split("|")]
        if all(re.match(r"^[-:]+$", c.strip()) for c in cells if c.strip()):
            continue  # Skip separator rows
        rows.append(cells)

    if not rows:
        return "\n".join(lines)

    num_cols = max(len(r) for r in rows)
    col_widths = [0] * num_cols
    for row in rows:
        for i, cell in enumerate(row):
            if i < num_cols:
                col_widths[i] = max(col_widths[i], len(cell))

    formatted: list[str] = []
    for idx, row in enumerate(rows):
        parts = []
        for i in range(num_cols):
            cell = row[i] if i < len(row) else ""
            parts.append(cell.ljust(col_widths[i]))
        formatted.append("  ".join(parts))
        if idx == 0 and len(rows) > 1:
            formatted.append("  ".join("─" * w for w in col_widths))

    return "<pre>" + "\n".join(formatted) + "</pre>"


# ---------------------------------------------------------------------------
# Blockquote conversion
# ---------------------------------------------------------------------------

def _convert_blockquotes(text: str) -> str:
    """Convert > lines to Telegram <blockquote> blocks."""
    lines = text.split("\n")
    result: list[str] = []
    bq_lines: list[str] = []
    in_bq = False

    for line in lines:
        if re.match(r"^&gt;\s?", line):
            in_bq = True
            bq_lines.append(re.sub(r"^&gt;\s?", "", line))
        else:
            if in_bq:
                content = "\n".join(bq_lines)
                if len(content) > 300:
                    result.append(f"<blockquote expandable>{content}</blockquote>")
                else:
                    result.append(f"<blockquote>{content}</blockquote>")
                bq_lines = []
                in_bq = False
            result.append(line)

    if in_bq:
        content = "\n".join(bq_lines)
        tag = "blockquote expandable" if len(content) > 300 else "blockquote"
        result.append(f"<{tag}>{content}</blockquote>")

    return "\n".join(result)


# ---------------------------------------------------------------------------
# HTML-aware message splitting
# ---------------------------------------------------------------------------

_PAIRED_TAGS = {"b", "i", "u", "s", "code", "pre", "a", "blockquote", "tg-spoiler"}


def split_html_message(text: str, max_len: int = 4000) -> list[str]:
    """Split HTML into chunks, closing/reopening tags across boundaries."""
    if len(text) <= max_len:
        return [text]

    chunks: list[str] = []
    while text:
        if len(text) <= max_len:
            chunks.append(text)
            break

        split_at = text.rfind("\n\n", 0, max_len)
        if split_at < max_len // 4:
            split_at = text.rfind("\n", 0, max_len)
        if split_at < max_len // 4:
            split_at = max_len

        chunk = text[:split_at]
        remainder = text[split_at:].lstrip("\n")

        chunk, reopen = _close_open_tags(chunk)
        chunks.append(chunk)
        text = reopen + remainder

    return chunks


def _close_open_tags(html: str) -> tuple[str, str]:
    """Close unclosed tags and return reopening string for next chunk."""
    tag_stack: list[str] = []
    open_tag_texts: list[str] = []

    for m in re.finditer(r"<(/?)(\w[\w-]*)([^>]*)>", html):
        is_close = m.group(1) == "/"
        tag_name = m.group(2).lower()
        if tag_name not in _PAIRED_TAGS:
            continue
        if is_close:
            for i in range(len(tag_stack) - 1, -1, -1):
                if tag_stack[i] == tag_name:
                    tag_stack.pop(i)
                    open_tag_texts.pop(i)
                    break
        else:
            tag_stack.append(tag_name)
            open_tag_texts.append(m.group(0))

    closing = "".join(f"</{t}>" for t in reversed(tag_stack))
    reopening = "".join(open_tag_texts)
    return html + closing, reopening
