from __future__ import annotations

import json
import re
from dataclasses import dataclass
from typing import Any, TextIO


@dataclass(frozen=True)
class _Line:
    indent: int
    content: str


_MAPPING_RE = re.compile(r"^([^:#][^:]*?):(?:\s(.*))?$")


def _read_text(stream: Any) -> str:
    if hasattr(stream, "read"):
        return str(stream.read())
    return str(stream or "")


def _prepare_lines(text: str) -> list[_Line]:
    lines: list[_Line] = []
    for raw in text.splitlines():
        stripped = raw.strip()
        if not stripped or stripped in {"---", "..."} or stripped.startswith("#"):
            continue
        indent = len(raw) - len(raw.lstrip(" "))
        lines.append(_Line(indent=indent, content=raw[indent:]))
    return lines


def _split_mapping(text: str) -> tuple[str, str | None]:
    match = _MAPPING_RE.match(text)
    if not match:
        raise ValueError(f"unsupported yaml mapping line: {text!r}")
    key = match.group(1).strip()
    value = match.group(2)
    return key, value if value is not None else None


def _parse_scalar(text: str) -> Any:
    value = text.strip()
    if value == "":
        return ""
    if value[0] in {'"', "'"} and value[-1:] == value[0]:
        if value[0] == '"':
            return json.loads(value)
        return value[1:-1]
    lowered = value.lower()
    if lowered in {"null", "~"}:
        return None
    if lowered == "true":
        return True
    if lowered == "false":
        return False
    if value.startswith("{") or value.startswith("["):
        try:
            return json.loads(value)
        except json.JSONDecodeError:
            pass
    if re.fullmatch(r"-?\d+", value):
        try:
            return int(value)
        except ValueError:
            return value
    if re.fullmatch(r"-?\d+\.\d+", value):
        try:
            return float(value)
        except ValueError:
            return value
    return value


def _parse_block(lines: list[_Line], index: int, indent: int) -> tuple[Any, int]:
    if index >= len(lines):
        return {}, index
    line = lines[index]
    stripped = line.content.strip()
    if line.indent == indent and (stripped.startswith("{") or stripped.startswith("[")):
        return _parse_scalar(stripped), index + 1
    if line.content.startswith("- ") and line.indent == indent:
        return _parse_list(lines, index, indent)
    return _parse_dict(lines, index, indent)


def _parse_dict(lines: list[_Line], index: int, indent: int) -> tuple[dict[str, Any], int]:
    payload: dict[str, Any] = {}
    while index < len(lines):
        line = lines[index]
        if line.indent < indent:
            break
        if line.indent != indent or line.content.startswith("- "):
            break
        key, inline_value = _split_mapping(line.content)
        index += 1
        if inline_value is None:
            if index < len(lines) and lines[index].indent > indent:
                value, index = _parse_block(lines, index, indent + 2)
            else:
                value = {}
        else:
            value = _parse_scalar(inline_value)
        payload[key] = value
    return payload, index


def _parse_list(lines: list[_Line], index: int, indent: int) -> tuple[list[Any], int]:
    items: list[Any] = []
    while index < len(lines):
        line = lines[index]
        if line.indent < indent:
            break
        if line.indent != indent or not line.content.startswith("- "):
            break
        item_text = line.content[2:].strip()
        index += 1
        if not item_text:
            if index < len(lines) and lines[index].indent > indent:
                value, index = _parse_block(lines, index, indent + 2)
            else:
                value = None
            items.append(value)
            continue
        if _MAPPING_RE.match(item_text):
            key, inline_value = _split_mapping(item_text)
            item: dict[str, Any] = {}
            if inline_value is None:
                if index < len(lines) and lines[index].indent > indent + 2:
                    value, index = _parse_block(lines, index, indent + 4)
                else:
                    value = {}
            else:
                value = _parse_scalar(inline_value)
            item[key] = value
            while index < len(lines):
                extra = lines[index]
                if extra.indent < indent + 2:
                    break
                if extra.indent == indent and extra.content.startswith("- "):
                    break
                if extra.indent != indent + 2 or extra.content.startswith("- "):
                    break
                child_key, child_inline = _split_mapping(extra.content)
                index += 1
                if child_inline is None:
                    if index < len(lines) and lines[index].indent > indent + 2:
                        child_value, index = _parse_block(lines, index, indent + 4)
                    else:
                        child_value = {}
                else:
                    child_value = _parse_scalar(child_inline)
                item[child_key] = child_value
            items.append(item)
            continue
        items.append(_parse_scalar(item_text))
    return items, index


def safe_load(stream: Any) -> Any:
    text = _read_text(stream)
    if not text.strip():
        return None
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        pass
    lines = _prepare_lines(text)
    if not lines:
        return None
    value, _ = _parse_block(lines, 0, lines[0].indent)
    return value


def _quote_string(value: str, *, allow_unicode: bool) -> str:
    plain_safe = (
        value
        and value == value.strip()
        and not value.startswith(("-", "?", ":", "{", "}", "[", "]", ",", "&", "*", "!", "|", ">", "@", "`"))
        and "\n" not in value
        and "#" not in value
        and ": " not in value
    )
    if plain_safe and value.lower() not in {"null", "true", "false", "~"}:
        return value
    return json.dumps(value, ensure_ascii=not allow_unicode)


def _dump_lines(value: Any, *, indent: int, allow_unicode: bool, sort_keys: bool) -> list[str]:
    prefix = " " * indent
    if isinstance(value, dict):
        if not value:
            return [prefix + "{}"]
        items = value.items()
        if sort_keys:
            items = sorted(items, key=lambda item: str(item[0]))
        lines: list[str] = []
        for key, item in items:
            rendered_key = str(key)
            if isinstance(item, (dict, list)):
                lines.append(f"{prefix}{rendered_key}:")
                lines.extend(_dump_lines(item, indent=indent + 2, allow_unicode=allow_unicode, sort_keys=sort_keys))
            else:
                lines.append(
                    f"{prefix}{rendered_key}: {_dump_scalar(item, allow_unicode=allow_unicode)}"
                )
        return lines
    if isinstance(value, list):
        if not value:
            return [prefix + "[]"]
        lines = []
        for item in value:
            if isinstance(item, (dict, list)):
                nested = _dump_lines(item, indent=indent + 2, allow_unicode=allow_unicode, sort_keys=sort_keys)
                head = nested[0].lstrip()
                lines.append(f"{prefix}- {head}")
                lines.extend(prefix + "  " + line.lstrip() for line in nested[1:])
            else:
                lines.append(f"{prefix}- {_dump_scalar(item, allow_unicode=allow_unicode)}")
        return lines
    return [prefix + _dump_scalar(value, allow_unicode=allow_unicode)]


def _dump_scalar(value: Any, *, allow_unicode: bool) -> str:
    if value is None:
        return "null"
    if value is True:
        return "true"
    if value is False:
        return "false"
    if isinstance(value, (int, float)):
        return str(value)
    return _quote_string(str(value), allow_unicode=allow_unicode)


def safe_dump(data: Any, stream: TextIO | None = None, *, allow_unicode: bool = True, sort_keys: bool = False, **_: Any) -> str | None:
    text = "\n".join(_dump_lines(data, indent=0, allow_unicode=allow_unicode, sort_keys=sort_keys)) + "\n"
    if stream is not None:
        stream.write(text)
        return None
    return text


dump = safe_dump
load = safe_load
