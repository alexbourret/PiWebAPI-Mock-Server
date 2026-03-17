import re
from datetime import datetime, timedelta, timezone
from typing import Iterable

ISO_RE = re.compile(r"^P(?:T(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?)$")


# def parse_time(value: str | None, default: datetime) -> datetime:
def parse_time(value, default):
    if not value:
        return default
    if value == "*":
        return datetime.now(timezone.utc)
    stripped_value = value.strip()
    if stripped_value.endswith("Z"):
        stripped_value = stripped_value[:-1] + "+00:00"
    date_time = datetime.fromisoformat(stripped_value)
    return date_time if date_time.tzinfo else date_time.replace(tzinfo=timezone.utc)


# def parse_interval(value: str | None, fallback: timedelta = timedelta(minutes=15)) -> timedelta:
def parse_interval(value, fallback=timedelta(minutes=15)):
    if not value:
        return fallback
    text = value.strip().lower()

    matched = re.match(r"^(\d+)([smhd])$", text)
    if matched:
        number = int(matched.group(1))
        unit = matched.group(2)
        if unit == "s":
            return timedelta(seconds=number)
        if unit == "m":
            return timedelta(minutes=number)
        if unit == "h":
            return timedelta(hours=number)
        if unit == "d":
            return timedelta(days=number)

    iso = ISO_RE.match(value.strip().upper())
    if iso:
        hours = int(iso.group(1) or 0)
        minutes = int(iso.group(2) or 0)
        seconds = int(iso.group(3) or 0)
        total = timedelta(hours=hours, minutes=minutes, seconds=seconds)
        if total.total_seconds() > 0:
            return total

    return fallback


def datetime_list(start: datetime, end: datetime, step: timedelta) -> Iterable[datetime]:
    if end < start:
        start, end = end, start
    current = start
    guard = 0
    max_points = 20000
    while current <= end and guard < max_points:
        yield current
        current += step
        guard += 1


def case_insensitive_get(object, key, default=None):
    if key in object:
        return object[key]
    lower_key = key.lower()
    for object_key, object_value in object.items():
        if object_key.lower() == lower_key:
            return object_value
    return default


def parse_json_path(json_path, data):
    if not json_path.startswith("$"):
        raise ValueError("JsonPath must start with '$'")
    nodes = [data]
    path_cursor = 1
    path_length = len(json_path)
    while path_cursor < path_length:
        char = json_path[path_cursor]
        if char == ".":
            path_cursor += 1
            start = path_cursor
            while path_cursor < path_length and json_path[path_cursor] not in ".[":
                path_cursor += 1
            key = json_path[start:path_cursor]
            if not key:
                raise ValueError(f"Invalid JsonPath segment in {json_path}")
            next_nodes = []
            for node in nodes:
                if isinstance(node, dict) and key in node:
                    next_nodes.append(node[key])
            nodes = next_nodes
        elif char == "[":
            end = json_path.find("]", path_cursor)
            if end < 0:
                raise ValueError(f"Unclosed bracket in JsonPath: {json_path}")
            token = json_path[path_cursor + 1: end].strip()
            next_nodes = []
            if token == "*":
                for node in nodes:
                    if isinstance(node, list):
                        next_nodes.extend(node)
                    elif isinstance(node, dict):
                        next_nodes.extend(node.values())
            else:
                try:
                    idx = int(token)
                except ValueError as e:
                    raise ValueError(f"Unsupported bracket token '{token}' in JsonPath") from e
                for node in nodes:
                    if isinstance(node, list) and -len(node) <= idx < len(node):
                        next_nodes.append(node[idx])
            nodes = next_nodes
            path_cursor = end + 1
        else:
            raise ValueError(f"Unexpected token '{char}' in JsonPath: {json_path}")
    return nodes
