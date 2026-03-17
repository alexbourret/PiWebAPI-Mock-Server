import base64
import hmac
import os
from typing import Dict


def build_users() -> Dict[str, str]:
    default_users = {
        "operator_north": "north123",
        "operator_south": "south123",
        "supervisor": "supervisor123",
        "admin": "admin123",
    }
    env_spec = os.getenv("MOCK_PIWEBAPI_USERS", "").strip()
    if not env_spec:
        return default_users

    users: Dict[str, str] = {}
    for pair in env_spec.split(","):
        item = pair.strip()
        if not item or ":" not in item:
            continue
        user, password = item.split(":", 1)
        user = user.strip()
        if user:
            users[user] = password
    return users or default_users


def is_valid_basic_auth(header_value: str, users: Dict[str, str]) -> bool:
    if not header_value.startswith("Basic "):
        return False
    try:
        decoded = base64.b64decode(header_value.split(" ", 1)[1], validate=True).decode("utf-8")
    except Exception:
        return False

    if ":" not in decoded:
        return False
    username, password = decoded.split(":", 1)
    expected = users.get(username)
    if expected is None:
        return False
    return hmac.compare_digest(expected, password)
