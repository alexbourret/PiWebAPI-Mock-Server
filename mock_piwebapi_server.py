#!/usr/bin/env python3
"""Mock AVEVA / OSIsoft PI Web API server.

This server generates realistic AF-like databases, elements, and attributes for
factory scenarios and exposes PI Web API-inspired endpoints.

Key behavior:
- 3 factory databases by default.
- Large nested element trees (avg depth around 5).
- Deterministic pseudo-random values for any attribute/time query.
"""

from __future__ import annotations

import argparse
import base64
import fnmatch
import hashlib
import hmac
import json
import os
import re
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Dict, Iterable, List, Optional, Tuple
from urllib.parse import parse_qs, urlparse


ISO_RE = re.compile(r"^P(?:T(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?)$")


@dataclass
class Attribute:
    web_id: str
    name: str
    path: str
    element_web_id: str
    units: str
    data_type: str = "Double"


@dataclass
class Element:
    web_id: str
    name: str
    path: str
    database_web_id: str
    parent_web_id: Optional[str]
    children: List[str] = field(default_factory=list)
    attributes: List[str] = field(default_factory=list)


@dataclass
class AssetDatabase:
    web_id: str
    name: str
    path: str
    root_element_web_id: str


class PiWebApiDataModel:
    def __init__(self, db_names: List[str], seed: str = "piwebapi-mock-seed") -> None:
        self.seed = seed
        self.databases_by_webid: Dict[str, AssetDatabase] = {}
        self.databases_by_name: Dict[str, AssetDatabase] = {}
        self.elements_by_webid: Dict[str, Element] = {}
        self.elements_by_path: Dict[Tuple[str, str], Element] = {}
        self.attributes_by_webid: Dict[str, Attribute] = {}
        self.attributes_by_path: Dict[str, Attribute] = {}

        for db_name in db_names:
            self._create_database(db_name)

    def _mk_web_id(self, kind: str, canonical: str) -> str:
        payload = f"{kind}|{canonical}".encode("utf-8")
        digest = hashlib.sha1(payload).hexdigest().upper()
        return f"{kind[:1].upper()}{digest[:22]}"

    def _norm_path(self, path: str) -> str:
        p = path.replace("/", "\\").strip()
        if not p.startswith("\\"):
            p = "\\" + p
        p = re.sub(r"\\+", r"\\", p)
        return p

    def _create_database(self, db_name: str) -> None:
        db_path = f"\\\\MockServer\\{db_name}"
        db_web_id = self._mk_web_id("database", db_path)

        root_name = db_name
        root_path = f"\\\\{db_name}\\{root_name}"
        root_web_id = self._mk_web_id("element", root_path)

        db = AssetDatabase(
            web_id=db_web_id,
            name=db_name,
            path=db_path,
            root_element_web_id=root_web_id,
        )
        self.databases_by_webid[db_web_id] = db
        self.databases_by_name[db_name.lower()] = db

        root = Element(
            web_id=root_web_id,
            name=root_name,
            path=root_path,
            database_web_id=db_web_id,
            parent_web_id=None,
        )
        self.elements_by_webid[root_web_id] = root
        self.elements_by_path[(db_web_id, self._norm_path(root_path).lower())] = root

        self._attach_attributes(root, is_leaf=False)

        for area_i in range(1, 9):
            area = self._create_child(root, db_web_id, f"Area-{area_i:02d}")
            for line_i in range(1, 6):
                line = self._create_child(area, db_web_id, f"Line-{line_i:02d}")
                for unit_i in range(1, 5):
                    unit = self._create_child(line, db_web_id, f"Unit-{unit_i:02d}")
                    for station_i in range(1, 4):
                        station = self._create_child(unit, db_web_id, f"Station-{station_i:02d}")
                        for cell_i in range(1, 3):
                            leaf = self._create_child(station, db_web_id, f"Cell-{cell_i:02d}")
                            self._attach_attributes(leaf, is_leaf=True)
                        self._attach_attributes(station, is_leaf=False)
                    self._attach_attributes(unit, is_leaf=False)
                self._attach_attributes(line, is_leaf=False)
            self._attach_attributes(area, is_leaf=False)

    def _create_child(self, parent: Element, db_web_id: str, name: str) -> Element:
        child_path = f"{parent.path}\\{name}"
        child_web_id = self._mk_web_id("element", child_path)
        child = Element(
            web_id=child_web_id,
            name=name,
            path=child_path,
            database_web_id=db_web_id,
            parent_web_id=parent.web_id,
        )
        self.elements_by_webid[child_web_id] = child
        self.elements_by_path[(db_web_id, self._norm_path(child_path).lower())] = child
        parent.children.append(child.web_id)
        return child

    def _attach_attributes(self, element: Element, is_leaf: bool) -> None:
        base_attrs = [
            ("Status", "state"),
            ("Power_kW", "kW"),
            ("Energy_kWh", "kWh"),
            ("Temperature_C", "degC"),
        ]
        leaf_attrs = [
            ("Pressure_bar", "bar"),
            ("Flow_m3_h", "m3/h"),
            ("Vibration_mm_s", "mm/s"),
            ("Output_pct", "%"),
            ("QualityScore", "score"),
            ("Setpoint", "unit"),
        ]

        # Ensure each element has at least one attribute name unique to that element.
        unique_suffix = element.web_id[-8:]
        unique_attr = (f"UniqueTag_{unique_suffix}", "id")
        attrs = base_attrs + (leaf_attrs if is_leaf else []) + [unique_attr]
        for attr_name, unit in attrs:
            attr_path = f"{element.path}|{attr_name}"
            web_id = self._mk_web_id("attribute", attr_path)
            attribute = Attribute(
                web_id=web_id,
                name=attr_name,
                path=attr_path,
                element_web_id=element.web_id,
                units=unit,
                data_type="Int32" if attr_name == "Status" else "Double",
            )
            self.attributes_by_webid[web_id] = attribute
            self.attributes_by_path[attr_path.lower()] = attribute
            element.attributes.append(web_id)

    def list_databases(self) -> List[AssetDatabase]:
        return list(self.databases_by_webid.values())

    def get_database(self, identifier: str) -> Optional[AssetDatabase]:
        return self.databases_by_webid.get(identifier) or self.databases_by_name.get(identifier.lower())

    def get_element(self, web_id: str) -> Optional[Element]:
        return self.elements_by_webid.get(web_id)

    def get_attribute(self, web_id: str) -> Optional[Attribute]:
        return self.attributes_by_webid.get(web_id)

    def find_element_by_path(self, db_web_id: str, path: str) -> Optional[Element]:
        return self.elements_by_path.get((db_web_id, self._norm_path(path).lower()))

    def descendant_element_ids(self, root_element_web_id: str) -> List[str]:
        out: List[str] = []
        stack: List[str] = [root_element_web_id]
        while stack:
            cur_id = stack.pop()
            out.append(cur_id)
            cur = self.get_element(cur_id)
            if not cur:
                continue
            stack.extend(cur.children)
        return out

    def deterministic_value(self, attribute: Attribute, timestamp: datetime) -> float | int:
        ts_key = timestamp.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        digest = hashlib.sha256(f"{self.seed}|{attribute.web_id}|{ts_key}".encode("utf-8")).hexdigest()
        bucket = int(digest[:16], 16) / float(0xFFFFFFFFFFFFFFFF)

        name = attribute.name
        if name == "Status":
            return 1 if bucket > 0.08 else 0
        if "Temperature" in name:
            return round(18.0 + bucket * 70.0, 3)
        if "Power" in name:
            return round(50.0 + bucket * 950.0, 3)
        if "Energy" in name:
            return round(500.0 + bucket * 9500.0, 3)
        if "Pressure" in name:
            return round(1.0 + bucket * 15.0, 3)
        if "Flow" in name:
            return round(5.0 + bucket * 240.0, 3)
        if "Vibration" in name:
            return round(bucket * 14.0, 3)
        if "Output" in name:
            return round(bucket * 100.0, 3)
        if "Quality" in name:
            return round(85.0 + bucket * 15.0, 3)
        if "Setpoint" in name:
            return round(5.0 + bucket * 95.0, 3)
        return round(bucket * 1000.0, 3)


def parse_time(value: str | None, default: datetime) -> datetime:
    if not value:
        return default
    if value == "*":
        return datetime.now(timezone.utc)
    v = value.strip()
    if v.endswith("Z"):
        v = v[:-1] + "+00:00"
    dt = datetime.fromisoformat(v)
    return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)


def parse_interval(value: str | None, fallback: timedelta = timedelta(minutes=15)) -> timedelta:
    if not value:
        return fallback
    txt = value.strip().lower()

    m = re.match(r"^(\d+)([smhd])$", txt)
    if m:
        n = int(m.group(1))
        unit = m.group(2)
        if unit == "s":
            return timedelta(seconds=n)
        if unit == "m":
            return timedelta(minutes=n)
        if unit == "h":
            return timedelta(hours=n)
        if unit == "d":
            return timedelta(days=n)

    iso = ISO_RE.match(value.strip().upper())
    if iso:
        h = int(iso.group(1) or 0)
        mnt = int(iso.group(2) or 0)
        s = int(iso.group(3) or 0)
        total = timedelta(hours=h, minutes=mnt, seconds=s)
        if total.total_seconds() > 0:
            return total

    return fallback


def datetime_list(start: datetime, end: datetime, step: timedelta) -> Iterable[datetime]:
    if end < start:
        start, end = end, start
    cur = start
    guard = 0
    max_points = 20000
    while cur <= end and guard < max_points:
        yield cur
        cur += step
        guard += 1


class PiWebApiHandler(BaseHTTPRequestHandler):
    model: PiWebApiDataModel = None  # type: ignore
    users: Dict[str, str] = {}
    asset_server_name: str = "Mock-AssetServer"
    asset_server_web_id: str = "ASMOCKSERVER001"

    def _write_json(self, status: int, payload: dict) -> None:
        body = json.dumps(payload).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _base_url(self) -> str:
        host = self.headers.get("Host", f"localhost:{self.server.server_port}")
        return f"http://{host}/piwebapi"

    def _db_item(self, db: AssetDatabase) -> dict:
        base = self._base_url()
        return {
            "WebId": db.web_id,
            "Id": db.web_id,
            "Name": db.name,
            "Path": db.path,
            "Links": {
                "Self": f"{base}/assetdatabases/{db.web_id}",
                "Elements": f"{base}/assetdatabases/{db.web_id}/elements",
            },
        }

    def _asset_server_item(self) -> dict:
        base = self._base_url()
        return {
            "WebId": self.asset_server_web_id,
            "Id": self.asset_server_web_id,
            "Name": self.asset_server_name,
            "Path": f"\\\\{self.asset_server_name}",
            "Links": {
                "Self": f"{base}/assetservers/{self.asset_server_web_id}",
                "Databases": f"{base}/assetservers/{self.asset_server_web_id}/assetdatabases",
            },
        }

    def _element_item(self, elem: Element) -> dict:
        base = self._base_url()
        return {
            "WebId": elem.web_id,
            "Id": elem.web_id,
            "Name": elem.name,
            "Path": elem.path,
            "HasChildren": bool(elem.children),
            "Links": {
                "Self": f"{base}/elements/{elem.web_id}",
                "Elements": f"{base}/elements/{elem.web_id}/elements",
                "Attributes": f"{base}/elements/{elem.web_id}/attributes",
            },
        }

    def _attribute_item(self, attr: Attribute) -> dict:
        base = self._base_url()
        return {
            "WebId": attr.web_id,
            "Id": attr.web_id,
            "Name": attr.name,
            "Path": attr.path,
            "Type": attr.data_type,
            "DefaultUnitsName": attr.units,
            "Links": {
                "Self": f"{base}/attributes/{attr.web_id}",
                "Value": f"{base}/streams/{attr.web_id}/value",
                "RecordedData": f"{base}/streams/{attr.web_id}/recorded",
            },
        }

    def _error(self, status: int, msg: str) -> None:
        self._write_json(status, {"Errors": [msg]})

    def _unauthorized(self, msg: str = "Authentication required") -> None:
        body = json.dumps({"Errors": [msg]}).encode("utf-8")
        self.send_response(HTTPStatus.UNAUTHORIZED)
        self.send_header("WWW-Authenticate", 'Basic realm="PIWebAPI Mock"')
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _auth_ok(self) -> bool:
        auth = self.headers.get("Authorization", "")
        if not auth.startswith("Basic "):
            return False
        try:
            decoded = base64.b64decode(auth.split(" ", 1)[1], validate=True).decode("utf-8")
        except Exception:
            return False

        if ":" not in decoded:
            return False
        username, password = decoded.split(":", 1)
        expected = self.users.get(username)
        if expected is None:
            return False
        return hmac.compare_digest(expected, password)

    def _parse_attribute_search_query(self, search_expr: str) -> Tuple[Optional[str], str]:
        """
        Supports query fragments like:
          Element:{Root:'Area-01\\Line-02' Name:'*'}
        """
        text = search_expr.strip()
        if not text:
            return None, "*"

        m = re.search(r"Element\s*:\s*\{(.*)\}\s*$", text, flags=re.IGNORECASE)
        if not m:
            return None, "*"
        body = m.group(1)

        root_m = re.search(r"Root\s*:\s*'([^']*)'", body, flags=re.IGNORECASE)
        name_m = re.search(r"Name\s*:\s*'([^']*)'", body, flags=re.IGNORECASE)

        root = root_m.group(1).strip() if root_m else None
        pattern = (name_m.group(1).strip() if name_m else "*") or "*"
        return root, pattern

    def _resolve_root_element_for_search(self, db: AssetDatabase, root_expr: Optional[str]) -> Optional[Element]:
        db_root = self.model.get_element(db.root_element_web_id)
        if not db_root:
            return None
        if not root_expr:
            return db_root

        normalized = root_expr.strip().replace("/", "\\").strip("\\")
        if not normalized:
            return db_root

        # Try path as provided (absolute-ish), then as relative to database root.
        direct = self.model.find_element_by_path(db.web_id, root_expr)
        if direct:
            return direct

        candidate = f"{db_root.path}\\{normalized}"
        rel = self.model.find_element_by_path(db.web_id, candidate)
        if rel:
            return rel

        return None

    def do_GET(self) -> None:  # noqa: N802
        parsed = urlparse(self.path)
        path = parsed.path.rstrip("/") or "/"
        query = parse_qs(parsed.query)
        query_ci = {k.lower(): v for k, v in query.items()}

        if path.startswith("/piwebapi") and not self._auth_ok():
            self._unauthorized("Missing or invalid basic authentication credentials")
            return

        if path == "/" or path == "/piwebapi":
            self._write_json(
                HTTPStatus.OK,
                {
                    "ProductVersion": "Mock-1.0",
                    "Links": {
                        "AssetServers": f"{self._base_url()}/assetservers",
                    },
                },
            )
            return

        if path == "/piwebapi/assetservers":
            item = self._asset_server_item()
            self._write_json(HTTPStatus.OK, {"Items": [item], "Total": 1})
            return

        m = re.match(r"^/piwebapi/assetservers/([^/]+)$", path)
        if m:
            if m.group(1) != self.asset_server_web_id:
                self._error(HTTPStatus.NOT_FOUND, "Asset server not found")
                return
            self._write_json(HTTPStatus.OK, self._asset_server_item())
            return

        m = re.match(r"^/piwebapi/assetservers/([^/]+)/assetdatabases$", path)
        if m:
            if m.group(1) != self.asset_server_web_id:
                self._error(HTTPStatus.NOT_FOUND, "Asset server not found")
                return
            items = [self._db_item(db) for db in self.model.list_databases()]
            self._write_json(HTTPStatus.OK, {"Items": items, "Total": len(items)})
            return

        if path == "/piwebapi/assetdatabases":
            items = [self._db_item(db) for db in self.model.list_databases()]
            self._write_json(HTTPStatus.OK, {"Items": items, "Total": len(items)})
            return

        m = re.match(r"^/piwebapi/assetdatabases/([^/]+)$", path)
        if m:
            db = self.model.get_database(m.group(1))
            if not db:
                self._error(HTTPStatus.NOT_FOUND, "Asset database not found")
                return
            self._write_json(HTTPStatus.OK, self._db_item(db))
            return

        m = re.match(r"^/piwebapi/assetdatabases/([^/]+)/elements$", path)
        if m:
            db = self.model.get_database(m.group(1))
            if not db:
                self._error(HTTPStatus.NOT_FOUND, "Asset database not found")
                return

            path_q = query_ci.get("path", [None])[0]
            if path_q:
                element = self.model.find_element_by_path(db.web_id, path_q)
                items = [self._element_item(element)] if element else []
            else:
                root = self.model.get_element(db.root_element_web_id)
                if not root:
                    items = []
                else:
                    items = [self._element_item(self.model.get_element(cid)) for cid in root.children]
                    items = [it for it in items if it is not None]

            self._write_json(HTTPStatus.OK, {"Items": items, "Total": len(items)})
            return

        m = re.match(r"^/piwebapi/elements/([^/]+)$", path)
        if m:
            elem = self.model.get_element(m.group(1))
            if not elem:
                self._error(HTTPStatus.NOT_FOUND, "Element not found")
                return
            self._write_json(HTTPStatus.OK, self._element_item(elem))
            return

        m = re.match(r"^/piwebapi/elements/([^/]+)/elements$", path)
        if m:
            elem = self.model.get_element(m.group(1))
            if not elem:
                self._error(HTTPStatus.NOT_FOUND, "Element not found")
                return
            items = [self._element_item(self.model.get_element(cid)) for cid in elem.children]
            items = [it for it in items if it is not None]
            self._write_json(HTTPStatus.OK, {"Items": items, "Total": len(items)})
            return

        m = re.match(r"^/piwebapi/elements/([^/]+)/attributes$", path)
        if m:
            elem = self.model.get_element(m.group(1))
            if not elem:
                self._error(HTTPStatus.NOT_FOUND, "Element not found")
                return
            items = [self._attribute_item(self.model.get_attribute(aid)) for aid in elem.attributes]
            items = [it for it in items if it is not None]
            self._write_json(HTTPStatus.OK, {"Items": items, "Total": len(items)})
            return

        if path == "/piwebapi/attributes/search":
            db_web_id = (query_ci.get("databasewebid", [None])[0] or "").strip()
            if not db_web_id:
                self._error(HTTPStatus.BAD_REQUEST, "Missing required parameter: databaseWebId")
                return
            db = self.model.get_database(db_web_id)
            if not db:
                self._error(HTTPStatus.NOT_FOUND, "Asset database not found")
                return

            search_expr = (query_ci.get("query", [""])[0] or "").strip()
            root_expr, name_pattern = self._parse_attribute_search_query(search_expr)
            root_elem = self._resolve_root_element_for_search(db, root_expr)
            if not root_elem:
                self._write_json(HTTPStatus.OK, {"Items": [], "Total": 0})
                return

            name_pattern_lc = name_pattern.lower()
            items: List[dict] = []
            for elem_id in self.model.descendant_element_ids(root_elem.web_id):
                elem = self.model.get_element(elem_id)
                if not elem:
                    continue
                for attr_id in elem.attributes:
                    attr = self.model.get_attribute(attr_id)
                    if not attr:
                        continue
                    if fnmatch.fnmatch(attr.name.lower(), name_pattern_lc):
                        items.append(self._attribute_item(attr))

            self._write_json(HTTPStatus.OK, {"Items": items, "Total": len(items)})
            return

        m = re.match(r"^/piwebapi/attributes/([^/]+)$", path)
        if m:
            attr = self.model.get_attribute(m.group(1))
            if not attr:
                self._error(HTTPStatus.NOT_FOUND, "Attribute not found")
                return
            self._write_json(HTTPStatus.OK, self._attribute_item(attr))
            return

        m = re.match(r"^/piwebapi/streams/([^/]+)/value$", path)
        if m:
            attr = self.model.get_attribute(m.group(1))
            if not attr:
                self._error(HTTPStatus.NOT_FOUND, "Attribute not found")
                return
            now = datetime.now(timezone.utc)
            t = parse_time(query_ci.get("time", [None])[0], now)
            value = self.model.deterministic_value(attr, t)
            self._write_json(
                HTTPStatus.OK,
                {
                    "Timestamp": t.astimezone(timezone.utc).isoformat().replace("+00:00", "Z"),
                    "Value": value,
                    "Good": True,
                    "Questionable": False,
                    "Substituted": False,
                },
            )
            return

        m = re.match(r"^/piwebapi/streams/([^/]+)/recorded$", path)
        if m:
            attr = self.model.get_attribute(m.group(1))
            if not attr:
                self._error(HTTPStatus.NOT_FOUND, "Attribute not found")
                return

            now = datetime.now(timezone.utc)
            try:
                start = parse_time(query_ci.get("starttime", [None])[0], now - timedelta(hours=8))
                end = parse_time(query_ci.get("endtime", [None])[0], now)
            except ValueError:
                self._error(
                    HTTPStatus.BAD_REQUEST,
                    "Invalid timestamp format for startTime/endTime. Expected ISO-8601.",
                )
                return
            step = parse_interval(query_ci.get("interval", [None])[0], fallback=timedelta(minutes=15))

            if end < start:
                self._error(HTTPStatus.BAD_REQUEST, "endTime must be greater than or equal to startTime")
                return

            items = []
            for ts in datetime_list(start, end, step):
                value = self.model.deterministic_value(attr, ts)
                items.append(
                    {
                        "Timestamp": ts.astimezone(timezone.utc).isoformat().replace("+00:00", "Z"),
                        "Value": value,
                        "Good": True,
                        "Questionable": False,
                        "Substituted": False,
                    }
                )

            self._write_json(
                HTTPStatus.OK,
                {
                    "Items": items,
                    "UnitsAbbreviation": attr.units,
                    "Total": len(items),
                },
            )
            return

        self._error(HTTPStatus.NOT_FOUND, f"Unsupported endpoint: {path}")

    def log_message(self, fmt: str, *args) -> None:
        # Keep server logs concise while preserving useful request tracing.
        print(f"[{self.log_date_time_string()}] {self.address_string()} {fmt % args}")


def build_default_model(seed: str = "piwebapi-mock-seed") -> PiWebApiDataModel:
    db_names = ["Factory-North", "Factory-South", "Factory-West"]
    return PiWebApiDataModel(db_names=db_names, seed=seed)


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
        if not item:
            continue
        if ":" not in item:
            continue
        user, pwd = item.split(":", 1)
        user = user.strip()
        if user:
            users[user] = pwd
    return users or default_users


def main() -> None:
    parser = argparse.ArgumentParser(description="Run PI Web API mock server")
    parser.add_argument("--host", default="0.0.0.0", help="Bind host")
    parser.add_argument("--port", type=int, default=8080, help="Bind port")
    parser.add_argument(
        "--seed",
        default="piwebapi-mock-seed",
        help="Seed controlling deterministic value generation",
    )
    args = parser.parse_args()

    model = build_default_model(seed=args.seed)
    PiWebApiHandler.model = model
    PiWebApiHandler.users = build_users()

    server = ThreadingHTTPServer((args.host, args.port), PiWebApiHandler)

    total_elements = len(model.elements_by_webid)
    total_attributes = len(model.attributes_by_webid)
    print(
        f"Mock PI Web API listening on http://{args.host}:{args.port}/piwebapi "
        f"(databases={len(model.databases_by_webid)}, elements={total_elements}, attributes={total_attributes}, users={len(PiWebApiHandler.users)})"
    )

    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nShutting down...")
    finally:
        server.server_close()


if __name__ == "__main__":
    main()
