import fnmatch
import json
import re
from datetime import datetime, timedelta, timezone
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import parse_qs, urlparse

from .auth import is_valid_basic_auth
from .batch import BatchExecutor
from .domain import AssetDatabase
from .model import PiWebApiDataModel
from .serializers import ApiSerializer
from .utils import datetime_list, parse_interval, parse_time


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

    def _serializer(self) -> ApiSerializer:
        return ApiSerializer(self.model, self._base_url())

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
        return is_valid_basic_auth(self.headers.get("Authorization", ""), self.users)

    def _parse_attribute_search_query(self, search_expr: str) -> Tuple[Optional[str], str]:
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

    def _resolve_root_element_for_search(self, db: AssetDatabase, root_expr: Optional[str]):
        db_root = self.model.get_element(db.root_element_web_id)
        if not db_root:
            return None
        if not root_expr:
            return db_root

        normalized = root_expr.strip().replace("/", "\\").strip("\\")
        if not normalized:
            return db_root

        direct = self.model.find_element_by_path(db.web_id, root_expr)
        if direct:
            return direct

        candidate = f"{db_root.path}\\{normalized}"
        return self.model.find_element_by_path(db.web_id, candidate)

    def _handle_get_internal(self, path: str, query_case_insensitive: Dict[str, List[str]]) -> Tuple[int, Dict[str, Any]]:
        ser = self._serializer()
        if path == "/" or path == "/piwebapi":
            return HTTPStatus.OK, {"ProductVersion": "Mock-1.0", "Links": {"AssetServers": f"{self._base_url()}/assetservers"}}

        if path == "/piwebapi/assetservers":
            item = ser.asset_server_item(self.asset_server_web_id, self.asset_server_name)
            return HTTPStatus.OK, {"Items": [item], "Total": 1}

        matched = re.match(r"^/piwebapi/assetservers/([^/]+)$", path)
        if matched:
            if matched.group(1) != self.asset_server_web_id:
                return HTTPStatus.NOT_FOUND, {"Errors": ["Asset server not found"]}
            return HTTPStatus.OK, ser.asset_server_item(self.asset_server_web_id, self.asset_server_name)

        matched = re.match(r"^/piwebapi/assetservers/([^/]+)/assetdatabases$", path)
        if matched:
            if matched.group(1) != self.asset_server_web_id:
                return HTTPStatus.NOT_FOUND, {"Errors": ["Asset server not found"]}
            items = [ser.db_item(database) for database in self.model.list_databases()]
            return HTTPStatus.OK, {"Items": items, "Total": len(items)}

        if path == "/piwebapi/assetdatabases":
            items = [ser.db_item(database) for database in self.model.list_databases()]
            return HTTPStatus.OK, {"Items": items, "Total": len(items)}

        matched = re.match(r"^/piwebapi/assetdatabases/([^/]+)$", path)
        if matched:
            database = self.model.get_database(matched.group(1))
            if not database:
                return HTTPStatus.NOT_FOUND, {"Errors": ["Asset database not found"]}
            return HTTPStatus.OK, ser.db_item(database)

        matched = re.match(r"^/piwebapi/assetdatabases/([^/]+)/elements$", path)
        if matched:
            database = self.model.get_database(matched.group(1))
            if not database:
                return HTTPStatus.NOT_FOUND, {"Errors": ["Asset database not found"]}
            path_query = query_case_insensitive.get("path", [None])[0]
            if path_query:
                element = self.model.find_element_by_path(database.web_id, path_query)
                items = [ser.element_item(element)] if element else []
            else:
                root = self.model.get_element(database.root_element_web_id)
                if not root:
                    items = []
                else:
                    items = [ser.element_item(self.model.get_element(child_id)) for child_id in root.children]
                    items = [item for item in items if item is not None]
            return HTTPStatus.OK, {"Items": items, "Total": len(items)}

        matched = re.match(r"^/piwebapi/assetdatabases/([^/]+)/elementtemplates$", path)
        if matched:
            database = self.model.get_database(matched.group(1))
            if not database:
                return HTTPStatus.NOT_FOUND, {"Errors": ["Asset database not found"]}
            items = [ser.element_template_item(template) for template in self.model.list_element_templates_for_database(database.web_id)]
            return HTTPStatus.OK, {"Items": items, "Total": len(items)}

        if path == "/piwebapi/elementtemplates":
            items = [ser.element_template_item(template) for template in self.model.list_element_templates()]
            return HTTPStatus.OK, {"Items": items, "Total": len(items)}

        matched = re.match(r"^/piwebapi/elementtemplates/([^/]+)/attributetemplates$", path)
        if matched:
            template = self.model.get_element_template(matched.group(1))
            if not template:
                return HTTPStatus.NOT_FOUND, {"Errors": ["Element template not found"]}
            items = [ser.attribute_template_item(template) for template in self.model.effective_attribute_templates(template.web_id)]
            return HTTPStatus.OK, {"Items": items, "Total": len(items)}

        matched = re.match(r"^/piwebapi/elementtemplates/([^/]+)$", path)
        if matched:
            template = self.model.get_element_template(matched.group(1))
            if not template:
                return HTTPStatus.NOT_FOUND, {"Errors": ["Element template not found"]}
            return HTTPStatus.OK, ser.element_template_item(template)

        matched = re.match(r"^/piwebapi/attributetemplates/([^/]+)/attributetemplates$", path)
        if matched:
            template = self.model.get_attribute_template(matched.group(1))
            if not template:
                return HTTPStatus.NOT_FOUND, {"Errors": ["Attribute template not found"]}
            items = [ser.attribute_template_item(self.model.get_attribute_template(cid)) for cid in template.children]
            items = [item for item in items if item is not None]
            return HTTPStatus.OK, {"Items": items, "Total": len(items)}

        matched = re.match(r"^/piwebapi/attributetemplates/([^/]+)$", path)
        if matched:
            template = self.model.get_attribute_template(matched.group(1))
            if not template:
                return HTTPStatus.NOT_FOUND, {"Errors": ["Attribute template not found"]}
            return HTTPStatus.OK, ser.attribute_template_item(template)

        matched = re.match(r"^/piwebapi/elements/([^/]+)$", path)
        if matched:
            element = self.model.get_element(matched.group(1))
            if not element:
                return HTTPStatus.NOT_FOUND, {"Errors": ["Element not found"]}
            return HTTPStatus.OK, ser.element_item(element)

        matched = re.match(r"^/piwebapi/elements/([^/]+)/elements$", path)
        if matched:
            element = self.model.get_element(matched.group(1))
            if not element:
                return HTTPStatus.NOT_FOUND, {"Errors": ["Element not found"]}
            items = [ser.element_item(self.model.get_element(child_id)) for child_id in element.children]
            items = [item for item in items if item is not None]
            return HTTPStatus.OK, {"Items": items, "Total": len(items)}

        matched = re.match(r"^/piwebapi/elements/([^/]+)/attributes$", path)
        if matched:
            element = self.model.get_element(matched.group(1))
            if not element:
                return HTTPStatus.NOT_FOUND, {"Errors": ["Element not found"]}
            items = [ser.attribute_item(self.model.get_attribute(aid)) for aid in element.attributes]
            items = [it for it in items if it is not None]
            return HTTPStatus.OK, {"Items": items, "Total": len(items)}

        if path == "/piwebapi/attributes/search":
            db_web_id = (query_case_insensitive.get("databasewebid", [None])[0] or "").strip()
            if not db_web_id:
                return HTTPStatus.BAD_REQUEST, {"Errors": ["Missing required parameter: databaseWebId"]}
            database = self.model.get_database(db_web_id)
            if not database:
                return HTTPStatus.NOT_FOUND, {"Errors": ["Asset database not found"]}
            search_expr = (query_case_insensitive.get("query", [""])[0] or "").strip()
            root_expr, name_pattern = self._parse_attribute_search_query(search_expr)
            root_elem = self._resolve_root_element_for_search(database, root_expr)
            if not root_elem:
                return HTTPStatus.OK, {"Items": [], "Total": 0}

            name_pattern_lc = name_pattern.lower()
            items = []
            for elem_id in self.model.descendant_element_ids(root_elem.web_id):
                element = self.model.get_element(elem_id)
                if not element:
                    continue
                for attr_id in element.attributes:
                    attribute = self.model.get_attribute(attr_id)
                    if attribute and fnmatch.fnmatch(attribute.name.lower(), name_pattern_lc):
                        items.append(ser.attribute_item(attribute))
            return HTTPStatus.OK, {"Items": items, "Total": len(items)}

        matched = re.match(r"^/piwebapi/attributes/([^/]+)/attributes$", path)
        if matched:
            attribute = self.model.get_attribute(matched.group(1))
            if not attribute:
                return HTTPStatus.NOT_FOUND, {"Errors": ["Attribute not found"]}
            items = [ser.attribute_item(self.model.get_attribute(cid)) for cid in attribute.children]
            items = [it for it in items if it is not None]
            return HTTPStatus.OK, {"Items": items, "Total": len(items)}

        matched = re.match(r"^/piwebapi/attributes/([^/]+)$", path)
        if matched:
            attribute = self.model.get_attribute(matched.group(1))
            if not attribute:
                return HTTPStatus.NOT_FOUND, {"Errors": ["Attribute not found"]}
            return HTTPStatus.OK, ser.attribute_item(attribute)

        matched = re.match(r"^/piwebapi/streams/([^/]+)/value$", path)
        if matched:
            attribute = self.model.get_attribute(matched.group(1))
            if not attribute:
                return HTTPStatus.NOT_FOUND, {"Errors": ["Attribute not found"]}
            now = datetime.now(timezone.utc)
            try:
                t = parse_time(query_case_insensitive.get("time", [None])[0], now)
            except ValueError:
                return HTTPStatus.BAD_REQUEST, {"Errors": ["Invalid timestamp format for time. Expected ISO-8601."]}
            value = self.model.deterministic_value(attribute, t)
            return HTTPStatus.OK, {
                "Timestamp": t.astimezone(timezone.utc).isoformat().replace("+00:00", "Z"),
                "Value": value,
                "Good": True,
                "Questionable": False,
                "Substituted": False,
            }

        matched = re.match(r"^/piwebapi/streams/([^/]+)/recorded$", path)
        if matched:
            attribute = self.model.get_attribute(matched.group(1))
            if not attribute:
                return HTTPStatus.NOT_FOUND, {"Errors": ["Attribute not found"]}
            now = datetime.now(timezone.utc)
            try:
                start = parse_time(query_case_insensitive.get("starttime", [None])[0], now - timedelta(hours=8))
                end = parse_time(query_case_insensitive.get("endtime", [None])[0], now)
            except ValueError:
                return HTTPStatus.BAD_REQUEST, {"Errors": ["Invalid timestamp format for startTime/endTime. Expected ISO-8601."]}
            step = parse_interval(query_case_insensitive.get("interval", [None])[0], fallback=timedelta(minutes=15))
            if end < start:
                return HTTPStatus.BAD_REQUEST, {"Errors": ["endTime must be greater than or equal to startTime"]}

            items = []
            for timestamp in datetime_list(start, end, step):
                value = self.model.deterministic_value(attribute, timestamp)
                items.append({
                    "Timestamp": timestamp.astimezone(timezone.utc).isoformat().replace("+00:00", "Z"),
                    "Value": value,
                    "Good": True,
                    "Questionable": False,
                    "Substituted": False,
                })
            return HTTPStatus.OK, {"Items": items, "UnitsAbbreviation": attribute.units, "Total": len(items)}

        return HTTPStatus.NOT_FOUND, {"Errors": [f"Unsupported endpoint: {path}"]}

    def _batch_read_json(self) -> Tuple[bool, Any, str]:
        try:
            content_length = int(self.headers.get("Content-Length", "0"))
        except ValueError:
            return False, None, "Invalid Content-Length"
        raw = self.rfile.read(content_length) if content_length > 0 else b""
        if not raw:
            return False, None, "Request body is required"
        try:
            return True, json.loads(raw.decode("utf-8")), ""
        except Exception:
            return False, None, "Invalid JSON payload"

    def _normalize_resource(self, resource: str, batch_results: Dict[str, Any], batch: BatchExecutor) -> str:
        value = resource.strip()
        if value.startswith("$"):
            resolved = batch.parse_json_path(value, batch_results)
            if not resolved:
                raise ValueError(f"JsonPath resource did not resolve: {value}")
            value = str(resolved[0])
        parsed = urlparse(value)
        if parsed.scheme in ("http", "https"):
            expected_host = self.headers.get("Host", f"localhost:{self.server.server_port}")
            if parsed.netloc.lower() != expected_host.lower():
                raise ValueError("External hosts are not allowed in batch resource URLs")
            path = parsed.path
            if not path.startswith("/piwebapi"):
                raise ValueError("Absolute batch resource URL must target /piwebapi")
            return path + (f"?{parsed.query}" if parsed.query else "")
        if value.startswith("/"):
            return value
        if value.startswith("piwebapi"):
            return "/" + value
        return f"/piwebapi/{value.lstrip('/')}"

    def _execute_internal_request(self, method: str, resource: str, body: Any, batch_results: Dict[str, Any]):
        batch = BatchExecutor(self._execute_internal_request)
        norm = self._normalize_resource(resource, batch_results, batch)
        parsed = urlparse(norm)
        path = parsed.path.rstrip("/") or "/"
        query_case_insensitive = {key.lower(): value for key, value in parse_qs(parsed.query).items()}

        method = method.upper()
        if method == "GET":
            status, payload = self._handle_get_internal(path, query_case_insensitive)
            return int(status), {"Content-Type": "application/json"}, payload
        if method == "POST" and path == "/piwebapi/batch":
            return 400, {"Content-Type": "application/json"}, {"Errors": ["Nested batch requests are not supported"]}
        return 405, {"Content-Type": "application/json"}, {"Errors": [f"Method {method} is not supported by this mock"]}

    def do_GET(self) -> None:  # noqa: N802
        parsed = urlparse(self.path)
        path = parsed.path.rstrip("/") or "/"
        query_case_insensitive = {key.lower(): value for key, value in parse_qs(parsed.query).items()}

        if path.startswith("/piwebapi") and not self._auth_ok():
            self._unauthorized("Missing or invalid basic authentication credentials")
            return
        status, payload = self._handle_get_internal(path, query_case_insensitive)
        self._write_json(int(status), payload)

    def do_POST(self) -> None:  # noqa: N802
        parsed = urlparse(self.path)
        path = parsed.path.rstrip("/") or "/"
        if path.startswith("/piwebapi") and not self._auth_ok():
            self._unauthorized("Missing or invalid basic authentication credentials")
            return
        if path != "/piwebapi/batch":
            self._error(HTTPStatus.NOT_FOUND, f"Unsupported endpoint: {path}")
            return

        ok, payload, error = self._batch_read_json()
        if not ok:
            self._write_json(HTTPStatus.BAD_REQUEST, {"Errors": [error]})
            return
        if not isinstance(payload, dict):
            self._write_json(HTTPStatus.BAD_REQUEST, {"Errors": ["Batch payload must be a JSON object"]})
            return

        batch = BatchExecutor(self._execute_internal_request)
        try:
            status, out = batch.execute(payload)
        except ValueError as error:
            self._write_json(HTTPStatus.BAD_REQUEST, {"Errors": [str(error)]})
            return
        self._write_json(int(status), out)

    def log_message(self, fmt: str, *args) -> None:
        print(f"[{self.log_date_time_string()}] {self.address_string()} {fmt % args}")


def make_server(host: str, port: int, model: PiWebApiDataModel, users: Dict[str, str]) -> ThreadingHTTPServer:
    PiWebApiHandler.model = model
    PiWebApiHandler.users = users
    return ThreadingHTTPServer((host, port), PiWebApiHandler)
