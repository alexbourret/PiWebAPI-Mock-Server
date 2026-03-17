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
from typing import Any, Dict, Iterable, List, Optional, Tuple
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
    parent_attribute_web_id: Optional[str] = None
    children: List[str] = field(default_factory=list)
    template_web_id: Optional[str] = None


@dataclass
class Element:
    web_id: str
    name: str
    path: str
    database_web_id: str
    parent_web_id: Optional[str]
    children: List[str] = field(default_factory=list)
    attributes: List[str] = field(default_factory=list)
    template_web_id: Optional[str] = None


@dataclass
class AttributeTemplate:
    web_id: str
    name: str
    path: str
    element_template_web_id: str
    units: str
    data_type: str = "Double"
    parent_attribute_template_web_id: Optional[str] = None
    children: List[str] = field(default_factory=list)


@dataclass
class ElementTemplate:
    web_id: str
    name: str
    path: str
    base_template_web_id: Optional[str] = None
    attribute_templates: List[str] = field(default_factory=list)
    child_element_templates: List[str] = field(default_factory=list)


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
        self.element_templates_by_webid: Dict[str, ElementTemplate] = {}
        self.element_templates_by_name: Dict[str, ElementTemplate] = {}
        self.attribute_templates_by_webid: Dict[str, AttributeTemplate] = {}
        self.attribute_templates_by_path: Dict[str, AttributeTemplate] = {}
        self.element_to_template: Dict[str, str] = {}
        self.attribute_to_template: Dict[str, str] = {}

        self._create_template_catalog()

        for db_name in db_names:
            self._create_database(db_name)

    def _create_template_catalog(self) -> None:
        equipment = self._create_element_template("TPL_EquipmentBase")
        factory = self._create_element_template("TPL_FactoryRoot")
        area = self._create_element_template("TPL_Area")
        line = self._create_element_template("TPL_Line")
        unit = self._create_element_template("TPL_Unit")
        station = self._create_element_template("TPL_Station")
        cell = self._create_element_template("TPL_Cell", base_template_name="TPL_Station")

        self._create_attribute_template(equipment, "Status", "state", "Int32")
        self._create_attribute_template(equipment, "Power_kW", "kW", "Double")
        self._create_attribute_template(equipment, "Energy_kWh", "kWh", "Double")
        temp_tpl = self._create_attribute_template(equipment, "Temperature_C", "degC", "Double")
        self._create_attribute_template(
            equipment,
            "SensorOffset_C",
            "degC",
            "Double",
            parent_attribute_template_web_id=temp_tpl.web_id,
        )
        self._create_attribute_template(equipment, "UniqueTag", "id", "Double")

        self._create_attribute_template(cell, "Pressure_bar", "bar", "Double")
        self._create_attribute_template(cell, "Flow_m3_h", "m3/h", "Double")
        self._create_attribute_template(cell, "Vibration_mm_s", "mm/s", "Double")
        self._create_attribute_template(cell, "Output_pct", "%", "Double")
        self._create_attribute_template(cell, "QualityScore", "score", "Double")
        self._create_attribute_template(cell, "Setpoint", "unit", "Double")

        # Element template hierarchy hints for response completeness.
        equipment.child_element_templates.extend(
            [factory.web_id, area.web_id, line.web_id, unit.web_id]
        )
        unit.child_element_templates.append(station.web_id)
        station.child_element_templates.append(cell.web_id)

    def _create_element_template(
        self, name: str, base_template_name: Optional[str] = None
    ) -> ElementTemplate:
        base_tpl = self.element_templates_by_name.get(base_template_name.lower()) if base_template_name else None
        path = f"\\\\Templates\\Elements\\{name}"
        web_id = self._mk_web_id("elementtemplate", path)
        template = ElementTemplate(
            web_id=web_id,
            name=name,
            path=path,
            base_template_web_id=base_tpl.web_id if base_tpl else None,
        )
        self.element_templates_by_webid[web_id] = template
        self.element_templates_by_name[name.lower()] = template
        return template

    def _create_attribute_template(
        self,
        element_template: ElementTemplate,
        name: str,
        units: str,
        data_type: str,
        parent_attribute_template_web_id: Optional[str] = None,
    ) -> AttributeTemplate:
        if parent_attribute_template_web_id:
            parent = self.attribute_templates_by_webid[parent_attribute_template_web_id]
            path = f"{parent.path}|{name}"
        else:
            path = f"{element_template.path}|{name}"
        web_id = self._mk_web_id("attributetemplate", path)
        attr_tpl = AttributeTemplate(
            web_id=web_id,
            name=name,
            path=path,
            element_template_web_id=element_template.web_id,
            units=units,
            data_type=data_type,
            parent_attribute_template_web_id=parent_attribute_template_web_id,
        )
        self.attribute_templates_by_webid[web_id] = attr_tpl
        self.attribute_templates_by_path[path.lower()] = attr_tpl
        element_template.attribute_templates.append(web_id)
        if parent_attribute_template_web_id:
            self.attribute_templates_by_webid[parent_attribute_template_web_id].children.append(web_id)
        return attr_tpl

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
            template_web_id=self._resolve_element_template_for_name(root_name),
        )
        self.elements_by_webid[root_web_id] = root
        self.elements_by_path[(db_web_id, self._norm_path(root_path).lower())] = root
        if root.template_web_id:
            self.element_to_template[root.web_id] = root.template_web_id

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
            template_web_id=self._resolve_element_template_for_name(name),
        )
        self.elements_by_webid[child_web_id] = child
        self.elements_by_path[(db_web_id, self._norm_path(child_path).lower())] = child
        if child.template_web_id:
            self.element_to_template[child.web_id] = child.template_web_id
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
        created: Dict[str, Attribute] = {}
        for attr_name, unit in attrs:
            attribute = self._create_attribute(
                element=element,
                attr_name=attr_name,
                unit=unit,
                data_type="Int32" if attr_name == "Status" else "Double",
                parent_attribute_web_id=None,
            )
            created[attr_name] = attribute

        # Ensure at least one sub-attribute exists under regular attributes.
        temperature_attr = created.get("Temperature_C")
        if temperature_attr:
            self._create_attribute(
                element=element,
                attr_name="SensorOffset_C",
                unit="degC",
                data_type="Double",
                parent_attribute_web_id=temperature_attr.web_id,
            )

    def _create_attribute(
        self,
        element: Element,
        attr_name: str,
        unit: str,
        data_type: str,
        parent_attribute_web_id: Optional[str],
    ) -> Attribute:
        if parent_attribute_web_id:
            parent = self.get_attribute(parent_attribute_web_id)
            if not parent:
                raise ValueError("Parent attribute does not exist")
            attr_path = f"{parent.path}|{attr_name}"
        else:
            attr_path = f"{element.path}|{attr_name}"

        web_id = self._mk_web_id("attribute", attr_path)
        attribute = Attribute(
            web_id=web_id,
            name=attr_name,
            path=attr_path,
            element_web_id=element.web_id,
            units=unit,
            data_type=data_type,
            parent_attribute_web_id=parent_attribute_web_id,
            template_web_id=self._resolve_attribute_template_for_attribute(
                element=element,
                attr_name=attr_name,
                parent_attribute_web_id=parent_attribute_web_id,
            ),
        )
        self.attributes_by_webid[web_id] = attribute
        self.attributes_by_path[attr_path.lower()] = attribute
        if attribute.template_web_id:
            self.attribute_to_template[web_id] = attribute.template_web_id
        if parent_attribute_web_id:
            parent = self.get_attribute(parent_attribute_web_id)
            if parent:
                parent.children.append(web_id)
        else:
            element.attributes.append(web_id)
        return attribute

    def list_databases(self) -> List[AssetDatabase]:
        return list(self.databases_by_webid.values())

    def get_database(self, identifier: str) -> Optional[AssetDatabase]:
        return self.databases_by_webid.get(identifier) or self.databases_by_name.get(identifier.lower())

    def get_element(self, web_id: str) -> Optional[Element]:
        return self.elements_by_webid.get(web_id)

    def get_attribute(self, web_id: str) -> Optional[Attribute]:
        return self.attributes_by_webid.get(web_id)

    def list_element_templates(self) -> List[ElementTemplate]:
        return sorted(self.element_templates_by_webid.values(), key=lambda t: t.name)

    def get_element_template(self, web_id: str) -> Optional[ElementTemplate]:
        return self.element_templates_by_webid.get(web_id)

    def get_attribute_template(self, web_id: str) -> Optional[AttributeTemplate]:
        return self.attribute_templates_by_webid.get(web_id)

    def list_element_templates_for_database(self, db_web_id: str) -> List[ElementTemplate]:
        template_ids = set()
        for elem in self.elements_by_webid.values():
            if elem.database_web_id != db_web_id:
                continue
            if elem.template_web_id:
                template_ids.add(elem.template_web_id)
        return sorted(
            [self.element_templates_by_webid[tid] for tid in template_ids],
            key=lambda t: t.name,
        )

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

    def element_template_lineage(self, element_template_web_id: str) -> List[ElementTemplate]:
        lineage: List[ElementTemplate] = []
        cur = self.get_element_template(element_template_web_id)
        while cur is not None:
            lineage.append(cur)
            cur = self.get_element_template(cur.base_template_web_id) if cur.base_template_web_id else None
        return list(reversed(lineage))

    def effective_attribute_templates(self, element_template_web_id: str) -> List[AttributeTemplate]:
        seen: Dict[str, AttributeTemplate] = {}
        for et in self.element_template_lineage(element_template_web_id):
            for at_id in et.attribute_templates:
                at = self.get_attribute_template(at_id)
                if not at:
                    continue
                key = f"{at.parent_attribute_template_web_id or ''}|{at.name.lower()}"
                seen[key] = at
        return list(seen.values())

    def _resolve_element_template_for_name(self, element_name: str) -> Optional[str]:
        txt = element_name.lower()
        if txt.startswith("factory-"):
            return self.element_templates_by_name["tpl_factoryroot"].web_id
        if txt.startswith("area-"):
            return self.element_templates_by_name["tpl_area"].web_id
        if txt.startswith("line-"):
            return self.element_templates_by_name["tpl_line"].web_id
        if txt.startswith("unit-"):
            return self.element_templates_by_name["tpl_unit"].web_id
        if txt.startswith("station-"):
            return self.element_templates_by_name["tpl_station"].web_id
        if txt.startswith("cell-"):
            return self.element_templates_by_name["tpl_cell"].web_id
        return self.element_templates_by_name["tpl_equipmentbase"].web_id

    def _resolve_attribute_template_for_attribute(
        self,
        element: Element,
        attr_name: str,
        parent_attribute_web_id: Optional[str],
    ) -> Optional[str]:
        if not element.template_web_id:
            return None
        effective = self.effective_attribute_templates(element.template_web_id)
        target_name = "UniqueTag" if attr_name.startswith("UniqueTag_") else attr_name
        parent_template_web_id: Optional[str] = None
        if parent_attribute_web_id:
            parent_template_web_id = self.attribute_to_template.get(parent_attribute_web_id)
        for at in effective:
            if at.name != target_name:
                continue
            if at.parent_attribute_template_web_id != parent_template_web_id:
                continue
            return at.web_id
        return None

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
                "ElementTemplates": f"{base}/assetdatabases/{db.web_id}/elementtemplates",
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
        item = {
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
        if elem.template_web_id:
            tpl = self.model.get_element_template(elem.template_web_id)
            if tpl:
                item["TemplateName"] = tpl.name
                item["Links"]["Template"] = f"{base}/elementtemplates/{tpl.web_id}"
        return item

    def _attribute_item(self, attr: Attribute) -> dict:
        base = self._base_url()
        item = {
            "WebId": attr.web_id,
            "Id": attr.web_id,
            "Name": attr.name,
            "Path": attr.path,
            "Type": attr.data_type,
            "DefaultUnitsName": attr.units,
            "HasChildren": bool(attr.children),
            "Links": {
                "Self": f"{base}/attributes/{attr.web_id}",
                "Attributes": f"{base}/attributes/{attr.web_id}/attributes",
                "Value": f"{base}/streams/{attr.web_id}/value",
                "RecordedData": f"{base}/streams/{attr.web_id}/recorded",
            },
        }
        if attr.template_web_id:
            tpl = self.model.get_attribute_template(attr.template_web_id)
            if tpl:
                item["TemplateName"] = tpl.name
                item["Links"]["Template"] = f"{base}/attributetemplates/{tpl.web_id}"
        return item

    def _element_template_item(self, tpl: ElementTemplate) -> dict:
        base = self._base_url()
        item = {
            "WebId": tpl.web_id,
            "Id": tpl.web_id,
            "Name": tpl.name,
            "Path": tpl.path,
            "BaseTemplate": "",
            "Links": {
                "Self": f"{base}/elementtemplates/{tpl.web_id}",
                "AttributeTemplates": f"{base}/elementtemplates/{tpl.web_id}/attributetemplates",
                "ElementTemplates": f"{base}/elementtemplates",
            },
        }
        if tpl.base_template_web_id:
            base_tpl = self.model.get_element_template(tpl.base_template_web_id)
            if base_tpl:
                item["BaseTemplate"] = base_tpl.name
                item["BaseTemplateName"] = base_tpl.name
                item["Links"]["BaseTemplate"] = f"{base}/elementtemplates/{base_tpl.web_id}"
        return item

    def _attribute_template_item(self, tpl: AttributeTemplate) -> dict:
        base = self._base_url()
        return {
            "WebId": tpl.web_id,
            "Id": tpl.web_id,
            "Name": tpl.name,
            "Path": tpl.path,
            "Type": tpl.data_type,
            "DefaultUnitsName": tpl.units,
            "HasChildren": bool(tpl.children),
            "Links": {
                "Self": f"{base}/attributetemplates/{tpl.web_id}",
                "AttributeTemplates": f"{base}/attributetemplates/{tpl.web_id}/attributetemplates",
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

    def _handle_get_internal(self, path: str, query_ci: Dict[str, List[str]]) -> Tuple[int, Dict[str, Any]]:
        if path == "/" or path == "/piwebapi":
            return HTTPStatus.OK, {
                "ProductVersion": "Mock-1.0",
                "Links": {
                    "AssetServers": f"{self._base_url()}/assetservers",
                },
            }

        if path == "/piwebapi/assetservers":
            item = self._asset_server_item()
            return HTTPStatus.OK, {"Items": [item], "Total": 1}

        m = re.match(r"^/piwebapi/assetservers/([^/]+)$", path)
        if m:
            if m.group(1) != self.asset_server_web_id:
                return HTTPStatus.NOT_FOUND, {"Errors": ["Asset server not found"]}
            return HTTPStatus.OK, self._asset_server_item()

        m = re.match(r"^/piwebapi/assetservers/([^/]+)/assetdatabases$", path)
        if m:
            if m.group(1) != self.asset_server_web_id:
                return HTTPStatus.NOT_FOUND, {"Errors": ["Asset server not found"]}
            items = [self._db_item(db) for db in self.model.list_databases()]
            return HTTPStatus.OK, {"Items": items, "Total": len(items)}

        if path == "/piwebapi/assetdatabases":
            items = [self._db_item(db) for db in self.model.list_databases()]
            return HTTPStatus.OK, {"Items": items, "Total": len(items)}

        m = re.match(r"^/piwebapi/assetdatabases/([^/]+)$", path)
        if m:
            db = self.model.get_database(m.group(1))
            if not db:
                return HTTPStatus.NOT_FOUND, {"Errors": ["Asset database not found"]}
            return HTTPStatus.OK, self._db_item(db)

        m = re.match(r"^/piwebapi/assetdatabases/([^/]+)/elements$", path)
        if m:
            db = self.model.get_database(m.group(1))
            if not db:
                return HTTPStatus.NOT_FOUND, {"Errors": ["Asset database not found"]}

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

            return HTTPStatus.OK, {"Items": items, "Total": len(items)}

        m = re.match(r"^/piwebapi/assetdatabases/([^/]+)/elementtemplates$", path)
        if m:
            db = self.model.get_database(m.group(1))
            if not db:
                return HTTPStatus.NOT_FOUND, {"Errors": ["Asset database not found"]}
            items = [self._element_template_item(t) for t in self.model.list_element_templates_for_database(db.web_id)]
            return HTTPStatus.OK, {"Items": items, "Total": len(items)}

        if path == "/piwebapi/elementtemplates":
            items = [self._element_template_item(t) for t in self.model.list_element_templates()]
            return HTTPStatus.OK, {"Items": items, "Total": len(items)}

        m = re.match(r"^/piwebapi/elementtemplates/([^/]+)/attributetemplates$", path)
        if m:
            tpl = self.model.get_element_template(m.group(1))
            if not tpl:
                return HTTPStatus.NOT_FOUND, {"Errors": ["Element template not found"]}
            attr_tpls = self.model.effective_attribute_templates(tpl.web_id)
            items = [self._attribute_template_item(t) for t in attr_tpls]
            return HTTPStatus.OK, {"Items": items, "Total": len(items)}

        m = re.match(r"^/piwebapi/elementtemplates/([^/]+)$", path)
        if m:
            tpl = self.model.get_element_template(m.group(1))
            if not tpl:
                return HTTPStatus.NOT_FOUND, {"Errors": ["Element template not found"]}
            return HTTPStatus.OK, self._element_template_item(tpl)

        m = re.match(r"^/piwebapi/attributetemplates/([^/]+)/attributetemplates$", path)
        if m:
            tpl = self.model.get_attribute_template(m.group(1))
            if not tpl:
                return HTTPStatus.NOT_FOUND, {"Errors": ["Attribute template not found"]}
            items = [self._attribute_template_item(self.model.get_attribute_template(cid)) for cid in tpl.children]
            items = [it for it in items if it is not None]
            return HTTPStatus.OK, {"Items": items, "Total": len(items)}

        m = re.match(r"^/piwebapi/attributetemplates/([^/]+)$", path)
        if m:
            tpl = self.model.get_attribute_template(m.group(1))
            if not tpl:
                return HTTPStatus.NOT_FOUND, {"Errors": ["Attribute template not found"]}
            return HTTPStatus.OK, self._attribute_template_item(tpl)

        m = re.match(r"^/piwebapi/elements/([^/]+)$", path)
        if m:
            elem = self.model.get_element(m.group(1))
            if not elem:
                return HTTPStatus.NOT_FOUND, {"Errors": ["Element not found"]}
            return HTTPStatus.OK, self._element_item(elem)

        m = re.match(r"^/piwebapi/elements/([^/]+)/elements$", path)
        if m:
            elem = self.model.get_element(m.group(1))
            if not elem:
                return HTTPStatus.NOT_FOUND, {"Errors": ["Element not found"]}
            items = [self._element_item(self.model.get_element(cid)) for cid in elem.children]
            items = [it for it in items if it is not None]
            return HTTPStatus.OK, {"Items": items, "Total": len(items)}

        m = re.match(r"^/piwebapi/elements/([^/]+)/attributes$", path)
        if m:
            elem = self.model.get_element(m.group(1))
            if not elem:
                return HTTPStatus.NOT_FOUND, {"Errors": ["Element not found"]}
            items = [self._attribute_item(self.model.get_attribute(aid)) for aid in elem.attributes]
            items = [it for it in items if it is not None]
            return HTTPStatus.OK, {"Items": items, "Total": len(items)}

        if path == "/piwebapi/attributes/search":
            db_web_id = (query_ci.get("databasewebid", [None])[0] or "").strip()
            if not db_web_id:
                return HTTPStatus.BAD_REQUEST, {"Errors": ["Missing required parameter: databaseWebId"]}
            db = self.model.get_database(db_web_id)
            if not db:
                return HTTPStatus.NOT_FOUND, {"Errors": ["Asset database not found"]}

            search_expr = (query_ci.get("query", [""])[0] or "").strip()
            root_expr, name_pattern = self._parse_attribute_search_query(search_expr)
            root_elem = self._resolve_root_element_for_search(db, root_expr)
            if not root_elem:
                return HTTPStatus.OK, {"Items": [], "Total": 0}

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

            return HTTPStatus.OK, {"Items": items, "Total": len(items)}

        m = re.match(r"^/piwebapi/attributes/([^/]+)/attributes$", path)
        if m:
            attr = self.model.get_attribute(m.group(1))
            if not attr:
                return HTTPStatus.NOT_FOUND, {"Errors": ["Attribute not found"]}
            items = [self._attribute_item(self.model.get_attribute(cid)) for cid in attr.children]
            items = [it for it in items if it is not None]
            return HTTPStatus.OK, {"Items": items, "Total": len(items)}

        m = re.match(r"^/piwebapi/attributes/([^/]+)$", path)
        if m:
            attr = self.model.get_attribute(m.group(1))
            if not attr:
                return HTTPStatus.NOT_FOUND, {"Errors": ["Attribute not found"]}
            return HTTPStatus.OK, self._attribute_item(attr)

        m = re.match(r"^/piwebapi/streams/([^/]+)/value$", path)
        if m:
            attr = self.model.get_attribute(m.group(1))
            if not attr:
                return HTTPStatus.NOT_FOUND, {"Errors": ["Attribute not found"]}
            now = datetime.now(timezone.utc)
            try:
                t = parse_time(query_ci.get("time", [None])[0], now)
            except ValueError:
                return HTTPStatus.BAD_REQUEST, {"Errors": ["Invalid timestamp format for time. Expected ISO-8601."]}
            value = self.model.deterministic_value(attr, t)
            return HTTPStatus.OK, {
                "Timestamp": t.astimezone(timezone.utc).isoformat().replace("+00:00", "Z"),
                "Value": value,
                "Good": True,
                "Questionable": False,
                "Substituted": False,
            }

        m = re.match(r"^/piwebapi/streams/([^/]+)/recorded$", path)
        if m:
            attr = self.model.get_attribute(m.group(1))
            if not attr:
                return HTTPStatus.NOT_FOUND, {"Errors": ["Attribute not found"]}

            now = datetime.now(timezone.utc)
            try:
                start = parse_time(query_ci.get("starttime", [None])[0], now - timedelta(hours=8))
                end = parse_time(query_ci.get("endtime", [None])[0], now)
            except ValueError:
                return HTTPStatus.BAD_REQUEST, {
                    "Errors": ["Invalid timestamp format for startTime/endTime. Expected ISO-8601."]
                }
            step = parse_interval(query_ci.get("interval", [None])[0], fallback=timedelta(minutes=15))

            if end < start:
                return HTTPStatus.BAD_REQUEST, {"Errors": ["endTime must be greater than or equal to startTime"]}

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
            return HTTPStatus.OK, {
                "Items": items,
                "UnitsAbbreviation": attr.units,
                "Total": len(items),
            }

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

    def _ci_get(self, obj: Dict[str, Any], key: str, default: Any = None) -> Any:
        if key in obj:
            return obj[key]
        kl = key.lower()
        for k, v in obj.items():
            if k.lower() == kl:
                return v
        return default

    def _parse_json_path(self, expr: str, data: Any) -> List[Any]:
        if not expr.startswith("$"):
            raise ValueError("JsonPath must start with '$'")
        nodes = [data]
        i = 1
        n = len(expr)
        while i < n:
            ch = expr[i]
            if ch == ".":
                i += 1
                start = i
                while i < n and expr[i] not in ".[":
                    i += 1
                key = expr[start:i]
                if not key:
                    raise ValueError(f"Invalid JsonPath segment in {expr}")
                next_nodes: List[Any] = []
                for node in nodes:
                    if isinstance(node, dict) and key in node:
                        next_nodes.append(node[key])
                nodes = next_nodes
            elif ch == "[":
                end = expr.find("]", i)
                if end < 0:
                    raise ValueError(f"Unclosed bracket in JsonPath: {expr}")
                token = expr[i + 1 : end].strip()
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
                i = end + 1
            else:
                raise ValueError(f"Unexpected token '{ch}' in JsonPath: {expr}")
        return nodes

    def _format_with_params(self, template: str, resolved_params: List[List[Any]], expand_index: int = 0) -> str:
        values: List[str] = []
        for vals in resolved_params:
            if not vals:
                raise ValueError("Parameter resolved to empty set")
            pick = vals[expand_index] if len(vals) > 1 else vals[0]
            values.append(str(pick))
        return template.format(*values)

    def _batch_expand_count(self, resolved_params: List[List[Any]]) -> int:
        if not resolved_params:
            return 1
        max_len = max(len(v) for v in resolved_params)
        for vals in resolved_params:
            if len(vals) not in (1, max_len):
                raise ValueError("Parameter expansion size mismatch")
        return max_len

    def _normalize_resource(self, resource: str, batch_results: Dict[str, Any]) -> str:
        value = resource.strip()
        if value.startswith("$"):
            resolved = self._parse_json_path(value, batch_results)
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

    def _execute_internal_request(
        self, method: str, resource: str, body: Optional[Any], batch_results: Dict[str, Any]
    ) -> Tuple[int, Dict[str, str], Any]:
        norm = self._normalize_resource(resource, batch_results)
        parsed = urlparse(norm)
        path = parsed.path.rstrip("/") or "/"
        query = parse_qs(parsed.query)
        query_ci = {k.lower(): v for k, v in query.items()}

        m = method.upper()
        if m == "GET":
            status, payload = self._handle_get_internal(path, query_ci)
            return int(status), {"Content-Type": "application/json"}, payload
        if m == "POST" and path == "/piwebapi/batch":
            return 400, {"Content-Type": "application/json"}, {"Errors": ["Nested batch requests are not supported"]}
        return 405, {"Content-Type": "application/json"}, {"Errors": [f"Method {m} is not supported by this mock"]}

    def _topological_order(self, requests: Dict[str, Dict[str, Any]]) -> List[str]:
        indegree: Dict[str, int] = {rid: 0 for rid in requests}
        edges: Dict[str, List[str]] = {rid: [] for rid in requests}
        for rid, req in requests.items():
            for pid in req["parent_ids"]:
                if pid not in requests:
                    raise ValueError(f"Unknown ParentId '{pid}' referenced by '{rid}'")
                edges[pid].append(rid)
                indegree[rid] += 1
        queue = [rid for rid, deg in indegree.items() if deg == 0]
        order: List[str] = []
        while queue:
            cur = queue.pop(0)
            order.append(cur)
            for nxt in edges[cur]:
                indegree[nxt] -= 1
                if indegree[nxt] == 0:
                    queue.append(nxt)
        if len(order) != len(requests):
            raise ValueError("Batch ParentIds contain a cycle")
        return order

    def _normalize_batch_request(self, request_id: str, raw: Any) -> Dict[str, Any]:
        if not isinstance(raw, dict):
            raise ValueError(f"Batch item '{request_id}' must be an object")
        method = str(self._ci_get(raw, "Method", "GET")).upper()
        resource = self._ci_get(raw, "Resource")
        request_template = self._ci_get(raw, "RequestTemplate")
        parent_ids = self._ci_get(raw, "ParentIds", []) or []
        parameters = self._ci_get(raw, "Parameters", []) or []
        content = self._ci_get(raw, "Content")

        if not isinstance(parent_ids, list):
            raise ValueError(f"ParentIds for '{request_id}' must be an array")
        if not isinstance(parameters, list):
            raise ValueError(f"Parameters for '{request_id}' must be an array")
        if resource is None and request_template is None:
            raise ValueError(f"Batch item '{request_id}' must define Resource or RequestTemplate")
        if request_template is not None and not isinstance(request_template, dict):
            raise ValueError(f"RequestTemplate for '{request_id}' must be an object")
        return {
            "method": method,
            "resource": resource,
            "request_template": request_template,
            "parent_ids": [str(x) for x in parent_ids],
            "parameters": [str(x) for x in parameters],
            "content": content,
        }

    def _execute_batch(self, payload: Dict[str, Any]) -> Tuple[int, Dict[str, Any]]:
        requests: Dict[str, Dict[str, Any]] = {}
        for rid, raw in payload.items():
            requests[str(rid)] = self._normalize_batch_request(str(rid), raw)

        order = self._topological_order(requests)
        batch_results: Dict[str, Any] = {}
        failed_deps: Dict[str, bool] = {}

        for rid in order:
            req = requests[rid]
            if any(failed_deps.get(pid, False) for pid in req["parent_ids"]):
                batch_results[rid] = {
                    "Status": 424,
                    "Headers": {"Content-Type": "application/json"},
                    "Content": {"Errors": ["Failed dependency"]},
                }
                failed_deps[rid] = True
                continue

            try:
                resolved_params = [self._parse_json_path(p, batch_results) for p in req["parameters"]]
                if req["request_template"] is not None:
                    tpl = req["request_template"]
                    tpl_resource = self._ci_get(tpl, "Resource")
                    if tpl_resource is None:
                        raise ValueError(f"RequestTemplate for '{rid}' must include Resource")
                    tpl_content = self._ci_get(tpl, "Content", req["content"])
                    count = self._batch_expand_count(resolved_params)
                    sub_items = []
                    statuses = []
                    for idx in range(count):
                        res = self._format_with_params(str(tpl_resource), resolved_params, idx)
                        st, hdr, ctt = self._execute_internal_request(req["method"], res, tpl_content, batch_results)
                        sub_items.append({"Status": st, "Headers": hdr, "Content": ctt})
                        statuses.append(st)
                    if not sub_items:
                        final_status = 400
                    elif len(set(statuses)) == 1:
                        final_status = statuses[0]
                    else:
                        final_status = 207
                    batch_results[rid] = {
                        "Status": final_status,
                        "Headers": {"Content-Type": "application/json"},
                        "Content": {"Items": sub_items},
                    }
                else:
                    resource = str(req["resource"])
                    if resolved_params:
                        resource = self._format_with_params(resource, resolved_params, 0)
                    st, hdr, ctt = self._execute_internal_request(req["method"], resource, req["content"], batch_results)
                    batch_results[rid] = {"Status": st, "Headers": hdr, "Content": ctt}
                failed_deps[rid] = int(batch_results[rid]["Status"]) >= 400
            except Exception as e:
                batch_results[rid] = {
                    "Status": 400,
                    "Headers": {"Content-Type": "application/json"},
                    "Content": {"Errors": [str(e)]},
                }
                failed_deps[rid] = True

        return HTTPStatus.MULTI_STATUS, batch_results

    def do_GET(self) -> None:  # noqa: N802
        parsed = urlparse(self.path)
        path = parsed.path.rstrip("/") or "/"
        query = parse_qs(parsed.query)
        query_ci = {k.lower(): v for k, v in query.items()}

        if path.startswith("/piwebapi") and not self._auth_ok():
            self._unauthorized("Missing or invalid basic authentication credentials")
            return
        status, payload = self._handle_get_internal(path, query_ci)
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
        ok, payload, err = self._batch_read_json()
        if not ok:
            self._write_json(HTTPStatus.BAD_REQUEST, {"Errors": [err]})
            return
        if not isinstance(payload, dict):
            self._write_json(HTTPStatus.BAD_REQUEST, {"Errors": ["Batch payload must be a JSON object"]})
            return
        try:
            status, out = self._execute_batch(payload)
        except ValueError as e:
            self._write_json(HTTPStatus.BAD_REQUEST, {"Errors": [str(e)]})
            return
        self._write_json(int(status), out)

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
