import hashlib
import re
from datetime import timezone
from typing import Dict, List, Optional, Tuple

from .domain import AssetDatabase, Attribute, AttributeTemplate, Element, ElementTemplate


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

        equipment.child_element_templates.extend([factory.web_id, area.web_id, line.web_id, unit.web_id])
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

    def _create_database(self, db_name: str) -> None:
        database_path = f"\\\\MockServer\\{db_name}"
        database_web_id = self._mk_web_id("database", database_path)

        root_name = db_name
        root_path = f"\\\\{db_name}\\{root_name}"
        root_web_id = self._mk_web_id("element", root_path)

        db = AssetDatabase(web_id=database_web_id, name=db_name, path=database_path, root_element_web_id=root_web_id)
        self.databases_by_webid[database_web_id] = db
        self.databases_by_name[db_name.lower()] = db

        root = Element(
            web_id=root_web_id,
            name=root_name,
            path=root_path,
            database_web_id=database_web_id,
            parent_web_id=None,
            template_web_id=self._resolve_element_template_for_name(root_name),
        )
        self.elements_by_webid[root_web_id] = root
        self.elements_by_path[(database_web_id, self._norm_path(root_path).lower())] = root
        if root.template_web_id:
            self.element_to_template[root.web_id] = root.template_web_id

        self._attach_attributes(root, is_leaf=False)

        for area_index in range(1, 9):
            area = self._create_child(root, database_web_id, f"Area-{area_index:02d}")
            for line_index in range(1, 6):
                line = self._create_child(area, database_web_id, f"Line-{line_index:02d}")
                for unit_index in range(1, 5):
                    unit = self._create_child(line, database_web_id, f"Unit-{unit_index:02d}")
                    for station_index in range(1, 4):
                        station = self._create_child(unit, database_web_id, f"Station-{station_index:02d}")
                        for cell_index in range(1, 3):
                            leaf = self._create_child(station, database_web_id, f"Cell-{cell_index:02d}")
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

    def get_element_template(self, web_id: Optional[str]) -> Optional[ElementTemplate]:
        return self.element_templates_by_webid.get(web_id) if web_id else None

    def get_attribute_template(self, web_id: Optional[str]) -> Optional[AttributeTemplate]:
        return self.attribute_templates_by_webid.get(web_id) if web_id else None

    def list_element_templates_for_database(self, db_web_id: str) -> List[ElementTemplate]:
        template_ids = set()
        for elem in self.elements_by_webid.values():
            if elem.database_web_id != db_web_id:
                continue
            if elem.template_web_id:
                template_ids.add(elem.template_web_id)
        return sorted([self.element_templates_by_webid[tid] for tid in template_ids], key=lambda t: t.name)

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

    # def deterministic_value(self, attribute: Attribute, timestamp: datetime) -> float | int:
    def deterministic_value(self, attribute, timestamp):
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


def build_default_model(seed: str = "piwebapi-mock-seed") -> PiWebApiDataModel:
    db_names = ["Factory-North", "Factory-South", "Factory-West"]
    return PiWebApiDataModel(db_names=db_names, seed=seed)
