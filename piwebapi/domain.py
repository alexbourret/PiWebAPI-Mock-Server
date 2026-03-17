from dataclasses import dataclass, field
from typing import List, Optional


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
