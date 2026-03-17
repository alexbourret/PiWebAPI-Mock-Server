from typing import Any, Dict

from .domain import AssetDatabase, Attribute, AttributeTemplate, Element, ElementTemplate
from .model import PiWebApiDataModel


class ApiSerializer:
    def __init__(self, model: PiWebApiDataModel, base_url: str) -> None:
        self.model = model
        self.base_url = base_url

    def db_item(self, db: AssetDatabase) -> Dict[str, Any]:
        return {
            "WebId": db.web_id,
            "Id": db.web_id,
            "Name": db.name,
            "Path": db.path,
            "Links": {
                "Self": f"{self.base_url}/assetdatabases/{db.web_id}",
                "Elements": f"{self.base_url}/assetdatabases/{db.web_id}/elements",
                "ElementTemplates": f"{self.base_url}/assetdatabases/{db.web_id}/elementtemplates",
            },
        }

    def asset_server_item(self, web_id: str, name: str) -> Dict[str, Any]:
        return {
            "WebId": web_id,
            "Id": web_id,
            "Name": name,
            "Path": f"\\{name}",
            "Links": {
                "Self": f"{self.base_url}/assetservers/{web_id}",
                "Databases": f"{self.base_url}/assetservers/{web_id}/assetdatabases",
            },
        }

    def element_item(self, elem: Element) -> Dict[str, Any]:
        item = {
            "WebId": elem.web_id,
            "Id": elem.web_id,
            "Name": elem.name,
            "Path": elem.path,
            "HasChildren": bool(elem.children),
            "Links": {
                "Self": f"{self.base_url}/elements/{elem.web_id}",
                "Elements": f"{self.base_url}/elements/{elem.web_id}/elements",
                "Attributes": f"{self.base_url}/elements/{elem.web_id}/attributes",
            },
        }
        if elem.template_web_id:
            tpl = self.model.get_element_template(elem.template_web_id)
            if tpl:
                item["TemplateName"] = tpl.name
                item["Links"]["Template"] = f"{self.base_url}/elementtemplates/{tpl.web_id}"
        return item

    def attribute_item(self, attr: Attribute) -> Dict[str, Any]:
        item = {
            "WebId": attr.web_id,
            "Id": attr.web_id,
            "Name": attr.name,
            "Path": attr.path,
            "Type": attr.data_type,
            "DefaultUnitsName": attr.units,
            "HasChildren": bool(attr.children),
            "Links": {
                "Self": f"{self.base_url}/attributes/{attr.web_id}",
                "Attributes": f"{self.base_url}/attributes/{attr.web_id}/attributes",
                "Value": f"{self.base_url}/streams/{attr.web_id}/value",
                "RecordedData": f"{self.base_url}/streams/{attr.web_id}/recorded",
            },
        }
        if attr.template_web_id:
            tpl = self.model.get_attribute_template(attr.template_web_id)
            if tpl:
                item["TemplateName"] = tpl.name
                item["Links"]["Template"] = f"{self.base_url}/attributetemplates/{tpl.web_id}"
        return item

    def element_template_item(self, tpl: ElementTemplate) -> Dict[str, Any]:
        item = {
            "WebId": tpl.web_id,
            "Id": tpl.web_id,
            "Name": tpl.name,
            "Path": tpl.path,
            "BaseTemplate": "",
            "Links": {
                "Self": f"{self.base_url}/elementtemplates/{tpl.web_id}",
                "AttributeTemplates": f"{self.base_url}/elementtemplates/{tpl.web_id}/attributetemplates",
                "ElementTemplates": f"{self.base_url}/elementtemplates",
            },
        }
        if tpl.base_template_web_id:
            base_tpl = self.model.get_element_template(tpl.base_template_web_id)
            if base_tpl:
                item["BaseTemplate"] = base_tpl.name
                item["BaseTemplateName"] = base_tpl.name
                item["Links"]["BaseTemplate"] = f"{self.base_url}/elementtemplates/{base_tpl.web_id}"
        return item

    def attribute_template_item(self, tpl: AttributeTemplate) -> Dict[str, Any]:
        return {
            "WebId": tpl.web_id,
            "Id": tpl.web_id,
            "Name": tpl.name,
            "Path": tpl.path,
            "Type": tpl.data_type,
            "DefaultUnitsName": tpl.units,
            "HasChildren": bool(tpl.children),
            "Links": {
                "Self": f"{self.base_url}/attributetemplates/{tpl.web_id}",
                "AttributeTemplates": f"{self.base_url}/attributetemplates/{tpl.web_id}/attributetemplates",
            },
        }
