from collections import deque
from http import HTTPStatus
from typing import Any, Callable, Dict, List, Tuple
from .utils import case_insensitive_get, parse_json_path

ExecutorFn = Callable[[str, str, Any, Dict[str, Any]], Tuple[int, Dict[str, str], Any]]


class BatchExecutor:
    def __init__(self, execute_request: ExecutorFn) -> None:
        self.execute_request = execute_request

    def format_with_params(self, template: str, resolved_params: List[List[Any]], expand_index: int = 0) -> str:
        values: List[str] = []
        for vals in resolved_params:
            if not vals:
                raise ValueError("Parameter resolved to empty set")
            pick = vals[expand_index] if len(vals) > 1 else vals[0]
            values.append(str(pick))
        return template.format(*values)

    def batch_expand_count(self, resolved_params: List[List[Any]]) -> int:
        if not resolved_params:
            return 1
        max_len = max(len(resolved_param) for resolved_param in resolved_params)
        for vals in resolved_params:
            if len(vals) not in (1, max_len):
                raise ValueError("Parameter expansion size mismatch")
        return max_len

    def topological_order(self, requests: Dict[str, Dict[str, Any]]) -> List[str]:
        indegree: Dict[str, int] = {request_id: 0 for request_id in requests}
        edges: Dict[str, List[str]] = {request_id: [] for request_id in requests}
        for request_id, request in requests.items():
            for parent_id in request["parent_ids"]:
                if parent_id not in requests:
                    raise ValueError(f"Unknown ParentId '{parent_id}' referenced by '{request_id}'")
                edges[parent_id].append(request_id)
                indegree[request_id] += 1
        queue = deque([rid for rid, deg in indegree.items() if deg == 0])
        order: List[str] = []
        while queue:
            current_item = queue.popleft()
            order.append(current_item)
            for next_item in edges[current_item]:
                indegree[next_item] -= 1
                if indegree[next_item] == 0:
                    queue.append(next_item)
        if len(order) != len(requests):
            raise ValueError("Batch ParentIds contain a cycle")
        return order

    def normalize_batch_request(self, request_id: str, raw: Any) -> Dict[str, Any]:
        if not isinstance(raw, dict):
            raise ValueError(f"Batch item '{request_id}' must be an object")
        method = str(case_insensitive_get(raw, "Method", "GET")).upper()
        resource = case_insensitive_get(raw, "Resource")
        request_template = case_insensitive_get(raw, "RequestTemplate")
        parent_ids = case_insensitive_get(raw, "ParentIds", []) or []
        parameters = case_insensitive_get(raw, "Parameters", []) or []
        content = case_insensitive_get(raw, "Content")

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
            "parent_ids": [str(parent_id) for parent_id in parent_ids],
            "parameters": [str(parameter) for parameter in parameters],
            "content": content,
        }

    def execute(self, payload: Dict[str, Any]) -> Tuple[int, Dict[str, Any]]:
        requests: Dict[str, Dict[str, Any]] = {}
        for request_id, raw in payload.items():
            requests[str(request_id)] = self.normalize_batch_request(str(request_id), raw)

        order = self.topological_order(requests)
        batch_results: Dict[str, Any] = {}
        failed_deps: Dict[str, bool] = {}

        for request_id in order:
            request = requests[request_id]
            if any(failed_deps.get(pid, False) for pid in request["parent_ids"]):
                batch_results[request_id] = {
                    "Status": 424,
                    "Headers": {"Content-Type": "application/json"},
                    "Content": {"Errors": ["Failed dependency"]},
                }
                failed_deps[request_id] = True
                continue

            try:
                resolved_params = [parse_json_path(parameter, batch_results) for parameter in request["parameters"]]
                if request["request_template"] is not None:
                    template = request["request_template"]
                    template_resource = case_insensitive_get(template, "Resource")
                    if template_resource is None:
                        raise ValueError(f"RequestTemplate for '{request_id}' must include Resource")
                    template_content = case_insensitive_get(template, "Content", request["content"])
                    count = self.batch_expand_count(resolved_params)
                    sub_items = []
                    statuses = []
                    for index in range(count):
                        res = self.format_with_params(str(template_resource), resolved_params, index)
                        status, headers, content = self.execute_request(request["method"], res, template_content, batch_results)
                        sub_items.append({"Status": status, "Headers": headers, "Content": content})
                        statuses.append(status)
                    final_status = 400 if not sub_items else (statuses[0] if len(set(statuses)) == 1 else 207)
                    batch_results[request_id] = {
                        "Status": final_status,
                        "Headers": {"Content-Type": "application/json"},
                        "Content": {"Items": sub_items},
                    }
                else:
                    resource = str(request["resource"])
                    if resolved_params:
                        resource = self.format_with_params(resource, resolved_params, 0)
                    status, headers, content = self.execute_request(request["method"], resource, request["content"], batch_results)
                    batch_results[request_id] = {"Status": status, "Headers": headers, "Content": content}
                failed_deps[request_id] = int(batch_results[request_id]["Status"]) >= 400
            except Exception as error:
                batch_results[request_id] = {
                    "Status": 400,
                    "Headers": {"Content-Type": "application/json"},
                    "Content": {"Errors": [str(error)]},
                }
                failed_deps[request_id] = True

        return HTTPStatus.MULTI_STATUS, batch_results
