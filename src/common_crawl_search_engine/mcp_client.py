from __future__ import annotations

import json
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Optional


class McpJsonRpcError(RuntimeError):
    def __init__(self, message: str, *, code: int | None = None, data: Any = None):
        super().__init__(message)
        self.code = code
        self.data = data


def normalize_mcp_endpoint(endpoint: str) -> str:
    s = str(endpoint or "").strip()
    if not s:
        raise ValueError("endpoint is required")

    # Support host:port shorthand (urlparse treats this as a scheme).
    if "://" not in s and not s.startswith("/"):
        s = "http://" + s

    # If the user passed a base URL, default to /mcp.
    parsed = urllib.parse.urlparse(s)
    if not parsed.scheme:
        # Allow /mcp-like relative endpoints.
        if s.startswith("/"):
            return s if s.endswith("/mcp") else s.rstrip("/") + "/mcp"
        raise ValueError(f"invalid endpoint: {endpoint}")

    if parsed.path.endswith("/mcp"):
        return urllib.parse.urlunparse(parsed)

    new_path = (parsed.path.rstrip("/") + "/mcp") if parsed.path else "/mcp"
    parsed = parsed._replace(path=new_path)
    return urllib.parse.urlunparse(parsed)


@dataclass
class McpCall:
    tool: str
    arguments: Dict[str, Any]


class CcindexMcpClient:
    """Minimal MCP-over-HTTP JSON-RPC client for the dashboard.

    Compatible with the FastAPI endpoint in common_crawl_search_engine.dashboard (POST /mcp).
    Uses only stdlib (urllib).
    """

    def __init__(
        self,
        *,
        endpoint: str,
        timeout_s: float = 30.0,
        headers: Optional[Dict[str, str]] = None,
    ) -> None:
        self.endpoint = normalize_mcp_endpoint(endpoint)
        self.timeout_s = float(timeout_s)
        self._headers = {"content-type": "application/json"}
        if headers:
            for k, v in headers.items():
                if v is None:
                    continue
                self._headers[str(k)] = str(v)
        self._id = 1

    def _next_id(self) -> int:
        i = self._id
        self._id += 1
        return i

    def _post_json(self, payload: Any) -> Any:
        data = json.dumps(payload).encode("utf-8")
        req = urllib.request.Request(self.endpoint, data=data, headers=self._headers, method="POST")
        try:
            with urllib.request.urlopen(req, timeout=self.timeout_s) as resp:
                raw = resp.read()
        except urllib.error.HTTPError as e:
            raw = e.read() if hasattr(e, "read") else b""
            raise McpJsonRpcError(
                f"HTTP {getattr(e, 'code', '?')}: {getattr(e, 'reason', 'error')}",
                code=int(getattr(e, "code", 0) or 0),
                data=(raw.decode("utf-8", errors="replace") if raw else None),
            )
        except Exception as e:
            raise McpJsonRpcError(f"request failed: {type(e).__name__}: {e}")

        if not raw:
            return None
        try:
            return json.loads(raw.decode("utf-8"))
        except Exception:
            raise McpJsonRpcError("non-JSON response", data=raw.decode("utf-8", errors="replace"))

    def list_tools(self) -> Dict[str, Any]:
        req_id = self._next_id()
        payload = {"jsonrpc": "2.0", "id": req_id, "method": "tools/list", "params": {}}
        resp = self._post_json(payload)
        if isinstance(resp, dict) and resp.get("error"):
            err = resp.get("error") or {}
            raise McpJsonRpcError(str(err.get("message") or "JSON-RPC error"), code=err.get("code"), data=err.get("data"))
        if not isinstance(resp, dict) or "result" not in resp:
            raise McpJsonRpcError("unexpected response")
        return resp["result"]

    def call_tool(self, name: str, arguments: Optional[Dict[str, Any]] = None) -> Any:
        req_id = self._next_id()
        payload = {
            "jsonrpc": "2.0",
            "id": req_id,
            "method": "tools/call",
            "params": {"name": str(name), "arguments": arguments or {}},
        }
        resp = self._post_json(payload)
        if isinstance(resp, dict) and resp.get("error"):
            err = resp.get("error") or {}
            raise McpJsonRpcError(str(err.get("message") or "JSON-RPC error"), code=err.get("code"), data=err.get("data"))
        if not isinstance(resp, dict) or "result" not in resp:
            raise McpJsonRpcError("unexpected response")
        return resp["result"]

    def batch_call(self, calls: Iterable[McpCall]) -> List[Any]:
        payload: List[Dict[str, Any]] = []
        for c in calls:
            req_id = self._next_id()
            payload.append(
                {
                    "jsonrpc": "2.0",
                    "id": req_id,
                    "method": "tools/call",
                    "params": {"name": str(c.tool), "arguments": dict(c.arguments or {})},
                }
            )

        resp = self._post_json(payload)
        if resp is None:
            return []
        if not isinstance(resp, list):
            raise McpJsonRpcError("unexpected batch response")

        by_id = {item.get("id"): item for item in resp if isinstance(item, dict)}
        out: List[Any] = []
        for item in payload:
            r = by_id.get(item["id"])
            if not isinstance(r, dict):
                out.append({"ok": False, "error": "missing response"})
                continue
            if r.get("error"):
                err = r.get("error") or {}
                out.append({"ok": False, "error": err.get("message"), "code": err.get("code"), "data": err.get("data")})
                continue
            out.append(r.get("result"))
        return out

    # ---- Convenience wrappers matching dashboard tools ----

    # Orchestrator settings
    def get_orchestrator_settings(self) -> Dict[str, Any]:
        r = self.call_tool("orchestrator_settings_get", {})
        return r if isinstance(r, dict) else {"ok": False, "error": "unexpected result"}

    def set_orchestrator_settings(self, settings: Dict[str, Any]) -> Dict[str, Any]:
        r = self.call_tool("orchestrator_settings_set", {"settings": settings or {}})
        return r if isinstance(r, dict) else {"ok": False, "error": "unexpected result"}

    # Collection catalog
    def collinfo_list(self, *, prefer_cache: bool = True) -> Dict[str, Any]:
        r = self.call_tool("cc_collinfo_list", {"prefer_cache": bool(prefer_cache)})
        return r if isinstance(r, dict) else {"ok": False, "error": "unexpected result"}

    def collinfo_update(self, *, url: str | None = None, timeout_s: float | None = None) -> Dict[str, Any]:
        args: Dict[str, Any] = {}
        if url is not None:
            args["url"] = str(url)
        if timeout_s is not None:
            args["timeout_s"] = float(timeout_s)
        r = self.call_tool("cc_collinfo_update", args)
        return r if isinstance(r, dict) else {"ok": False, "error": "unexpected result"}

    # Bulk operations
    def collections_status(self, collections: List[str], *, parallelism: int = 8) -> Dict[str, Any]:
        r = self.call_tool(
            "orchestrator_collections_status",
            {"collections": [str(c) for c in (collections or [])], "parallelism": int(parallelism)},
        )
        return r if isinstance(r, dict) else {"ok": False, "error": "unexpected result"}

    def delete_collection_indexes(self, collections: List[str]) -> Dict[str, Any]:
        r = self.call_tool("orchestrator_delete_collection_indexes", {"collections": [str(c) for c in (collections or [])]})
        return r if isinstance(r, dict) else {"ok": False, "error": "unexpected result"}

    # Jobs
    def jobs_list(self, *, limit: int = 50) -> Dict[str, Any]:
        r = self.call_tool("orchestrator_jobs_list", {"limit": int(limit)})
        return r if isinstance(r, dict) else {"ok": False, "error": "unexpected result"}

    def job_status(self, *, pid: int | None = None, log_path: str | None = None, lines: int = 200) -> Dict[str, Any]:
        r = self.call_tool(
            "orchestrator_job_status",
            {
                "pid": int(pid) if pid is not None else None,
                "log_path": str(log_path) if log_path is not None else None,
                "lines": int(lines),
            },
        )
        return r if isinstance(r, dict) else {"ok": False, "error": "unexpected result"}
