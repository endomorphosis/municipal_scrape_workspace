// Minimal MCP-over-HTTP JSON-RPC client for ccindex dashboard.
// The dashboard uses this to call the server's MCP JSON-RPC endpoint.
//
// Endpoint:
//   POST /mcp  (JSON-RPC 2.0)
// Methods:
//   - tools/list
//   - tools/call  { name: string, arguments: object }

export class CcindexMcpClient {
  constructor({ endpoint } = {}) {
    this.endpoint = endpoint || defaultEndpoint();
    this._id = 1;
  }

  async _rpc(method, params) {
    const id = this._id++;
    const payload = { jsonrpc: "2.0", id, method, params };

    const resp = await fetch(this.endpoint, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify(payload),
    });

    const data = await resp.json();
    if (data && data.error) {
      const msg = data.error.message || "JSON-RPC error";
      const err = new Error(msg);
      err.data = data.error;
      throw err;
    }
    return data.result;
  }

  async listTools() {
    return this._rpc("tools/list", {});
  }

  async callTool(name, args = {}) {
    return this._rpc("tools/call", { name, arguments: args });
  }
}

function defaultEndpoint() {
  try {
    if (typeof window === "undefined") return "/mcp";

    const meta = document.querySelector("meta[name='ccindex-base-path']");
    const basePath = (meta && meta.content ? String(meta.content) : "").trim();
    const normalized = basePath && basePath !== "/" ? basePath.replace(/\/$/, "") : "";
    return `${normalized}/mcp`;
  } catch (_e) {
    return "/mcp";
  }
}

export const ccindexMcp = new CcindexMcpClient();
