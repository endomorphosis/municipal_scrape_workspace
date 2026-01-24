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

  // ---- Orchestrator helpers (Index tab) ----
  async getOrchestratorSettings() {
    return this.callTool("orchestrator_settings_get", {});
  }

  async setOrchestratorSettings(settings) {
    return this.callTool("orchestrator_settings_set", { settings: settings || {} });
  }

  async orchestratorCollectionStatus(collection) {
    return this.callTool("orchestrator_collection_status", { collection });
  }

  async orchestratorDeleteCollectionIndex(collection) {
    return this.callTool("orchestrator_delete_collection_index", { collection });
  }

  async orchestratorJobPlan(params) {
    return this.callTool("orchestrator_job_plan", params || {});
  }

  async orchestratorJobStart(planned, { label } = {}) {
    return this.callTool("orchestrator_job_start", { planned, label: label || "orchestrator" });
  }

  async orchestratorJobTail(log_path, { lines } = {}) {
    return this.callTool("orchestrator_job_tail", { log_path, lines: lines || 200 });
  }

  async orchestratorJobStop(pid, { sig } = {}) {
    return this.callTool("orchestrator_job_stop", { pid, sig: sig || "TERM" });
  }

  // ---- Collection catalog (Common Crawl collinfo) ----
  async collinfoList({ prefer_cache } = {}) {
    return this.callTool("cc_collinfo_list", { prefer_cache: prefer_cache ?? true });
  }

  async collinfoUpdate({ url, timeout_s } = {}) {
    return this.callTool("cc_collinfo_update", {
      url: url ?? "https://index.commoncrawl.org/collinfo.json",
      timeout_s: timeout_s ?? 15.0,
    });
  }

  // ---- Bulk operations ----
  async orchestratorCollectionsStatus(collections, { parallelism } = {}) {
    return this.callTool("orchestrator_collections_status", {
      collections: collections || [],
      parallelism: parallelism ?? 8,
    });
  }

  async orchestratorDeleteCollectionIndexes(collections) {
    return this.callTool("orchestrator_delete_collection_indexes", { collections: collections || [] });
  }

  // ---- Jobs ----
  async orchestratorJobsList({ limit } = {}) {
    return this.callTool("orchestrator_jobs_list", { limit: limit ?? 50 });
  }

  async orchestratorJobStatus({ pid, log_path, lines } = {}) {
    return this.callTool("orchestrator_job_status", {
      pid: pid ?? null,
      log_path: log_path ?? null,
      lines: lines ?? 200,
    });
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
