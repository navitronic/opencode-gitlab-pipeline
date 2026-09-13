// @bun
// src/index.ts
import { resolve as resolve4 } from "path";
import { tool } from "@opencode-ai/plugin";

// src/delivery.ts
class DeliveryQueue {
  client;
  directory;
  entries = new Map;
  flights = new Map;
  timers = new Map;
  sessions = new Map;
  resources = new Map;
  busy = new Set;
  tools = new Map;
  disposed = false;
  constructor(client, directory) {
    this.client = client;
    this.directory = directory;
  }
  enqueue(result, acknowledged = Promise.resolve()) {
    if (this.disposed)
      throw new Error("Delivery queue disposed");
    const existing = this.entries.get(result.watchID);
    if (existing)
      return existing.messageID;
    if (new TextEncoder().encode(result.text).byteLength > 4096)
      throw new Error("Completion exceeds 4 KiB");
    if (this.entries.size >= 256) {
      const oldest = [...this.entries].find(([, entry2]) => entry2.state === "delivered");
      if (!oldest)
        throw new Error("Delivery queue full");
      this.entries.delete(oldest[0]);
      const sessionID = oldest[1].result.sessionID;
      if (![...this.entries.values()].some((entry2) => entry2.result.sessionID === sessionID))
        this.sessions.delete(sessionID);
    }
    const entry = {
      result: { ...result, model: result.model && { ...result.model } },
      messageID: `msg_${BigInt.asUintN(48, BigInt(Date.now()) * 0x1000n).toString(16).padStart(12, "0")}${crypto.randomUUID().replaceAll("-", "").slice(0, 14)}`,
      state: "pending",
      ready: false,
      attempts: 0
    };
    this.entries.set(result.watchID, entry);
    if (!this.sessions.has(result.sessionID))
      this.sessions.set(result.sessionID, new AbortController);
    acknowledged.then(() => {
      if (this.disposed || this.entries.get(result.watchID) !== entry)
        return;
      entry.ready = true;
      (this.flights.get(result.sessionID) ?? Promise.resolve()).then(() => this.flush(result.sessionID));
    }, () => {
      this.entries.delete(result.watchID);
    });
    return entry.messageID;
  }
  inspect(watchID) {
    const entry = this.entries.get(watchID);
    return entry && {
      messageID: entry.messageID,
      state: entry.state,
      attempts: entry.attempts
    };
  }
  own(sessionID, release) {
    if (this.disposed) {
      release();
      return () => {};
    }
    const owned = this.resources.get(sessionID) ?? new Set;
    owned.add(release);
    this.resources.set(sessionID, owned);
    return () => {
      owned.delete(release);
      if (!owned.size)
        this.resources.delete(sessionID);
    };
  }
  async flush(sessionID) {
    await Promise.resolve();
    if (this.disposed)
      return;
    const running = this.flights.get(sessionID);
    if (running)
      return running;
    const flight = this.drain(sessionID).finally(() => {
      this.flights.delete(sessionID);
    });
    this.flights.set(sessionID, flight);
    return flight;
  }
  async drain(sessionID) {
    const signal = this.sessions.get(sessionID)?.signal;
    if (!signal || signal.aborted)
      return;
    const query = { directory: this.directory };
    for (const entry of this.entries.values()) {
      if (entry.result.sessionID !== sessionID || !entry.ready || entry.state === "delivered")
        continue;
      if (signal.aborted || this.disposed)
        return;
      if (entry.state === "delivery_uncertain") {
        try {
          const response = await this.client.session.message({
            path: { id: sessionID, messageID: entry.messageID },
            query,
            signal
          });
          if (!signal.aborted && response.data?.info.id === entry.messageID)
            entry.state = "delivered";
        } catch {}
        continue;
      }
      if (this.tools.get(sessionID)?.size)
        return;
      this.busy.delete(sessionID);
      try {
        const status = await this.client.session.status({
          query,
          signal
        });
        if (signal.aborted || this.disposed)
          return;
        if (!status.data) {
          this.schedule(sessionID);
          return;
        }
        if ((status.data[sessionID]?.type ?? "idle") !== "idle" || this.busy.has(sessionID) || this.tools.get(sessionID)?.size)
          return;
      } catch {
        if (!signal.aborted)
          this.schedule(sessionID);
        return;
      }
      entry.state = "delivering";
      entry.attempts++;
      try {
        const response = await this.client.session.promptAsync({
          path: { id: sessionID },
          query,
          signal,
          body: {
            messageID: entry.messageID,
            agent: entry.result.agent,
            model: entry.result.model,
            parts: [
              {
                type: "text",
                text: entry.result.text,
                synthetic: true
              }
            ]
          }
        });
        if (signal.aborted || this.disposed)
          return;
        if (response.response.status === 204)
          entry.state = "delivered";
        else if ([400, 401, 403, 404, 429].includes(response.response.status))
          entry.state = "pending";
        else
          entry.state = "delivery_uncertain";
      } catch {
        entry.state = "delivery_uncertain";
      }
      if (entry.state !== "delivered" && entry.attempts < 3 && !signal.aborted)
        this.schedule(sessionID);
      return;
    }
  }
  schedule(sessionID) {
    if (this.disposed || this.timers.has(sessionID))
      return;
    const timer = setTimeout(() => {
      this.timers.delete(sessionID);
      this.flush(sessionID);
    }, 1000);
    timer.unref();
    this.timers.set(sessionID, timer);
  }
  deleteSession(sessionID) {
    this.sessions.get(sessionID)?.abort();
    this.sessions.delete(sessionID);
    clearTimeout(this.timers.get(sessionID));
    this.timers.delete(sessionID);
    this.busy.delete(sessionID);
    this.tools.delete(sessionID);
    for (const [id, entry] of this.entries)
      if (entry.result.sessionID === sessionID)
        this.entries.delete(id);
    const owned = this.resources.get(sessionID);
    this.resources.delete(sessionID);
    for (const release of owned ?? []) {
      try {
        release();
      } catch {}
    }
  }
  dispose() {
    if (this.disposed)
      return;
    this.disposed = true;
    for (const id of new Set([
      ...this.sessions.keys(),
      ...this.resources.keys(),
      ...this.tools.keys(),
      ...this.busy
    ]))
      this.deleteSession(id);
  }
  hooks = {
    event: async ({ event }) => {
      if (this.disposed)
        return;
      if (event.type === "session.deleted")
        this.deleteSession(event.properties.info.id);
      if (event.type === "session.status") {
        const { sessionID, status } = event.properties;
        if (status.type === "idle") {
          this.busy.delete(sessionID);
          this.tools.delete(sessionID);
          (this.flights.get(sessionID) ?? Promise.resolve()).then(() => this.flush(sessionID));
        } else
          this.busy.add(sessionID);
      }
    },
    "tool.execute.before": async ({ sessionID, callID }) => {
      if (this.disposed)
        return;
      const calls = this.tools.get(sessionID) ?? new Set;
      calls.add(callID);
      this.tools.set(sessionID, calls);
    },
    "tool.execute.after": async ({ sessionID, callID }) => {
      this.tools.get(sessionID)?.delete(callID);
      if (!this.tools.get(sessionID)?.size)
        this.tools.delete(sessionID);
    },
    dispose: async () => {
      this.dispose();
    }
  };
}

// src/gitlab.ts
import { stripVTControlCharacters } from "util";
import { resolve as resolve2 } from "path";

// src/process.ts
import { spawn } from "child_process";
import { realpath } from "fs/promises";
import { isAbsolute, join, relative, resolve } from "path";
var PROCESS_LIMITS = {
  bytes: 1024 * 1024,
  timeoutMs: 30000,
  maxTimeoutMs: 30 * 60 * 1000
};
async function askPermission(context, input, signal) {
  if (signal.aborted)
    return false;
  return new Promise((resolve2, reject) => {
    const abort = () => resolve2(false);
    signal.addEventListener("abort", abort, { once: true });
    context.ask(input).then(() => resolve2(!signal.aborted), reject).finally(() => signal.removeEventListener("abort", abort));
  });
}
var runProcess = async (request, context) => {
  request = {
    ...request,
    args: [...request.args],
    env: request.env && { ...request.env },
    stdin: typeof request.stdin === "object" ? Buffer.from(request.stdin) : request.stdin
  };
  const started = Date.now();
  const signal = AbortSignal.any([
    context.abort,
    ...request.signal ? [request.signal] : []
  ]);
  const result = {
    status: "canceled",
    exitCode: null,
    signal: null,
    stdout: Buffer.alloc(0),
    stderr: Buffer.alloc(0),
    stdoutBytes: 0,
    stderrBytes: 0,
    durationMs: 0
  };
  if (signal.aborted)
    return result;
  const maxBytes = request.maxBytes ?? PROCESS_LIMITS.bytes;
  const timeoutMs = request.timeoutMs ?? PROCESS_LIMITS.timeoutMs;
  if (!Number.isInteger(maxBytes) || maxBytes < 0 || maxBytes > PROCESS_LIMITS.bytes)
    throw new Error("maxBytes must be between 0 and 1 MiB");
  if (!Number.isInteger(timeoutMs) || timeoutMs < 1 || timeoutMs > PROCESS_LIMITS.maxTimeoutMs)
    throw new Error("timeoutMs must be between 1 and 1800000");
  if (!request.executable || [request.executable, ...request.args].some((arg) => arg.includes("\x00")))
    throw new Error("Executable and argv must not contain NUL");
  if (Buffer.byteLength(request.args.join("")) + Buffer.byteLength(request.stdin ?? "") > PROCESS_LIMITS.bytes)
    throw new Error("Arguments and stdin exceed 1 MiB");
  if (process.platform === "win32")
    throw new Error("Process-group cleanup requires POSIX");
  const cwd = await realpath(resolve(context.directory, request.cwd ?? "."));
  const worktree = await realpath(context.worktree);
  const outside = relative(worktree, cwd);
  if (outside === ".." || outside.startsWith("../") || isAbsolute(outside)) {
    if (!await askPermission(context, {
      permission: "external_directory",
      patterns: [cwd, join(cwd, "*")],
      always: [],
      metadata: { cwd }
    }, signal))
      return result;
  }
  const argv = [request.executable, ...request.args];
  const command = argv.map((arg) => /^[\w./:@%+=,-]+$/.test(arg) ? arg : "'" + arg.replaceAll("'", "'\\''") + "'").join(" ");
  if (!await askPermission(context, {
    permission: "bash",
    patterns: [...new Set([argv.join(" "), command])],
    always: [],
    metadata: {
      command,
      executable: request.executable,
      args: [...request.args],
      cwd,
      env: { ...request.env },
      stdin: typeof request.stdin === "string" ? request.stdin : undefined,
      stdinBytes: Buffer.byteLength(request.stdin ?? "")
    }
  }, signal))
    return result;
  if (signal.aborted)
    return {
      ...result,
      status: "canceled",
      durationMs: Date.now() - started
    };
  result.status = "success";
  return new Promise((finish) => {
    const stdout = Buffer.alloc(Math.floor(maxBytes * 3 / 4));
    const stderr = Buffer.alloc(maxBytes - stdout.length);
    const retained = { stdout: 0, stderr: 0 };
    const child = spawn(request.executable, [...request.args], {
      cwd,
      env: { ...process.env, ...request.env },
      shell: false,
      detached: true,
      stdio: [
        request.stdin === undefined ? "ignore" : "pipe",
        "pipe",
        "pipe"
      ]
    });
    const kill = () => {
      if (!child.pid)
        return;
      try {
        process.kill(-child.pid, "SIGKILL");
      } catch {
        child.kill("SIGKILL");
      }
    };
    const cancel = () => {
      result.status = "canceled";
      kill();
    };
    signal.addEventListener("abort", cancel, { once: true });
    const timer = setTimeout(() => {
      result.status = "timeout";
      kill();
    }, timeoutMs);
    const capture = (stream, chunk) => {
      result[stream === "stdout" ? "stdoutBytes" : "stderrBytes"] += chunk.length;
      const buffer = stream === "stdout" ? stdout : stderr;
      retained[stream] += chunk.copy(buffer, retained[stream]);
    };
    child.stdout?.on("data", (chunk) => capture("stdout", chunk));
    child.stderr?.on("data", (chunk) => capture("stderr", chunk));
    child.on("error", (error) => {
      result.status = error.code === "ENOENT" ? "not_found" : "spawn_error";
    });
    child.once("exit", kill);
    child.once("close", (code, exitSignal) => {
      clearTimeout(timer);
      signal.removeEventListener("abort", cancel);
      result.exitCode = code;
      result.signal = exitSignal;
      if (result.status === "success" && code !== 0)
        result.status = "exit_error";
      result.stdout = Buffer.from(stdout.subarray(0, retained.stdout));
      result.stderr = Buffer.from(stderr.subarray(0, retained.stderr));
      result.durationMs = Date.now() - started;
      finish(result);
    });
    child.stdin?.on("error", () => {});
    child.stdin?.end(request.stdin);
    if (signal.aborted)
      cancel();
  });
};

// src/gitlab.ts
class GitLabCommandError extends Error {
  result;
  constructor(result) {
    super(`Command ${result.error ?? result.status}: ${stripVTControlCharacters(result.stderr.toString()).slice(0, 512)}`);
    this.result = result;
    this.name = "GitLabCommandError";
  }
}
function commandText(result) {
  if (result.status !== "success")
    throw new GitLabCommandError(result);
  if (result.stdoutBytes !== result.stdout.length)
    throw new Error("Command response is truncated");
  return new TextDecoder("utf-8", { fatal: true }).decode(result.stdout).trim();
}
function numericID(value) {
  if (typeof value !== "string" && typeof value !== "number" || typeof value === "number" && !Number.isSafeInteger(value) || !/^\d{1,20}$/.test(String(value)) || BigInt(value) < 1n)
    throw new Error("ID must be a positive integer");
  return BigInt(value).toString();
}
function projectIdentity(host, repo) {
  if (!host || !/^[a-zA-Z0-9.-]+(?::\d+)?$/.test(host) || !repo || !/^[\w.-]+(?:\/[\w.-]+)+$/.test(repo) || repo.split("/").some((part) => part === "." || part === "..") || host.length + repo.length > 512)
    throw new Error("Supply an explicit host and namespace/project repo context");
  return { host: host.toLowerCase(), repo };
}
function remoteIdentity(remote) {
  const scp = remote.match(/^(?:[^/@:]+@)?([^/:]+):([^/].*)$/);
  if (scp && !remote.includes("://"))
    return projectIdentity(scp[1], scp[2]?.replace(/\.git$/, ""));
  const url = new URL(remote);
  if (!["https:", "http:", "ssh:"].includes(url.protocol) || url.search || url.hash)
    throw new Error("Remote must identify a GitLab host and project");
  return projectIdentity(url.protocol === "ssh:" ? url.hostname : url.host, decodeURIComponent(url.pathname.slice(1)).replace(/\.git$/, ""));
}
function normalizeTarget(input) {
  if (input.url !== undefined) {
    if (input.kind !== undefined || input.id !== undefined || input.host !== undefined || input.repo !== undefined)
      throw new Error("Supply a URL or kind/ID/repo context, not both");
    const url = new URL(input.url);
    const match = url.pathname.match(/^\/(.+)\/-\/(pipelines|jobs)\/(\d+)\/?$/);
    if (!["http:", "https:"].includes(url.protocol) || url.username || url.password || !match)
      throw new Error("Expected a GitLab pipeline or job URL");
    const identity2 = projectIdentity(url.host, decodeURIComponent(match[1]));
    const id2 = numericID(match[3]);
    return {
      ...identity2,
      kind: match[2] === "jobs" ? "job" : "pipeline",
      id: id2,
      url: `${url.protocol}//${identity2.host}/${identity2.repo}/-/${match[2]}/${id2}`
    };
  }
  if (input.kind !== "pipeline" && input.kind !== "job")
    throw new Error("Supply target kind pipeline or job");
  const identity = projectIdentity(input.host, input.repo);
  const id = numericID(input.id);
  return {
    ...identity,
    kind: input.kind,
    id,
    url: `https://${identity.host}/${identity.repo}/-/${input.kind === "job" ? "jobs" : "pipelines"}/${id}`
  };
}

class GitLabClient {
  runner;
  constructor(runner = runProcess) {
    this.runner = runner;
  }
  async git(args, input, context) {
    return this.runner({
      executable: "git",
      args,
      cwd: input.cwd,
      signal: input.signal,
      env: {
        GIT_TERMINAL_PROMPT: "0",
        GIT_ASKPASS: "/usr/bin/false",
        SSH_ASKPASS: "/usr/bin/false",
        GIT_EDITOR: "/usr/bin/false",
        GIT_PAGER: "/bin/cat"
      }
    }, context);
  }
  async resolveTarget(input, context) {
    if (input.url !== undefined || input.host !== undefined || input.repo !== undefined)
      return normalizeTarget(input);
    normalizeTarget({
      ...input,
      host: "context.test",
      repo: "context/project"
    });
    const remote = commandText(await this.git(["remote", "get-url", "--", input.remote ?? "origin"], input, context));
    return normalizeTarget({ ...input, ...remoteIdentity(remote) });
  }
  async inspect(input, context) {
    const target = await this.resolveTarget(input, context);
    const response = await this.execute({
      args: [
        "api",
        `projects/${encodeURIComponent(target.repo)}/${target.kind === "job" ? "jobs" : "pipelines"}/${target.id}`,
        "--method",
        "GET"
      ],
      host: target.host,
      repo: target.repo,
      cwd: input.cwd,
      signal: input.signal,
      timeoutMs: input.timeoutMs
    }, context);
    const text = commandText(response);
    let data;
    try {
      data = JSON.parse(text);
    } catch (cause) {
      throw new Error("Invalid JSON response from GitLab", { cause });
    }
    if (!data || typeof data !== "object" || Array.isArray(data))
      throw new Error("Invalid GitLab response shape");
    const record = data;
    const commit = record.commit;
    const sha = target.kind === "pipeline" ? record.sha : commit && typeof commit === "object" && ("id" in commit) ? commit.id : undefined;
    if (record.id !== Number(target.id) || !Number.isSafeInteger(record.id) || typeof record.status !== "string" || !record.status.length || record.status.length > 128 || typeof sha !== "string" || !/^[a-f0-9]{40}(?:[a-f0-9]{24})?$/.test(sha) || typeof record.ref !== "string" || !record.ref.length || record.ref.length > 1024 || typeof record.web_url !== "string")
      throw new Error("Invalid GitLab response identity or shape");
    let returned;
    try {
      returned = normalizeTarget({ url: record.web_url });
    } catch (cause) {
      throw new Error("Invalid GitLab response URL", { cause });
    }
    if (returned.host !== target.host || returned.repo !== target.repo || returned.kind !== target.kind || returned.id !== target.id)
      throw new Error("GitLab response does not match requested target");
    let pipelineID;
    if (target.kind === "job") {
      const pipeline = record.pipeline;
      if (!pipeline || typeof pipeline !== "object" || !("id" in pipeline))
        throw new Error("Invalid GitLab response pipeline");
      try {
        pipelineID = numericID(pipeline.id);
      } catch (cause) {
        throw new Error("Invalid GitLab response pipeline ID", {
          cause
        });
      }
    }
    const inspection = {
      target: returned,
      rawStatus: record.status,
      sha,
      ref: record.ref,
      ...pipelineID ? { pipelineID } : {}
    };
    if (Buffer.byteLength(JSON.stringify(inspection)) > 4096)
      throw new Error("GitLab response exceeds inspection summary limit");
    return inspection;
  }
  async push(input, context, watches) {
    const mergeRequest = input.mergeRequest ? {
      ...projectIdentity(input.mergeRequest.host, input.mergeRequest.repo),
      iid: numericID(input.mergeRequest.iid)
    } : undefined;
    let branch = input.branch;
    if (branch === undefined) {
      const current = await this.git(["symbolic-ref", "--quiet", "--short", "HEAD"], input, context);
      if (current.status === "exit_error" && current.exitCode === 1)
        throw new Error("Detached HEAD requires an explicit destination branch");
      branch = commandText(current);
    }
    if (!branch || branch.startsWith("-"))
      throw new Error("Supply a destination branch name");
    commandText(await this.git(["check-ref-format", `refs/heads/${branch}`], input, context));
    const sha = commandText(await this.git(["rev-parse", "--verify", "HEAD^{commit}"], input, context));
    if (!/^[a-f0-9]{40}(?:[a-f0-9]{24})?$/.test(sha))
      throw new Error("Invalid captured commit SHA");
    const urls = commandText(await this.git([
      "remote",
      "get-url",
      "--push",
      "--all",
      "--",
      input.remote ?? "origin"
    ], input, context)).split(`
`);
    if (urls.length !== 1 || !urls[0])
      throw new Error("Expected exactly one push destination");
    const destination = urls[0];
    const target = {
      ...remoteIdentity(destination),
      sha,
      ref: branch,
      ...mergeRequest ? { mergeRequest } : {}
    };
    const result = await this.git(["push", "--", destination, `${sha}:refs/heads/${branch}`], input, context);
    if (result.status !== "success")
      return { push: "failed", ci: "not_started", target, result };
    if (context.abort.aborted || input.signal?.aborted)
      return {
        push: "success",
        ci: "not_started",
        target,
        watchError: "Canceled before discovery start"
      };
    try {
      const { watchID } = await watches.startDiscovery(target, {
        ...context,
        directory: resolve2(context.directory, input.cwd ?? ".")
      });
      return { push: "success", ci: "discovering", target, watchID };
    } catch (error) {
      return {
        push: "success",
        ci: "not_started",
        target,
        watchError: error instanceof Error ? error.message : String(error)
      };
    }
  }
  async execute(request, context) {
    if (!request.args.length)
      throw new Error("Supply a noninteractive glab command and explicit arguments");
    if (request.host && !/^[a-zA-Z0-9.-]+(?::\d+)?$/.test(request.host))
      throw new Error("host must be a hostname with optional port");
    if (request.repo && (!request.host || !/^[\w.-]+(?:\/[\w.-]+)+$/.test(request.repo)))
      throw new Error("repo requires an explicit host and a namespace/project path");
    const result = await this.runner({
      executable: "glab",
      args: [...request.args],
      cwd: request.cwd,
      stdin: request.stdin,
      timeoutMs: request.timeoutMs,
      signal: request.signal,
      env: {
        GLAB_NO_PROMPT: "1",
        NO_PROMPT: "1",
        PROMPT_DISABLED: "1",
        GIT_TERMINAL_PROMPT: "0",
        GIT_ASKPASS: "/usr/bin/false",
        SSH_ASKPASS: "/usr/bin/false",
        GLAB_EDITOR: "/usr/bin/false",
        EDITOR: "/usr/bin/false",
        VISUAL: "/usr/bin/false",
        GIT_EDITOR: "/usr/bin/false",
        GLAB_BROWSER: "/usr/bin/false",
        BROWSER: "/usr/bin/false",
        GLAB_PAGER: "/bin/cat",
        PAGER: "/bin/cat",
        GIT_PAGER: "/bin/cat",
        NO_COLOR: "1",
        TERM: "dumb",
        GLAB_CHECK_UPDATE: "false",
        CHECK_UPDATE: "false",
        GLAB_ENABLE_CI_AUTOLOGIN: "false",
        GLAB_SEND_TELEMETRY: "false",
        GLAB_SHOW_WHATS_NEW: "false",
        ...request.host ? { GITLAB_HOST: request.host } : {},
        ...request.repo ? { GITLAB_REPO: `${request.host}/${request.repo}` } : {}
      }
    }, context);
    if (result.status !== "exit_error")
      return result;
    const message = stripVTControlCharacters(result.stderr.toString());
    const error = /not logged|auth login|authentication|\b401\b|unauthorized/i.test(message) ? "authentication" : /\b403\b|forbidden/i.test(message) ? "authorization" : /non.?interactive|no.?prompt|requires.*(?:terminal|tty)|could not prompt|editor|browser/i.test(message) ? "interactive_required" : /\bHTTP\b|connection|request failed|dial tcp|timeout|TLS|EOF/i.test(message) ? "request_failed" : "command_failed";
    return { ...result, error };
  }
}

class OutputCache {
  maxBytes;
  maxEntries;
  entries = new Map;
  bytes = 0;
  constructor(maxBytes = 16 * 1024 * 1024, maxEntries = 128) {
    this.maxBytes = maxBytes;
    this.maxEntries = maxEntries;
    if (!Number.isInteger(maxBytes) || maxBytes < 1 || !Number.isInteger(maxEntries) || maxEntries < 1)
      throw new Error("Cache limits must be positive integers");
  }
  store(sessionID, result) {
    const bytes = result.stdout.length + result.stderr.length;
    if (bytes > Math.min(this.maxBytes, 1024 * 1024))
      throw new Error("Result exceeds cache retention limit");
    while (this.bytes + bytes > this.maxBytes || this.entries.size >= this.maxEntries) {
      const id2 = this.entries.keys().next().value;
      if (id2 === undefined)
        break;
      this.remove(id2);
    }
    const id = crypto.randomUUID();
    this.entries.set(id, { sessionID, result, bytes });
    this.bytes += bytes;
    return id;
  }
  read(sessionID, id, stream = "stdout", offset = 0, limit = 2048) {
    if (!Number.isSafeInteger(offset) || offset < 0 || !Number.isInteger(limit) || limit < 1 || limit > 8192)
      throw new Error("Use a nonnegative byte offset and limit of 1\u20138192 bytes");
    const entry = this.entries.get(id);
    if (!entry || entry.sessionID !== sessionID)
      return {
        status: "unavailable",
        reason: "Result expired, evicted, or not available in this session"
      };
    const buffer = entry.result[stream];
    const total = entry.result[stream === "stdout" ? "stdoutBytes" : "stderrBytes"];
    let binary = false;
    try {
      const text = new TextDecoder("utf-8", { fatal: true }).decode(buffer, { stream: true });
      binary = /[\x00-\x08\x0e-\x1a]/.test(stripVTControlCharacters(text));
    } catch {
      binary = true;
    }
    const page = {
      status: "available",
      stream,
      encoding: binary ? "base64" : "utf8",
      offset,
      retainedBytes: buffer.length,
      discardedBytes: total - buffer.length
    };
    let length = Math.min(limit, Math.max(0, buffer.length - offset));
    do {
      const part = buffer.subarray(offset, offset + length);
      page.encoding = "base64";
      page.data = part.toString("base64");
      if (!binary) {
        try {
          const text = new TextDecoder("utf-8", {
            fatal: true
          }).decode(part);
          page.data = stripVTControlCharacters(text).replace(/[\x00-\x08\x0b-\x1f\x7f-\x9f]/g, "");
          page.encoding = "utf8";
        } catch {}
      }
      page.nextOffset = offset + length;
      page.truncated = offset + length < total;
      if (Buffer.byteLength(JSON.stringify(page)) <= 8192)
        return page;
      length = Math.floor(length / 2);
    } while (length >= 0);
    return page;
  }
  remove(id) {
    const entry = this.entries.get(id);
    if (entry)
      this.bytes -= entry.bytes;
    this.entries.delete(id);
  }
  deleteSession(sessionID) {
    for (const [id, entry] of this.entries)
      if (entry.sessionID === sessionID)
        this.remove(id);
  }
  clear() {
    this.entries.clear();
    this.bytes = 0;
  }
}

// src/watches.ts
import { resolve as resolve3 } from "path";
var defaultClock = {
  now: Date.now,
  wait: (ms, signal) => new Promise((resolve4, reject) => {
    signal.throwIfAborted();
    const abort = () => {
      clearTimeout(timer);
      reject(signal.reason);
    };
    const timer = setTimeout(() => {
      signal.removeEventListener("abort", abort);
      resolve4();
    }, ms);
    timer.unref();
    signal.addEventListener("abort", abort, { once: true });
  })
};

class WatchManager {
  gitlab;
  complete;
  clock;
  watches = new Map;
  disposed = false;
  constructor(gitlab, complete, clock = defaultClock) {
    this.gitlab = gitlab;
    this.complete = complete;
    this.clock = clock;
  }
  async start(target, context, timeoutMs = 1800000) {
    return this.begin({ target: normalizeTarget({ url: target.url }) }, context, timeoutMs);
  }
  async startDiscovery(discovery, context, timeoutMs = 1800000) {
    normalizeTarget({ ...discovery, kind: "pipeline", id: "1" });
    if (!/^[a-f0-9]{40}(?:[a-f0-9]{24})?$/.test(discovery.sha) || !discovery.ref || discovery.ref.length > 1024)
      throw new Error("Invalid discovery SHA or ref");
    if (discovery.mergeRequest) {
      normalizeTarget({
        ...discovery.mergeRequest,
        kind: "pipeline",
        id: discovery.mergeRequest.iid
      });
    }
    return this.begin({ discovery: structuredClone(discovery) }, context, timeoutMs);
  }
  async begin(input, context, timeoutMs) {
    if (this.disposed)
      throw new Error("Watch manager disposed");
    if (!Number.isInteger(timeoutMs) || timeoutMs < 1 || timeoutMs > 1800000)
      throw new Error("Watch timeout must be 1\u20131800000 ms");
    context.abort.throwIfAborted();
    const identity = input.target ?? input.discovery;
    const key = JSON.stringify([
      context.sessionID,
      input.target ?? input.discovery
    ]);
    const existing = [...this.watches.values()].find((w) => w.key === key && !w.outcome);
    if (existing)
      return { watchID: existing.watchID, state: "watching" };
    const permission = {
      permission: "gitlab_watch",
      patterns: [
        `${identity.host}/${identity.repo}/${input.target ? `${input.target.kind}/${input.target.id}` : `sha/${input.discovery.sha}`}`
      ],
      always: [],
      metadata: { ...input, timeoutMs, background: true }
    };
    await new Promise((accept, reject) => {
      const abort = () => reject(context.abort.reason);
      context.abort.addEventListener("abort", abort, { once: true });
      context.ask(permission).then(accept, reject).finally(() => context.abort.removeEventListener("abort", abort));
      if (context.abort.aborted)
        abort();
    });
    context.abort.throwIfAborted();
    if (this.disposed)
      throw new Error("Watch manager disposed");
    const duplicate = [...this.watches.values()].find((w) => w.key === key && !w.outcome);
    if (duplicate)
      return { watchID: duplicate.watchID, state: "watching" };
    if ([...this.watches.values()].filter((w) => !w.outcome).length >= 16)
      throw new Error("Maximum 16 active watches");
    if (this.watches.size >= 128) {
      const oldest = [...this.watches.values()].find((w) => w.outcome && !w.flight);
      if (!oldest)
        throw new Error("Watch result retention is full");
      this.watches.delete(oldest.watchID);
    }
    const controller = new AbortController;
    const watch = {
      ...input,
      watchID: crypto.randomUUID(),
      controller,
      key,
      context: {
        ...context,
        directory: resolve3(context.directory),
        abort: controller.signal
      },
      started: this.clock.now(),
      deadline: this.clock.now() + timeoutMs
    };
    this.watches.set(watch.watchID, watch);
    const deadline = setTimeout(() => {
      watch.deadlineExpired = true;
      watch.outcome ??= watch.target ? "timeout" : "missing_pipeline";
      controller.abort();
    }, timeoutMs);
    deadline.unref();
    watch.flight = this.monitor(watch).finally(() => {
      clearTimeout(deadline);
      watch.flight = undefined;
    });
    return {
      watchID: watch.watchID,
      state: input.target ? "watching" : "discovering"
    };
  }
  get(sessionID, watchID) {
    const watch = this.watches.get(watchID);
    return watch?.context.sessionID === sessionID ? this.summary(watch) : undefined;
  }
  list(sessionID, offset = 0) {
    if (!Number.isSafeInteger(offset) || offset < 0)
      throw new Error("Invalid watch offset");
    const watches = [...this.watches.values()].filter((w) => w.context.sessionID === sessionID);
    return {
      watches: watches.slice(offset, offset + 8).map((w) => ({
        watchID: w.watchID,
        state: w.outcome ?? (w.target ? "watching" : "discovering")
      })),
      nextOffset: offset + 8 < watches.length ? offset + 8 : undefined
    };
  }
  stop(sessionID, watchID) {
    const watch = this.watches.get(watchID);
    if (!watch || watch.context.sessionID !== sessionID)
      throw new Error("Watch unavailable in this session");
    if (!watch.outcome)
      watch.outcome = "stopped";
    watch.controller.abort();
    return this.summary(watch);
  }
  deleteSession(sessionID) {
    for (const watch of this.watches.values()) {
      if (watch.context.sessionID !== sessionID)
        continue;
      this.stop(sessionID, watch.watchID);
      if (!watch.flight)
        this.watches.delete(watch.watchID);
      else
        watch.flight.then(() => this.watches.delete(watch.watchID));
    }
  }
  async settled(watchID) {
    await this.watches.get(watchID)?.flight;
  }
  async dispose() {
    this.disposed = true;
    for (const watch of this.watches.values())
      watch.controller.abort();
    await Promise.allSettled([...this.watches.values()].map((w) => w.flight));
    this.watches.clear();
  }
  summary(watch) {
    const target = watch.target;
    const summary = {
      watchID: watch.watchID,
      outcome: watch.outcome,
      state: watch.outcome ? "finished" : target ? "watching" : "discovering",
      target: target && {
        kind: target.kind,
        id: target.id,
        url: target.url
      },
      sha: watch.sha ?? watch.discovery?.sha,
      rawStatus: watch.rawStatus,
      error: watch.error?.slice(0, 400),
      detailsError: watch.detailsError?.slice(0, 200),
      notificationError: watch.notificationError?.slice(0, 200),
      candidates: watch.candidates?.map((c) => ({
        id: c.id,
        url: c.url
      })),
      failedJobs: watch.failedJobs,
      failedJobCount: watch.failedJobCount
    };
    for (const key of Object.keys(summary))
      if (summary[key] === undefined)
        delete summary[key];
    if (Buffer.byteLength(JSON.stringify(summary)) > 4096) {
      delete summary.candidates;
      delete summary.failedJobs;
      summary.truncated = true;
    }
    return summary;
  }
  remaining(watch) {
    return Math.min(watch.deadline, watch.target ? watch.deadline : watch.started + 120000) - this.clock.now();
  }
  async json(watch, project, endpoint) {
    const remaining = this.remaining(watch);
    if (remaining <= 0)
      throw new Error("Watch deadline reached");
    const result = await this.gitlab.execute({
      args: [
        "api",
        `projects/${encodeURIComponent(project.repo)}/${endpoint}`,
        "--method",
        "GET"
      ],
      ...project,
      timeoutMs: Math.min(30000, remaining),
      signal: AbortSignal.any([
        watch.controller.signal,
        AbortSignal.timeout(Math.min(30000, remaining))
      ])
    }, watch.context);
    if (result.status !== "success")
      throw new GitLabCommandError(result);
    if (result.stdoutBytes !== result.stdout.length)
      throw new Error("GitLab JSON exceeded capture limit");
    return JSON.parse(result.stdout.toString());
  }
  async discover(watch) {
    const discovery = watch.discovery;
    const sources = [
      {
        project: discovery,
        endpoint: `pipelines?sha=${discovery.sha}&ref=${encodeURIComponent(discovery.ref)}`,
        checkRef: true
      }
    ];
    if (discovery.mergeRequest)
      sources.push({
        project: discovery.mergeRequest,
        endpoint: `merge_requests/${discovery.mergeRequest.iid}/pipelines?`,
        checkRef: false
      });
    const matches = new Map;
    for (const source of sources) {
      for (let page = 1;page <= 20; page++) {
        const data = await this.json(watch, source.project, `${source.endpoint}&per_page=100&page=${page}`);
        if (!Array.isArray(data))
          throw new Error("Invalid pipeline list");
        for (const entry of data) {
          if (!entry || typeof entry !== "object" || typeof entry.sha !== "string" || typeof entry.ref !== "string")
            throw new Error("Invalid pipeline candidate");
          if (entry.sha !== discovery.sha || source.checkRef && entry.ref !== discovery.ref)
            continue;
          const target = normalizeTarget({ url: entry.web_url });
          const allowedProjects = source.checkRef ? [source.project] : [source.project, discovery];
          if (!allowedProjects.some((project) => target.host === project.host && target.repo === project.repo) || target.kind !== "pipeline" || target.id !== String(entry.id))
            throw new Error("Pipeline candidate identity mismatch");
          matches.set(target.url, target);
        }
        if (data.length < 100)
          break;
        if (page === 20)
          throw new Error("Pipeline discovery exceeds 2000 candidates; select a pipeline explicitly");
      }
    }
    if (matches.size > 1) {
      watch.candidates = [...matches.values()].slice(0, 5);
      watch.outcome = "ambiguous";
    } else if (matches.size === 1)
      watch.target = [...matches.values()][0];
  }
  async failureDetails(watch) {
    if (watch.target?.kind !== "pipeline")
      return;
    watch.failedJobs = [];
    watch.failedJobCount = 0;
    for (let page = 1;page <= 20; page++) {
      const data = await this.json(watch, watch.target, `pipelines/${watch.target.id}/jobs?scope[]=failed&per_page=100&page=${page}`);
      if (!Array.isArray(data))
        throw new Error("Invalid failed job list");
      watch.failedJobCount += data.length;
      for (const job of data) {
        if (watch.failedJobs.length >= 5)
          break;
        if (!job || typeof job.name !== "string")
          throw new Error("Invalid failed job");
        const target = normalizeTarget({ url: job.web_url });
        if (target.kind !== "job" || target.host !== watch.target.host || target.repo !== watch.target.repo || target.id !== String(job.id))
          throw new Error("Failed job identity mismatch");
        watch.failedJobs.push({
          id: target.id,
          name: job.name.slice(0, 100),
          url: target.url
        });
      }
      if (data.length < 100)
        return;
      if (page === 20)
        watch.detailsError = "Failed-job count is a lower bound; list exceeded 2000 jobs";
    }
  }
  async monitor(watch) {
    let failures = 0;
    let nativeTraceUsed = false;
    const signal = watch.controller.signal;
    try {
      while (!signal.aborted && !watch.outcome) {
        const remaining = this.remaining(watch);
        if (remaining <= 0) {
          watch.outcome = failures ? "monitoring_error" : watch.target ? "timeout" : "missing_pipeline";
          break;
        }
        try {
          if (!watch.target) {
            await this.discover(watch);
            failures = 0;
            watch.error = undefined;
            if (watch.outcome)
              break;
            if (!watch.target) {
              if (this.clock.now() - watch.started >= 120000) {
                watch.outcome = "missing_pipeline";
                break;
              }
              await this.clock.wait(Math.min(3000, remaining, 120000 - (this.clock.now() - watch.started)), signal);
              continue;
            }
          }
          const observed = await this.gitlab.inspect({
            url: watch.target.url,
            signal,
            timeoutMs: Math.max(1, Math.min(30000, watch.deadline - this.clock.now()))
          }, watch.context);
          watch.rawStatus = observed.rawStatus;
          watch.error = undefined;
          watch.sha = observed.sha;
          if (watch.discovery && watch.discovery.sha !== observed.sha)
            throw new Error("Selected pipeline SHA no longer matches pushed commit");
          const status = observed.rawStatus;
          if (["success", "failed", "canceled", "skipped"].includes(status))
            watch.outcome = status;
          else if (status === "manual")
            watch.outcome = "manual_action_required";
          else if (![
            "created",
            "waiting_for_resource",
            "preparing",
            "pending",
            "running",
            "scheduled",
            "canceling"
          ].includes(status))
            watch.outcome = "unsupported_state";
          if (watch.outcome) {
            if (watch.outcome === "failed" && !signal.aborted) {
              try {
                await this.failureDetails(watch);
              } catch (error) {
                watch.detailsError = error instanceof Error ? error.message : String(error);
              }
            }
            break;
          }
          failures = 0;
          if (watch.target.kind === "job" && !nativeTraceUsed && ["running", "pending"].includes(status) && watch.deadline - this.clock.now() > 30000) {
            nativeTraceUsed = true;
            await this.gitlab.execute({
              args: ["ci", "trace", watch.target.id],
              host: watch.target.host,
              repo: watch.target.repo,
              timeoutMs: 30000,
              signal
            }, watch.context);
            continue;
          }
        } catch (error) {
          if (signal.aborted)
            break;
          if (error instanceof GitLabCommandError && (["timeout", "canceled"].includes(error.result.status) || error.result.error === "request_failed" && !/HTTP (?:400|401|403|404)\b/.test(error.message))) {
            failures++;
            watch.error = error.message;
          } else
            throw error;
        }
        const delay = Math.min(30000, 3000 * 2 ** Math.min(failures, 4), this.remaining(watch));
        if (delay > 0)
          await this.clock.wait(delay, signal);
      }
    } catch (error) {
      if (!signal.aborted) {
        watch.outcome = "monitoring_error";
        watch.error = error instanceof Error ? error.message : String(error);
      }
    }
    if (this.disposed || watch.outcome === "stopped" || signal.aborted && !watch.deadlineExpired)
      return;
    try {
      this.complete({
        watchID: watch.watchID,
        sessionID: watch.context.sessionID,
        agent: watch.context.agent,
        text: JSON.stringify(this.summary(watch))
      });
    } catch (error) {
      watch.notificationError = error instanceof Error ? error.message : String(error);
    }
  }
}

// src/index.ts
var src_default = async ({ client, directory }) => {
  const delivery = new DeliveryQueue(client, directory);
  const gitlab = new GitLabClient;
  const cache = new OutputCache;
  const watches = new WatchManager(gitlab, (completion) => {
    delivery.enqueue(completion);
  });
  const running = new Set;
  let disposed = false;
  async function operation(context, execute) {
    if (disposed)
      throw new Error("Plugin disposed");
    if (running.size >= 8)
      throw new Error("Too many active GitLab commands (maximum 8)");
    const controller = new AbortController;
    const signal = AbortSignal.any([context.abort, controller.signal]);
    const release = delivery.own(context.sessionID, () => controller.abort());
    const flight = execute(signal);
    running.add(flight);
    try {
      return await flight;
    } finally {
      release();
      running.delete(flight);
    }
  }
  return {
    ...delivery.hooks,
    tool: {
      gitlab_watch: tool({
        description: "Start a background watch of an exact pipeline/job URL or ID. Returns immediately; completion wakes this session automatically. Do not poll or sleep. Also list/get/stop session-local watches; stop does not cancel GitLab work.",
        args: {
          action: tool.schema.enum(["start", "list", "get", "stop"]),
          watchID: tool.schema.string().uuid().optional(),
          url: tool.schema.string().optional(),
          kind: tool.schema.enum(["pipeline", "job"]).optional(),
          id: tool.schema.string().optional(),
          host: tool.schema.string().optional(),
          repo: tool.schema.string().optional(),
          cwd: tool.schema.string().optional(),
          timeoutMs: tool.schema.number().int().min(1).max(1800000).optional(),
          offset: tool.schema.number().int().min(0).optional()
        },
        async execute(args, context) {
          if (disposed)
            throw new Error("Plugin disposed");
          if (args.action === "list")
            return JSON.stringify(watches.list(context.sessionID, args.offset));
          if (args.action === "get" || args.action === "stop") {
            if (!args.watchID)
              throw new Error("watchID is required");
            const result = args.action === "stop" ? watches.stop(context.sessionID, args.watchID) : watches.get(context.sessionID, args.watchID);
            return JSON.stringify({
              ...result ?? { status: "unavailable" },
              delivery: result ? delivery.inspect(args.watchID) : undefined
            });
          }
          return operation(context, async (signal) => {
            const target = await gitlab.resolveTarget({ ...args, signal }, context);
            return JSON.stringify(await watches.start(target, {
              ...context,
              abort: signal,
              directory: resolve4(context.directory, args.cwd ?? ".")
            }, args.timeoutMs));
          });
        }
      }),
      gitlab_push: tool({
        description: "Push the captured current commit to a GitLab branch and start background CI discovery for that SHA. Completion wakes this session. Push success is not CI success. Prepare commits with existing Git tools; use gitlab for MR operations.",
        args: {
          cwd: tool.schema.string().optional(),
          remote: tool.schema.string().optional(),
          branch: tool.schema.string().optional(),
          timeoutMs: tool.schema.number().int().min(1).max(1800000).optional(),
          mergeRequest: tool.schema.object({
            host: tool.schema.string(),
            repo: tool.schema.string(),
            iid: tool.schema.string()
          }).optional()
        },
        async execute(args, context) {
          return operation(context, async (signal) => {
            const pushed = await gitlab.push({ ...args, signal }, { ...context, abort: signal }, {
              startDiscovery: (input, origin) => watches.startDiscovery(input, origin, args.timeoutMs)
            });
            if (pushed.push === "failed") {
              const resultID = cache.store(context.sessionID, pushed.result);
              return JSON.stringify({
                push: "failed",
                ci: "not_started",
                resultID,
                stderr: cache.read(context.sessionID, resultID, "stderr", 0, 512)
              });
            }
            return JSON.stringify({
              push: pushed.push,
              ci: pushed.ci,
              sha: pushed.target.sha,
              ...pushed.ci === "discovering" ? { watchID: pushed.watchID } : { error: pushed.watchError.slice(0, 400) }
            });
          });
        }
      }),
      gitlab_inspect: tool({
        description: "Inspect one exact GitLab pipeline or job. Supply a URL, or kind and ID with host/repo; otherwise resolve the current repository's origin. Returns raw status, never waits for CI.",
        args: {
          url: tool.schema.string().optional(),
          kind: tool.schema.enum(["pipeline", "job"]).optional(),
          id: tool.schema.union([
            tool.schema.string(),
            tool.schema.number().int().positive()
          ]).optional(),
          host: tool.schema.string().optional(),
          repo: tool.schema.string().optional(),
          cwd: tool.schema.string().optional(),
          remote: tool.schema.string().optional()
        },
        async execute(args, context) {
          if (disposed)
            throw new Error("Plugin disposed");
          if (running.size >= 8)
            throw new Error("Too many active GitLab commands (maximum 8)");
          const controller = new AbortController;
          const release = delivery.own(context.sessionID, () => controller.abort());
          const flight = gitlab.inspect({ ...args, signal: controller.signal }, context);
          running.add(flight);
          try {
            const result = await flight;
            if (controller.signal.aborted || context.abort.aborted || disposed)
              return JSON.stringify({ status: "canceled" });
            return JSON.stringify(result);
          } catch (error) {
            if (controller.signal.aborted || context.abort.aborted || disposed)
              return JSON.stringify({ status: "canceled" });
            if (!(error instanceof GitLabCommandError))
              throw error;
            const resultID = cache.store(context.sessionID, error.result);
            return JSON.stringify({
              status: error.result.status,
              error: error.result.error,
              exitCode: error.result.exitCode,
              resultID,
              stderr: cache.read(context.sessionID, resultID, "stderr", 0, 512)
            });
          } finally {
            running.delete(flight);
            release();
          }
        }
      }),
      gitlab: tool({
        description: "Run noninteractive glab argv once, with existing CLI credentials. Supply explicit flags; stdin supports API --input -. Streams drain until exit/timeout. Output is bounded; binary stdout is base64. CLI flags override host/repo defaults. No shell syntax. Cached output is session-local.",
        args: {
          args: tool.schema.array(tool.schema.string()).min(1),
          cwd: tool.schema.string().optional().describe("Working directory relative to the session directory"),
          host: tool.schema.string().optional().describe("GitLab hostname, optionally with port"),
          repo: tool.schema.string().optional().describe("Nested namespace/project path; requires host"),
          stdin: tool.schema.string().optional(),
          timeoutMs: tool.schema.number().int().min(1).max(1800000).optional()
        },
        async execute(args, context) {
          if (disposed)
            throw new Error("Plugin disposed");
          if (running.size >= 8)
            throw new Error("Too many active GitLab commands (maximum 8)");
          const controller = new AbortController;
          const release = delivery.own(context.sessionID, () => controller.abort());
          const flight = gitlab.execute({ ...args, signal: controller.signal }, context);
          running.add(flight);
          try {
            const result = await flight;
            if (controller.signal.aborted || context.abort.aborted || disposed)
              return JSON.stringify({ status: "canceled" });
            const resultID = cache.store(context.sessionID, result);
            const previewBytes = result.status === "success" ? 64 : 256;
            return JSON.stringify({
              resultID,
              status: result.status,
              error: result.error,
              exitCode: result.exitCode,
              signal: result.signal,
              durationMs: result.durationMs,
              stdout: cache.read(context.sessionID, resultID, "stdout", 0, previewBytes),
              stderr: cache.read(context.sessionID, resultID, "stderr", 0, previewBytes)
            });
          } finally {
            running.delete(flight);
            release();
          }
        }
      }),
      gitlab_output: tool({
        description: "Read retained command output by resultID. Byte offsets refer to raw stdout/stderr. At most 8 KiB per response; overflow was discarded and cannot be read. Binary data is base64. References expire on eviction, session deletion, or restart.",
        args: {
          resultID: tool.schema.string().uuid().optional(),
          jobURL: tool.schema.string().optional().describe("Fetch a job trace on demand instead of reading a cached command"),
          stream: tool.schema.enum(["stdout", "stderr"]).optional(),
          offset: tool.schema.number().int().min(0).optional(),
          limit: tool.schema.number().int().min(1).max(8192).optional()
        },
        async execute(args, context) {
          if (!!args.resultID === !!args.jobURL)
            throw new Error("Supply resultID or jobURL");
          if (args.jobURL) {
            return operation(context, async (signal) => {
              const target = await gitlab.resolveTarget({ url: args.jobURL }, context);
              if (target.kind !== "job")
                throw new Error("Trace requires a job URL");
              const result = await gitlab.execute({
                args: [
                  "api",
                  `projects/${encodeURIComponent(target.repo)}/jobs/${target.id}/trace`,
                  "--method",
                  "GET"
                ],
                host: target.host,
                repo: target.repo,
                signal
              }, context);
              if (result.status !== "success")
                throw new GitLabCommandError(result);
              const resultID = cache.store(context.sessionID, result);
              return JSON.stringify({
                resultID,
                ...cache.read(context.sessionID, resultID, "stdout", args.offset, Math.min(args.limit ?? 2048, 4096))
              });
            });
          }
          return JSON.stringify(cache.read(context.sessionID, args.resultID, args.stream, args.offset, args.limit));
        }
      })
    },
    async event(input) {
      await delivery.hooks.event?.(input);
      if (input.event.type === "session.deleted") {
        cache.deleteSession(input.event.properties.info.id);
        watches.deleteSession(input.event.properties.info.id);
      }
    },
    async dispose() {
      disposed = true;
      const stopped = watches.dispose();
      await delivery.hooks.dispose?.();
      await Promise.allSettled(running);
      await stopped;
      cache.clear();
    }
  };
};
export {
  src_default as default
};
