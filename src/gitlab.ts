import { stripVTControlCharacters } from "node:util";
import { resolve } from "node:path";
import type { ToolContext } from "@opencode-ai/plugin";
import { runProcess, type ProcessResult, type ProcessRunner } from "./process";

export interface GitLabRequest {
    args: string[];
    cwd?: string;
    host?: string;
    repo?: string;
    stdin?: string;
    timeoutMs?: number;
    signal?: AbortSignal;
}

export type GitLabError =
    | "authentication"
    | "authorization"
    | "request_failed"
    | "interactive_required"
    | "command_failed";
export type GitLabResult = ProcessResult & { error?: GitLabError };

export interface ProjectIdentity {
    host: string;
    repo: string;
}

export interface GitLabTarget extends ProjectIdentity {
    kind: "pipeline" | "job";
    id: string;
    url: string;
}

export interface TargetInput {
    url?: string;
    kind?: "pipeline" | "job";
    id?: string | number;
    host?: string;
    repo?: string;
    cwd?: string;
    remote?: string;
    signal?: AbortSignal;
    timeoutMs?: number;
}

export interface PipelineDiscovery extends ProjectIdentity {
    sha: string;
    ref: string;
    /** Lookup context only; it does not prove a fork or merged-result pipeline matches sha. */
    mergeRequest?: ProjectIdentity & { iid: string };
}

/** Starts owned background discovery and returns immediately, without selecting a pipeline. */
export interface WatchStarter {
    startDiscovery(
        input: PipelineDiscovery,
        context: ToolContext,
    ): { watchID: string } | Promise<{ watchID: string }>;
}

export interface PushInput {
    cwd?: string;
    remote?: string;
    branch?: string;
    mergeRequest?: ProjectIdentity & { iid: string | number };
    signal?: AbortSignal;
}

export type PushResult =
    | {
          push: "failed";
          ci: "not_started";
          target: PipelineDiscovery;
          result: ProcessResult;
      }
    | {
          push: "success";
          ci: "discovering";
          target: PipelineDiscovery;
          watchID: string;
      }
    | {
          push: "success";
          ci: "not_started";
          target: PipelineDiscovery;
          watchError: string;
      };

export interface GitLabInspection {
    target: GitLabTarget;
    rawStatus: string;
    sha: string;
    ref: string;
    pipelineID?: string;
}

/** Keeps original bounded stdout/stderr and process/CLI failure classification. */
export class GitLabCommandError extends Error {
    constructor(public readonly result: GitLabResult) {
        super(
            `Command ${result.error ?? result.status}: ${stripVTControlCharacters(result.stderr.toString()).slice(0, 512)}`,
        );
        this.name = "GitLabCommandError";
    }
}

function commandText(result: GitLabResult): string {
    if (result.status !== "success") throw new GitLabCommandError(result);
    if (result.stdoutBytes !== result.stdout.length)
        throw new Error("Command response is truncated");
    return new TextDecoder("utf-8", { fatal: true })
        .decode(result.stdout)
        .trim();
}

function numericID(value: unknown): string {
    if (
        (typeof value !== "string" && typeof value !== "number") ||
        (typeof value === "number" && !Number.isSafeInteger(value)) ||
        !/^\d{1,20}$/.test(String(value)) ||
        BigInt(value) < 1n
    )
        throw new Error("ID must be a positive integer");
    return BigInt(value).toString();
}

function projectIdentity(
    host: string | undefined,
    repo: string | undefined,
): ProjectIdentity {
    if (
        !host ||
        !/^[a-zA-Z0-9.-]+(?::\d+)?$/.test(host) ||
        !repo ||
        !/^[\w.-]+(?:\/[\w.-]+)+$/.test(repo) ||
        repo.split("/").some((part) => part === "." || part === "..") ||
        host.length + repo.length > 512
    )
        throw new Error(
            "Supply an explicit host and namespace/project repo context",
        );
    return { host: host.toLowerCase(), repo };
}

function remoteIdentity(remote: string): ProjectIdentity {
    const scp = remote.match(/^(?:[^/@:]+@)?([^/:]+):([^/].*)$/);
    if (scp && !remote.includes("://"))
        return projectIdentity(scp[1], scp[2]?.replace(/\.git$/, ""));
    const url = new URL(remote);
    if (
        !["https:", "http:", "ssh:"].includes(url.protocol) ||
        url.search ||
        url.hash
    )
        throw new Error("Remote must identify a GitLab host and project");
    // SSH transport ports are not GitLab API ports.
    return projectIdentity(
        url.protocol === "ssh:" ? url.hostname : url.host,
        decodeURIComponent(url.pathname.slice(1)).replace(/\.git$/, ""),
    );
}

export function normalizeTarget(input: TargetInput): GitLabTarget {
    if (input.url !== undefined) {
        if (
            input.kind !== undefined ||
            input.id !== undefined ||
            input.host !== undefined ||
            input.repo !== undefined
        )
            throw new Error("Supply a URL or kind/ID/repo context, not both");
        const url = new URL(input.url);
        const match = url.pathname.match(
            /^\/(.+)\/-\/(pipelines|jobs)\/(\d+)\/?$/,
        );
        if (
            !["http:", "https:"].includes(url.protocol) ||
            url.username ||
            url.password ||
            !match
        )
            throw new Error("Expected a GitLab pipeline or job URL");
        const identity = projectIdentity(
            url.host,
            decodeURIComponent(match[1]!),
        );
        const id = numericID(match[3]);
        return {
            ...identity,
            kind: match[2] === "jobs" ? "job" : "pipeline",
            id,
            url: `${url.protocol}//${identity.host}/${identity.repo}/-/${match[2]}/${id}`,
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
        url: `https://${identity.host}/${identity.repo}/-/${input.kind === "job" ? "jobs" : "pipelines"}/${id}`,
    };
}

/** One call is one attempt; mutations are never retried here. */
export class GitLabClient {
    constructor(private readonly runner: ProcessRunner = runProcess) {}

    private async git(
        args: string[],
        input: { cwd?: string; signal?: AbortSignal },
        context: ToolContext,
    ): Promise<ProcessResult> {
        return this.runner(
            {
                executable: "git",
                args,
                cwd: input.cwd,
                signal: input.signal,
                env: {
                    GIT_TERMINAL_PROMPT: "0",
                    GIT_ASKPASS: "/usr/bin/false",
                    SSH_ASKPASS: "/usr/bin/false",
                    GIT_EDITOR: "/usr/bin/false",
                    GIT_PAGER: "/bin/cat",
                },
            },
            context,
        );
    }

    async resolveTarget(
        input: TargetInput,
        context: ToolContext,
    ): Promise<GitLabTarget> {
        if (
            input.url !== undefined ||
            input.host !== undefined ||
            input.repo !== undefined
        )
            return normalizeTarget(input);
        // Validate the target before asking permission to read repository configuration.
        normalizeTarget({
            ...input,
            host: "context.test",
            repo: "context/project",
        });
        const remote = commandText(
            await this.git(
                ["remote", "get-url", "--", input.remote ?? "origin"],
                input,
                context,
            ),
        );
        return normalizeTarget({ ...input, ...remoteIdentity(remote) });
    }

    async inspect(
        input: TargetInput,
        context: ToolContext,
    ): Promise<GitLabInspection> {
        const target = await this.resolveTarget(input, context);
        const response = await this.execute(
            {
                args: [
                    "api",
                    `projects/${encodeURIComponent(target.repo)}/${target.kind === "job" ? "jobs" : "pipelines"}/${target.id}`,
                    "--method",
                    "GET",
                ],
                // Explicit environment routing also supports ports, which glab's --hostname rejects.
                host: target.host,
                repo: target.repo,
                cwd: input.cwd,
                signal: input.signal,
                timeoutMs: input.timeoutMs,
            },
            context,
        );
        const text = commandText(response);
        let data: unknown;
        try {
            data = JSON.parse(text);
        } catch (cause) {
            throw new Error("Invalid JSON response from GitLab", { cause });
        }
        if (!data || typeof data !== "object" || Array.isArray(data))
            throw new Error("Invalid GitLab response shape");
        const record = data as Record<string, unknown>;
        const commit = record.commit;
        const sha =
            target.kind === "pipeline"
                ? record.sha
                : commit && typeof commit === "object" && "id" in commit
                  ? commit.id
                  : undefined;
        if (
            record.id !== Number(target.id) ||
            !Number.isSafeInteger(record.id) ||
            typeof record.status !== "string" ||
            !record.status.length ||
            record.status.length > 128 ||
            typeof sha !== "string" ||
            !/^[a-f0-9]{40}(?:[a-f0-9]{24})?$/.test(sha) ||
            typeof record.ref !== "string" ||
            !record.ref.length ||
            record.ref.length > 1024 ||
            typeof record.web_url !== "string"
        )
            throw new Error("Invalid GitLab response identity or shape");
        let returned: GitLabTarget;
        try {
            returned = normalizeTarget({ url: record.web_url });
        } catch (cause) {
            throw new Error("Invalid GitLab response URL", { cause });
        }
        if (
            returned.host !== target.host ||
            returned.repo !== target.repo ||
            returned.kind !== target.kind ||
            returned.id !== target.id
        )
            throw new Error("GitLab response does not match requested target");
        let pipelineID: string | undefined;
        if (target.kind === "job") {
            const pipeline = record.pipeline;
            if (
                !pipeline ||
                typeof pipeline !== "object" ||
                !("id" in pipeline)
            )
                throw new Error("Invalid GitLab response pipeline");
            try {
                pipelineID = numericID(pipeline.id);
            } catch (cause) {
                throw new Error("Invalid GitLab response pipeline ID", {
                    cause,
                });
            }
        }
        const inspection: GitLabInspection = {
            target: returned,
            rawStatus: record.status,
            sha,
            ref: record.ref,
            ...(pipelineID ? { pipelineID } : {}),
        };
        if (Buffer.byteLength(JSON.stringify(inspection)) > 4096)
            throw new Error("GitLab response exceeds inspection summary limit");
        return inspection;
    }

    async push(
        input: PushInput,
        context: ToolContext,
        watches: WatchStarter,
    ): Promise<PushResult> {
        const mergeRequest = input.mergeRequest
            ? {
                  ...projectIdentity(
                      input.mergeRequest.host,
                      input.mergeRequest.repo,
                  ),
                  iid: numericID(input.mergeRequest.iid),
              }
            : undefined;
        let branch = input.branch;
        if (branch === undefined) {
            const current = await this.git(
                ["symbolic-ref", "--quiet", "--short", "HEAD"],
                input,
                context,
            );
            if (current.status === "exit_error" && current.exitCode === 1)
                throw new Error(
                    "Detached HEAD requires an explicit destination branch",
                );
            branch = commandText(current);
        }
        if (!branch || branch.startsWith("-"))
            throw new Error("Supply a destination branch name");
        commandText(
            await this.git(
                ["check-ref-format", `refs/heads/${branch}`],
                input,
                context,
            ),
        );
        const sha = commandText(
            await this.git(
                ["rev-parse", "--verify", "HEAD^{commit}"],
                input,
                context,
            ),
        );
        if (!/^[a-f0-9]{40}(?:[a-f0-9]{24})?$/.test(sha))
            throw new Error("Invalid captured commit SHA");
        const urls = commandText(
            await this.git(
                [
                    "remote",
                    "get-url",
                    "--push",
                    "--all",
                    "--",
                    input.remote ?? "origin",
                ],
                input,
                context,
            ),
        ).split("\n");
        if (urls.length !== 1 || !urls[0])
            throw new Error("Expected exactly one push destination");
        const destination = urls[0];
        const target: PipelineDiscovery = {
            ...remoteIdentity(destination),
            sha,
            ref: branch,
            ...(mergeRequest ? { mergeRequest } : {}),
        };
        const result = await this.git(
            ["push", "--", destination, `${sha}:refs/heads/${branch}`],
            input,
            context,
        );
        if (result.status !== "success")
            return { push: "failed", ci: "not_started", target, result };
        if (context.abort.aborted || input.signal?.aborted)
            return {
                push: "success",
                ci: "not_started",
                target,
                watchError: "Canceled before discovery start",
            };
        try {
            const { watchID } = await watches.startDiscovery(target, {
                ...context,
                directory: resolve(context.directory, input.cwd ?? "."),
            });
            return { push: "success", ci: "discovering", target, watchID };
        } catch (error) {
            return {
                push: "success",
                ci: "not_started",
                target,
                watchError:
                    error instanceof Error ? error.message : String(error),
            };
        }
    }

    async execute(
        request: GitLabRequest,
        context: ToolContext,
    ): Promise<GitLabResult> {
        if (!request.args.length)
            throw new Error(
                "Supply a noninteractive glab command and explicit arguments",
            );
        if (request.host && !/^[a-zA-Z0-9.-]+(?::\d+)?$/.test(request.host))
            throw new Error("host must be a hostname with optional port");
        if (
            request.repo &&
            (!request.host || !/^[\w.-]+(?:\/[\w.-]+)+$/.test(request.repo))
        )
            throw new Error(
                "repo requires an explicit host and a namespace/project path",
            );
        const result = await this.runner(
            {
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
                    ...(request.host ? { GITLAB_HOST: request.host } : {}),
                    ...(request.repo
                        ? { GITLAB_REPO: `${request.host}/${request.repo}` }
                        : {}),
                },
            },
            context,
        );
        if (result.status !== "exit_error") return result;
        const message = stripVTControlCharacters(result.stderr.toString());
        const error: GitLabError =
            /not logged|auth login|authentication|\b401\b|unauthorized/i.test(
                message,
            )
                ? "authentication"
                : /\b403\b|forbidden/i.test(message)
                  ? "authorization"
                  : /non.?interactive|no.?prompt|requires.*(?:terminal|tty)|could not prompt|editor|browser/i.test(
                          message,
                      )
                    ? "interactive_required"
                    : /\bHTTP\b|connection|request failed|dial tcp|timeout|TLS|EOF/i.test(
                            message,
                        )
                      ? "request_failed"
                      : "command_failed";
        return { ...result, error };
    }
}

export interface OutputPage {
    status: "available" | "unavailable";
    reason?: string;
    stream?: "stdout" | "stderr";
    encoding?: "utf8" | "base64";
    data?: string;
    offset?: number;
    nextOffset?: number;
    retainedBytes?: number;
    discardedBytes?: number;
    truncated?: boolean;
}

/** Raw byte offsets; only retained prefixes are available. No unbounded eviction tombstones. */
export class OutputCache {
    private readonly entries = new Map<
        string,
        { sessionID: string; result: ProcessResult; bytes: number }
    >();
    private bytes = 0;
    constructor(
        private readonly maxBytes = 16 * 1024 * 1024,
        private readonly maxEntries = 128,
    ) {
        if (
            !Number.isInteger(maxBytes) ||
            maxBytes < 1 ||
            !Number.isInteger(maxEntries) ||
            maxEntries < 1
        )
            throw new Error("Cache limits must be positive integers");
    }

    store(sessionID: string, result: ProcessResult): string {
        const bytes = result.stdout.length + result.stderr.length;
        if (bytes > Math.min(this.maxBytes, 1024 * 1024))
            throw new Error("Result exceeds cache retention limit");
        while (
            this.bytes + bytes > this.maxBytes ||
            this.entries.size >= this.maxEntries
        ) {
            const id = this.entries.keys().next().value;
            if (id === undefined) break;
            this.remove(id);
        }
        const id = crypto.randomUUID();
        this.entries.set(id, { sessionID, result, bytes });
        this.bytes += bytes;
        return id;
    }

    read(
        sessionID: string,
        id: string,
        stream: "stdout" | "stderr" = "stdout",
        offset = 0,
        limit = 2048,
    ): OutputPage {
        if (
            !Number.isSafeInteger(offset) ||
            offset < 0 ||
            !Number.isInteger(limit) ||
            limit < 1 ||
            limit > 8192
        )
            throw new Error(
                "Use a nonnegative byte offset and limit of 1–8192 bytes",
            );
        const entry = this.entries.get(id);
        if (!entry || entry.sessionID !== sessionID)
            return {
                status: "unavailable",
                reason: "Result expired, evicted, or not available in this session",
            };
        const buffer = entry.result[stream];
        const total =
            entry.result[stream === "stdout" ? "stdoutBytes" : "stderrBytes"];
        let binary = false;
        try {
            const text = new TextDecoder("utf-8", { fatal: true }).decode(
                buffer,
                { stream: true },
            );
            binary = /[\x00-\x08\x0e-\x1a]/.test(
                stripVTControlCharacters(text),
            );
        } catch {
            binary = true;
        }
        const page: OutputPage = {
            status: "available",
            stream,
            encoding: binary ? "base64" : "utf8",
            offset,
            retainedBytes: buffer.length,
            discardedBytes: total - buffer.length,
        };
        let length = Math.min(limit, Math.max(0, buffer.length - offset));
        do {
            const part = buffer.subarray(offset, offset + length);
            page.encoding = "base64";
            page.data = part.toString("base64");
            if (!binary) {
                try {
                    const text = new TextDecoder("utf-8", {
                        fatal: true,
                    }).decode(part);
                    page.data = stripVTControlCharacters(text).replace(
                        /[\x00-\x08\x0b-\x1f\x7f-\x9f]/g,
                        "",
                    );
                    page.encoding = "utf8";
                } catch {
                    // Byte offsets can split a character; base64 preserves those bytes.
                }
            }
            page.nextOffset = offset + length;
            page.truncated = offset + length < total;
            if (Buffer.byteLength(JSON.stringify(page)) <= 8192) return page;
            length = Math.floor(length / 2);
        } while (length >= 0);
        return page;
    }

    private remove(id: string) {
        const entry = this.entries.get(id);
        if (entry) this.bytes -= entry.bytes;
        this.entries.delete(id);
    }

    deleteSession(sessionID: string): void {
        for (const [id, entry] of this.entries)
            if (entry.sessionID === sessionID) this.remove(id);
    }

    clear(): void {
        this.entries.clear();
        this.bytes = 0;
    }
}
