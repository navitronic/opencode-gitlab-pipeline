import type { Plugin, ToolContext } from "@opencode-ai/plugin";
import { resolve } from "node:path";
import { tool } from "@opencode-ai/plugin";
import { DeliveryQueue } from "./delivery";
import { GitLabClient, GitLabCommandError, OutputCache } from "./gitlab";
import { WatchManager } from "./watches";

export default (async ({ client, directory }) => {
    const delivery = new DeliveryQueue(client, directory);
    const gitlab = new GitLabClient();
    const cache = new OutputCache();
    const watches = new WatchManager(gitlab, (completion) => {
        delivery.enqueue(completion);
    });
    const running = new Set<Promise<unknown>>();
    let disposed = false;
    async function operation<T>(
        context: ToolContext,
        execute: (signal: AbortSignal) => Promise<T>,
    ): Promise<T> {
        if (disposed) throw new Error("Plugin disposed");
        if (running.size >= 8)
            throw new Error("Too many active GitLab commands (maximum 8)");
        const controller = new AbortController();
        const signal = AbortSignal.any([context.abort, controller.signal]);
        const release = delivery.own(context.sessionID, () =>
            controller.abort(),
        );
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
                description:
                    "Start a background watch of an exact pipeline/job URL or ID. Returns immediately; completion wakes this session automatically. Do not poll or sleep. Also list/get/stop session-local watches; stop does not cancel GitLab work.",
                args: {
                    action: tool.schema.enum(["start", "list", "get", "stop"]),
                    watchID: tool.schema.string().uuid().optional(),
                    url: tool.schema.string().optional(),
                    kind: tool.schema.enum(["pipeline", "job"]).optional(),
                    id: tool.schema.string().optional(),
                    host: tool.schema.string().optional(),
                    repo: tool.schema.string().optional(),
                    cwd: tool.schema.string().optional(),
                    timeoutMs: tool.schema
                        .number()
                        .int()
                        .min(1)
                        .max(1800000)
                        .optional(),
                    offset: tool.schema.number().int().min(0).optional(),
                },
                async execute(args, context) {
                    if (disposed) throw new Error("Plugin disposed");
                    if (args.action === "list")
                        return JSON.stringify(
                            watches.list(context.sessionID, args.offset),
                        );
                    if (args.action === "get" || args.action === "stop") {
                        if (!args.watchID)
                            throw new Error("watchID is required");
                        const result =
                            args.action === "stop"
                                ? watches.stop(context.sessionID, args.watchID)
                                : watches.get(context.sessionID, args.watchID);
                        return JSON.stringify({
                            ...(result ?? { status: "unavailable" }),
                            delivery: result
                                ? delivery.inspect(args.watchID)
                                : undefined,
                        });
                    }
                    return operation(context, async (signal) => {
                        const target = await gitlab.resolveTarget(
                            { ...args, signal },
                            context,
                        );
                        return JSON.stringify(
                            await watches.start(
                                target,
                                {
                                    ...context,
                                    abort: signal,
                                    directory: resolve(
                                        context.directory,
                                        args.cwd ?? ".",
                                    ),
                                },
                                args.timeoutMs,
                            ),
                        );
                    });
                },
            }),
            gitlab_push: tool({
                description:
                    "Push the captured current commit to a GitLab branch and start background CI discovery for that SHA. Completion wakes this session. Push success is not CI success. Prepare commits with existing Git tools; use gitlab for MR operations.",
                args: {
                    cwd: tool.schema.string().optional(),
                    remote: tool.schema.string().optional(),
                    branch: tool.schema.string().optional(),
                    timeoutMs: tool.schema
                        .number()
                        .int()
                        .min(1)
                        .max(1800000)
                        .optional(),
                    mergeRequest: tool.schema
                        .object({
                            host: tool.schema.string(),
                            repo: tool.schema.string(),
                            iid: tool.schema.string(),
                        })
                        .optional(),
                },
                async execute(args, context) {
                    return operation(context, async (signal) => {
                        const pushed = await gitlab.push(
                            { ...args, signal },
                            { ...context, abort: signal },
                            {
                                startDiscovery: (input, origin) =>
                                    watches.startDiscovery(
                                        input,
                                        origin,
                                        args.timeoutMs,
                                    ),
                            },
                        );
                        if (pushed.push === "failed") {
                            const resultID = cache.store(
                                context.sessionID,
                                pushed.result,
                            );
                            return JSON.stringify({
                                push: "failed",
                                ci: "not_started",
                                resultID,
                                stderr: cache.read(
                                    context.sessionID,
                                    resultID,
                                    "stderr",
                                    0,
                                    512,
                                ),
                            });
                        }
                        return JSON.stringify({
                            push: pushed.push,
                            ci: pushed.ci,
                            sha: pushed.target.sha,
                            ...(pushed.ci === "discovering"
                                ? { watchID: pushed.watchID }
                                : { error: pushed.watchError.slice(0, 400) }),
                        });
                    });
                },
            }),
            gitlab_inspect: tool({
                description:
                    "Inspect one exact GitLab pipeline or job. Supply a URL, or kind and ID with host/repo; otherwise resolve the current repository's origin. Returns raw status, never waits for CI.",
                args: {
                    url: tool.schema.string().optional(),
                    kind: tool.schema.enum(["pipeline", "job"]).optional(),
                    id: tool.schema
                        .union([
                            tool.schema.string(),
                            tool.schema.number().int().positive(),
                        ])
                        .optional(),
                    host: tool.schema.string().optional(),
                    repo: tool.schema.string().optional(),
                    cwd: tool.schema.string().optional(),
                    remote: tool.schema.string().optional(),
                },
                async execute(args, context) {
                    if (disposed) throw new Error("Plugin disposed");
                    if (running.size >= 8)
                        throw new Error(
                            "Too many active GitLab commands (maximum 8)",
                        );
                    const controller = new AbortController();
                    const release = delivery.own(context.sessionID, () =>
                        controller.abort(),
                    );
                    const flight = gitlab.inspect(
                        { ...args, signal: controller.signal },
                        context,
                    );
                    running.add(flight);
                    try {
                        const result = await flight;
                        if (
                            controller.signal.aborted ||
                            context.abort.aborted ||
                            disposed
                        )
                            return JSON.stringify({ status: "canceled" });
                        return JSON.stringify(result);
                    } catch (error) {
                        if (
                            controller.signal.aborted ||
                            context.abort.aborted ||
                            disposed
                        )
                            return JSON.stringify({ status: "canceled" });
                        if (!(error instanceof GitLabCommandError)) throw error;
                        const resultID = cache.store(
                            context.sessionID,
                            error.result,
                        );
                        return JSON.stringify({
                            status: error.result.status,
                            error: error.result.error,
                            exitCode: error.result.exitCode,
                            resultID,
                            stderr: cache.read(
                                context.sessionID,
                                resultID,
                                "stderr",
                                0,
                                512,
                            ),
                        });
                    } finally {
                        running.delete(flight);
                        release();
                    }
                },
            }),
            gitlab: tool({
                description:
                    "Run noninteractive glab argv once, with existing CLI credentials. Supply explicit flags; stdin supports API --input -. Streams drain until exit/timeout. Output is bounded; binary stdout is base64. CLI flags override host/repo defaults. No shell syntax. Cached output is session-local.",
                args: {
                    args: tool.schema.array(tool.schema.string()).min(1),
                    cwd: tool.schema
                        .string()
                        .optional()
                        .describe(
                            "Working directory relative to the session directory",
                        ),
                    host: tool.schema
                        .string()
                        .optional()
                        .describe("GitLab hostname, optionally with port"),
                    repo: tool.schema
                        .string()
                        .optional()
                        .describe(
                            "Nested namespace/project path; requires host",
                        ),
                    stdin: tool.schema.string().optional(),
                    timeoutMs: tool.schema
                        .number()
                        .int()
                        .min(1)
                        .max(1800000)
                        .optional(),
                },
                async execute(args, context) {
                    if (disposed) throw new Error("Plugin disposed");
                    if (running.size >= 8)
                        throw new Error(
                            "Too many active GitLab commands (maximum 8)",
                        );
                    const controller = new AbortController();
                    const release = delivery.own(context.sessionID, () =>
                        controller.abort(),
                    );
                    const flight = gitlab.execute(
                        { ...args, signal: controller.signal },
                        context,
                    );
                    running.add(flight);
                    try {
                        const result = await flight;
                        if (
                            controller.signal.aborted ||
                            context.abort.aborted ||
                            disposed
                        )
                            return JSON.stringify({ status: "canceled" });
                        const resultID = cache.store(context.sessionID, result);
                        const previewBytes =
                            result.status === "success" ? 64 : 256;
                        return JSON.stringify({
                            resultID,
                            status: result.status,
                            error: result.error,
                            exitCode: result.exitCode,
                            signal: result.signal,
                            durationMs: result.durationMs,
                            stdout: cache.read(
                                context.sessionID,
                                resultID,
                                "stdout",
                                0,
                                previewBytes,
                            ),
                            stderr: cache.read(
                                context.sessionID,
                                resultID,
                                "stderr",
                                0,
                                previewBytes,
                            ),
                        });
                    } finally {
                        running.delete(flight);
                        release();
                    }
                },
            }),
            gitlab_output: tool({
                description:
                    "Read retained command output by resultID. Byte offsets refer to raw stdout/stderr. At most 8 KiB per response; overflow was discarded and cannot be read. Binary data is base64. References expire on eviction, session deletion, or restart.",
                args: {
                    resultID: tool.schema.string().uuid().optional(),
                    jobURL: tool.schema
                        .string()
                        .optional()
                        .describe(
                            "Fetch a job trace on demand instead of reading a cached command",
                        ),
                    stream: tool.schema.enum(["stdout", "stderr"]).optional(),
                    offset: tool.schema.number().int().min(0).optional(),
                    limit: tool.schema
                        .number()
                        .int()
                        .min(1)
                        .max(8192)
                        .optional(),
                },
                async execute(args, context) {
                    if (!!args.resultID === !!args.jobURL)
                        throw new Error("Supply resultID or jobURL");
                    if (args.jobURL) {
                        return operation(context, async (signal) => {
                            const target = await gitlab.resolveTarget(
                                { url: args.jobURL },
                                context,
                            );
                            if (target.kind !== "job")
                                throw new Error("Trace requires a job URL");
                            const result = await gitlab.execute(
                                {
                                    args: [
                                        "api",
                                        `projects/${encodeURIComponent(target.repo)}/jobs/${target.id}/trace`,
                                        "--method",
                                        "GET",
                                    ],
                                    host: target.host,
                                    repo: target.repo,
                                    signal,
                                },
                                context,
                            );
                            if (result.status !== "success")
                                throw new GitLabCommandError(result);
                            const resultID = cache.store(
                                context.sessionID,
                                result,
                            );
                            return JSON.stringify({
                                resultID,
                                ...cache.read(
                                    context.sessionID,
                                    resultID,
                                    "stdout",
                                    args.offset,
                                    Math.min(args.limit ?? 2048, 4096),
                                ),
                            });
                        });
                    }
                    return JSON.stringify(
                        cache.read(
                            context.sessionID,
                            args.resultID!,
                            args.stream,
                            args.offset,
                            args.limit,
                        ),
                    );
                },
            }),
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
        },
    };
}) satisfies Plugin;
