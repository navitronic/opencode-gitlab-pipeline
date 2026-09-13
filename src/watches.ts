import { resolve } from "node:path";
import type { ToolContext } from "@opencode-ai/plugin";
import type { Completion } from "./delivery";
import {
    GitLabClient,
    GitLabCommandError,
    normalizeTarget,
    type GitLabTarget,
    type PipelineDiscovery,
    type ProjectIdentity,
} from "./gitlab";

interface Clock {
    now(): number;
    wait(ms: number, signal: AbortSignal): Promise<void>;
}

const defaultClock: Clock = {
    now: Date.now,
    wait: (ms, signal) =>
        new Promise((resolve, reject) => {
            signal.throwIfAborted();
            const abort = () => {
                clearTimeout(timer);
                reject(signal.reason);
            };
            const timer = setTimeout(() => {
                signal.removeEventListener("abort", abort);
                resolve();
            }, ms);
            timer.unref();
            signal.addEventListener("abort", abort, { once: true });
        }),
};

type Outcome =
    | "success"
    | "failed"
    | "canceled"
    | "skipped"
    | "manual_action_required"
    | "unsupported_state"
    | "missing_pipeline"
    | "ambiguous"
    | "timeout"
    | "monitoring_error"
    | "stopped";
interface Watch {
    watchID: string;
    context: ToolContext;
    controller: AbortController;
    target?: GitLabTarget;
    discovery?: PipelineDiscovery;
    key: string;
    started: number;
    deadline: number;
    outcome?: Outcome;
    rawStatus?: string;
    sha?: string;
    error?: string;
    detailsError?: string;
    candidates?: GitLabTarget[];
    failedJobs?: { id: string; name: string; url: string }[];
    failedJobCount?: number;
    notificationError?: string;
    deadlineExpired?: boolean;
    flight?: Promise<void>;
}

/** In-memory, session-owned watches. Every subprocess retains OpenCode permission checks. */
export class WatchManager {
    private readonly watches = new Map<string, Watch>();
    private disposed = false;
    constructor(
        private readonly gitlab: GitLabClient,
        private readonly complete: (completion: Completion) => void,
        private readonly clock: Clock = defaultClock,
    ) {}

    async start(
        target: GitLabTarget,
        context: ToolContext,
        timeoutMs = 1800000,
    ) {
        return this.begin(
            { target: normalizeTarget({ url: target.url }) },
            context,
            timeoutMs,
        );
    }

    async startDiscovery(
        discovery: PipelineDiscovery,
        context: ToolContext,
        timeoutMs = 1800000,
    ) {
        normalizeTarget({ ...discovery, kind: "pipeline", id: "1" });
        if (
            !/^[a-f0-9]{40}(?:[a-f0-9]{24})?$/.test(discovery.sha) ||
            !discovery.ref ||
            discovery.ref.length > 1024
        )
            throw new Error("Invalid discovery SHA or ref");
        if (discovery.mergeRequest) {
            normalizeTarget({
                ...discovery.mergeRequest,
                kind: "pipeline",
                id: discovery.mergeRequest.iid,
            });
        }
        return this.begin(
            { discovery: structuredClone(discovery) },
            context,
            timeoutMs,
        );
    }

    private async begin(
        input: { target?: GitLabTarget; discovery?: PipelineDiscovery },
        context: ToolContext,
        timeoutMs: number,
    ) {
        if (this.disposed) throw new Error("Watch manager disposed");
        if (
            !Number.isInteger(timeoutMs) ||
            timeoutMs < 1 ||
            timeoutMs > 1800000
        )
            throw new Error("Watch timeout must be 1–1800000 ms");
        context.abort.throwIfAborted();
        const identity = input.target ?? input.discovery!;
        const key = JSON.stringify([
            context.sessionID,
            input.target ?? input.discovery,
        ]);
        const existing = [...this.watches.values()].find(
            (w) => w.key === key && !w.outcome,
        );
        if (existing) return { watchID: existing.watchID, state: "watching" };
        const permission: Parameters<ToolContext["ask"]>[0] = {
            permission: "gitlab_watch",
            patterns: [
                `${identity.host}/${identity.repo}/${input.target ? `${input.target.kind}/${input.target.id}` : `sha/${input.discovery!.sha}`}`,
            ],
            always: [],
            metadata: { ...input, timeoutMs, background: true },
        };
        await new Promise<void>((accept, reject) => {
            const abort = () => reject(context.abort.reason);
            context.abort.addEventListener("abort", abort, { once: true });
            void context
                .ask(permission)
                .then(accept, reject)
                .finally(() =>
                    context.abort.removeEventListener("abort", abort),
                );
            if (context.abort.aborted) abort();
        });
        context.abort.throwIfAborted();
        if (this.disposed) throw new Error("Watch manager disposed");
        const duplicate = [...this.watches.values()].find(
            (w) => w.key === key && !w.outcome,
        );
        if (duplicate) return { watchID: duplicate.watchID, state: "watching" };
        if ([...this.watches.values()].filter((w) => !w.outcome).length >= 16)
            throw new Error("Maximum 16 active watches");
        if (this.watches.size >= 128) {
            const oldest = [...this.watches.values()].find(
                (w) => w.outcome && !w.flight,
            );
            if (!oldest) throw new Error("Watch result retention is full");
            this.watches.delete(oldest.watchID);
        }
        const controller = new AbortController();
        const watch: Watch = {
            ...input,
            watchID: crypto.randomUUID(),
            controller,
            key,
            // A completed initiating tool is not the lifetime owner. Preserve its permission function.
            context: {
                ...context,
                directory: resolve(context.directory),
                abort: controller.signal,
            },
            started: this.clock.now(),
            deadline: this.clock.now() + timeoutMs,
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
            state: input.target ? "watching" : "discovering",
        };
    }

    get(sessionID: string, watchID: string) {
        const watch = this.watches.get(watchID);
        return watch?.context.sessionID === sessionID
            ? this.summary(watch)
            : undefined;
    }

    list(sessionID: string, offset = 0) {
        if (!Number.isSafeInteger(offset) || offset < 0)
            throw new Error("Invalid watch offset");
        const watches = [...this.watches.values()].filter(
            (w) => w.context.sessionID === sessionID,
        );
        return {
            watches: watches.slice(offset, offset + 8).map((w) => ({
                watchID: w.watchID,
                state: w.outcome ?? (w.target ? "watching" : "discovering"),
            })),
            nextOffset: offset + 8 < watches.length ? offset + 8 : undefined,
        };
    }

    stop(sessionID: string, watchID: string) {
        const watch = this.watches.get(watchID);
        if (!watch || watch.context.sessionID !== sessionID)
            throw new Error("Watch unavailable in this session");
        if (!watch.outcome) watch.outcome = "stopped";
        watch.controller.abort();
        return this.summary(watch);
    }

    deleteSession(sessionID: string) {
        for (const watch of this.watches.values()) {
            if (watch.context.sessionID !== sessionID) continue;
            this.stop(sessionID, watch.watchID);
            // Keep running flights until dispose can await their child cleanup.
            if (!watch.flight) this.watches.delete(watch.watchID);
            else
                void watch.flight.then(() =>
                    this.watches.delete(watch.watchID),
                );
        }
    }

    async settled(watchID: string) {
        await this.watches.get(watchID)?.flight;
    }

    async dispose() {
        this.disposed = true;
        for (const watch of this.watches.values()) watch.controller.abort();
        await Promise.allSettled(
            [...this.watches.values()].map((w) => w.flight),
        );
        this.watches.clear();
    }

    private summary(watch: Watch): Record<string, unknown> {
        const target = watch.target;
        const summary: Record<string, unknown> = {
            watchID: watch.watchID,
            outcome: watch.outcome,
            state: watch.outcome
                ? "finished"
                : target
                  ? "watching"
                  : "discovering",
            target: target && {
                kind: target.kind,
                id: target.id,
                url: target.url,
            },
            sha: watch.sha ?? watch.discovery?.sha,
            rawStatus: watch.rawStatus,
            error: watch.error?.slice(0, 400),
            detailsError: watch.detailsError?.slice(0, 200),
            notificationError: watch.notificationError?.slice(0, 200),
            candidates: watch.candidates?.map((c) => ({
                id: c.id,
                url: c.url,
            })),
            failedJobs: watch.failedJobs,
            failedJobCount: watch.failedJobCount,
        };
        for (const key of Object.keys(summary))
            if (summary[key] === undefined) delete summary[key];
        if (Buffer.byteLength(JSON.stringify(summary)) > 4096) {
            delete summary.candidates;
            delete summary.failedJobs;
            summary.truncated = true;
        }
        return summary;
    }

    private remaining(watch: Watch) {
        return (
            Math.min(
                watch.deadline,
                watch.target ? watch.deadline : watch.started + 120000,
            ) - this.clock.now()
        );
    }

    private async json(
        watch: Watch,
        project: ProjectIdentity,
        endpoint: string,
    ): Promise<unknown> {
        const remaining = this.remaining(watch);
        if (remaining <= 0) throw new Error("Watch deadline reached");
        const result = await this.gitlab.execute(
            {
                args: [
                    "api",
                    `projects/${encodeURIComponent(project.repo)}/${endpoint}`,
                    "--method",
                    "GET",
                ],
                ...project,
                timeoutMs: Math.min(30000, remaining),
                signal: AbortSignal.any([
                    watch.controller.signal,
                    AbortSignal.timeout(Math.min(30000, remaining)),
                ]),
            },
            watch.context,
        );
        if (result.status !== "success") throw new GitLabCommandError(result);
        if (result.stdoutBytes !== result.stdout.length)
            throw new Error("GitLab JSON exceeded capture limit");
        return JSON.parse(result.stdout.toString());
    }

    private async discover(watch: Watch) {
        const discovery = watch.discovery!;
        const sources = [
            {
                project: discovery as ProjectIdentity,
                endpoint: `pipelines?sha=${discovery.sha}&ref=${encodeURIComponent(discovery.ref)}`,
                checkRef: true,
            },
        ];
        if (discovery.mergeRequest)
            sources.push({
                project: discovery.mergeRequest,
                endpoint: `merge_requests/${discovery.mergeRequest.iid}/pipelines?`,
                checkRef: false,
            });
        const matches = new Map<string, GitLabTarget>();
        for (const source of sources) {
            for (let page = 1; page <= 20; page++) {
                const data = await this.json(
                    watch,
                    source.project,
                    `${source.endpoint}&per_page=100&page=${page}`,
                );
                if (!Array.isArray(data))
                    throw new Error("Invalid pipeline list");
                for (const entry of data) {
                    if (
                        !entry ||
                        typeof entry !== "object" ||
                        typeof entry.sha !== "string" ||
                        typeof entry.ref !== "string"
                    )
                        throw new Error("Invalid pipeline candidate");
                    if (
                        entry.sha !== discovery.sha ||
                        (source.checkRef && entry.ref !== discovery.ref)
                    )
                        continue;
                    const target = normalizeTarget({ url: entry.web_url });
                    const allowedProjects = source.checkRef
                        ? [source.project]
                        : [source.project, discovery];
                    if (
                        !allowedProjects.some(
                            (project) =>
                                target.host === project.host &&
                                target.repo === project.repo,
                        ) ||
                        target.kind !== "pipeline" ||
                        target.id !== String(entry.id)
                    )
                        throw new Error("Pipeline candidate identity mismatch");
                    matches.set(target.url, target);
                }
                if (data.length < 100) break;
                if (page === 20)
                    throw new Error(
                        "Pipeline discovery exceeds 2000 candidates; select a pipeline explicitly",
                    );
            }
        }
        if (matches.size > 1) {
            watch.candidates = [...matches.values()].slice(0, 5);
            watch.outcome = "ambiguous";
        } else if (matches.size === 1) watch.target = [...matches.values()][0];
    }

    private async failureDetails(watch: Watch) {
        if (watch.target?.kind !== "pipeline") return;
        watch.failedJobs = [];
        watch.failedJobCount = 0;
        for (let page = 1; page <= 20; page++) {
            const data = await this.json(
                watch,
                watch.target,
                `pipelines/${watch.target.id}/jobs?scope[]=failed&per_page=100&page=${page}`,
            );
            if (!Array.isArray(data))
                throw new Error("Invalid failed job list");
            watch.failedJobCount += data.length;
            for (const job of data) {
                if (watch.failedJobs.length >= 5) break;
                if (!job || typeof job.name !== "string")
                    throw new Error("Invalid failed job");
                const target = normalizeTarget({ url: job.web_url });
                if (
                    target.kind !== "job" ||
                    target.host !== watch.target.host ||
                    target.repo !== watch.target.repo ||
                    target.id !== String(job.id)
                )
                    throw new Error("Failed job identity mismatch");
                watch.failedJobs.push({
                    id: target.id,
                    name: job.name.slice(0, 100),
                    url: target.url,
                });
            }
            if (data.length < 100) return;
            if (page === 20)
                watch.detailsError =
                    "Failed-job count is a lower bound; list exceeded 2000 jobs";
        }
    }

    private async monitor(watch: Watch) {
        let failures = 0;
        let nativeTraceUsed = false;
        const signal = watch.controller.signal;
        try {
            while (!signal.aborted && !watch.outcome) {
                const remaining = this.remaining(watch);
                if (remaining <= 0) {
                    watch.outcome = failures
                        ? "monitoring_error"
                        : watch.target
                          ? "timeout"
                          : "missing_pipeline";
                    break;
                }
                try {
                    if (!watch.target) {
                        await this.discover(watch);
                        failures = 0;
                        watch.error = undefined;
                        if (watch.outcome) break;
                        if (!watch.target) {
                            if (this.clock.now() - watch.started >= 120000) {
                                watch.outcome = "missing_pipeline";
                                break;
                            }
                            await this.clock.wait(
                                Math.min(
                                    3000,
                                    remaining,
                                    120000 - (this.clock.now() - watch.started),
                                ),
                                signal,
                            );
                            continue;
                        }
                    }
                    const observed = await this.gitlab.inspect(
                        {
                            url: watch.target.url,
                            signal,
                            timeoutMs: Math.max(
                                1,
                                Math.min(
                                    30000,
                                    watch.deadline - this.clock.now(),
                                ),
                            ),
                        },
                        watch.context,
                    );
                    watch.rawStatus = observed.rawStatus;
                    watch.error = undefined;
                    watch.sha = observed.sha;
                    if (watch.discovery && watch.discovery.sha !== observed.sha)
                        throw new Error(
                            "Selected pipeline SHA no longer matches pushed commit",
                        );
                    const status = observed.rawStatus;
                    if (
                        ["success", "failed", "canceled", "skipped"].includes(
                            status,
                        )
                    )
                        watch.outcome = status as Outcome;
                    else if (status === "manual")
                        watch.outcome = "manual_action_required";
                    else if (
                        ![
                            "created",
                            "waiting_for_resource",
                            "preparing",
                            "pending",
                            "running",
                            "scheduled",
                            "canceling",
                        ].includes(status)
                    )
                        watch.outcome = "unsupported_state";
                    if (watch.outcome) {
                        if (watch.outcome === "failed" && !signal.aborted) {
                            try {
                                await this.failureDetails(watch);
                            } catch (error) {
                                watch.detailsError =
                                    error instanceof Error
                                        ? error.message
                                        : String(error);
                            }
                        }
                        break;
                    }
                    failures = 0;
                    if (
                        watch.target.kind === "job" &&
                        !nativeTraceUsed &&
                        ["running", "pending"].includes(status) &&
                        watch.deadline - this.clock.now() > 30000
                    ) {
                        nativeTraceUsed = true;
                        // glab owns its poll loop. A bounded watchdog covers manual/skipped trace hangs.
                        await this.gitlab.execute(
                            {
                                args: ["ci", "trace", watch.target.id],
                                host: watch.target.host,
                                repo: watch.target.repo,
                                timeoutMs: 30000,
                                signal,
                            },
                            watch.context,
                        );
                        continue;
                    }
                } catch (error) {
                    if (signal.aborted) break;
                    if (
                        error instanceof GitLabCommandError &&
                        (["timeout", "canceled"].includes(
                            error.result.status,
                        ) ||
                            (error.result.error === "request_failed" &&
                                !/HTTP (?:400|401|403|404)\b/.test(
                                    error.message,
                                )))
                    ) {
                        failures++;
                        watch.error = error.message;
                    } else throw error;
                }
                const delay = Math.min(
                    30000,
                    3000 * 2 ** Math.min(failures, 4),
                    this.remaining(watch),
                );
                if (delay > 0) await this.clock.wait(delay, signal);
            }
        } catch (error) {
            if (!signal.aborted) {
                watch.outcome = "monitoring_error";
                watch.error =
                    error instanceof Error ? error.message : String(error);
            }
        }
        if (
            this.disposed ||
            watch.outcome === "stopped" ||
            (signal.aborted && !watch.deadlineExpired)
        )
            return;
        try {
            this.complete({
                watchID: watch.watchID,
                sessionID: watch.context.sessionID,
                agent: watch.context.agent,
                text: JSON.stringify(this.summary(watch)),
            });
        } catch (error) {
            watch.notificationError =
                error instanceof Error ? error.message : String(error);
        }
    }
}
