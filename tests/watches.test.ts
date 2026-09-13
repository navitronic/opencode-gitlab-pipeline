import { expect, test } from "bun:test";
import type { ToolContext } from "@opencode-ai/plugin";
import { GitLabClient, normalizeTarget } from "../src/gitlab";
import type { ProcessResult } from "../src/process";
import { runProcess } from "../src/process";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { WatchManager } from "../src/watches";

const sha = "a".repeat(40);
test.skipIf(!Bun.which("glab"))(
    "installed glab native job trace yields one authoritative failure",
    async () => {
        const root = await mkdtemp(join(tmpdir(), "opencode-gitlab-trace-"));
        const paths: string[] = [];
        const commands: string[][] = [];
        let reads = 0;
        const server = Bun.serve({
            port: 0,
            hostname: "127.0.0.1",
            fetch(request): Response {
                const path = new URL(request.url).pathname;
                paths.push(path);
                if (path.endsWith("/trace"))
                    return new Response("private fixture trace\n");
                return Response.json({
                    id: 12,
                    name: "test",
                    status: ++reads === 1 ? "running" : "failed",
                    sha,
                    commit: { id: sha },
                    pipeline: { id: 13 },
                    ref: "main",
                    web_url: `${server.url}org/repo/-/jobs/12`,
                });
            },
        });
        const messages: string[] = [];
        const gitlab = new GitLabClient((request, ctx) => {
            commands.push([...request.args]);
            return runProcess(
                {
                    ...request,
                    env: {
                        ...request.env,
                        HOME: root,
                        XDG_CONFIG_HOME: root,
                        GLAB_CONFIG_DIR: root,
                        GITLAB_TOKEN: "fixture-only",
                        GLAB_API_PROTOCOL: "http",
                        API_PROTOCOL: "http",
                    },
                },
                ctx,
            );
        });
        const manager = new WatchManager(gitlab, (c) => {
            messages.push(c.text);
        });
        try {
            const ack = await manager.start(
                normalizeTarget({ url: `${server.url}org/repo/-/jobs/12` }),
                { ...context(), directory: root, worktree: root },
            );
            await manager.settled(ack.watchID);
            expect(commands).toHaveLength(3);
            expect(commands[1]).toEqual(["ci", "trace", "12"]);
            expect(paths.some((path) => path.endsWith("/trace"))).toBe(true);
            expect(messages).toHaveLength(1);
            expect(JSON.parse(messages[0]!).outcome).toBe("failed");
            expect(messages[0]).not.toContain("private fixture trace");
        } finally {
            await manager.dispose();
            server.stop(true);
            await rm(root, { recursive: true, force: true });
        }
    },
    15000,
);
test("default clock can create and dispose a watch", async () => {
    const manager = new WatchManager(
        new GitLabClient(async () =>
            result({
                id: 12,
                status: "success",
                sha,
                ref: "main",
                web_url: target.url,
            }),
        ),
        () => {},
    );
    const ack = await manager.start(target, context());
    await manager.settled(ack.watchID);
    expect(manager.get("session", ack.watchID)?.outcome).toBe("success");
    await manager.dispose();
});

test("watch deadline is explicit and aborting the initiating tool does not stop the watch", async () => {
    let time = 0;
    const origin = new AbortController();
    const completions: string[] = [];
    const manager = new WatchManager(
        new GitLabClient(async () => {
            origin.abort();
            return result({
                id: 12,
                status: "running",
                sha,
                ref: "main",
                web_url: target.url,
            });
        }),
        (c) => {
            completions.push(c.text);
        },
        {
            now: () => time,
            wait: async (ms, signal) => {
                expect(signal.aborted).toBe(false);
                time += ms;
            },
        },
    );
    const ack = await manager.start(
        target,
        { ...context(), abort: origin.signal },
        5000,
    );
    await manager.settled(ack.watchID);
    expect(JSON.parse(completions[0]!).outcome).toBe("timeout");
});

test("transient read errors back off while authentication fails immediately", async () => {
    for (const auth of [false, true]) {
        let calls = 0;
        let time = 0;
        const waits: number[] = [];
        const completions: string[] = [];
        const manager = new WatchManager(
            new GitLabClient(async () => {
                if (++calls > 1)
                    return result({
                        id: 12,
                        status: "success",
                        sha,
                        ref: "main",
                        web_url: target.url,
                    });
                const stderr = Buffer.from(
                    auth ? "HTTP 401 Unauthorized" : "HTTP 503 request failed",
                );
                return {
                    ...result(null),
                    status: "exit_error",
                    exitCode: 1,
                    stderr,
                    stderrBytes: stderr.length,
                };
            }),
            (c) => {
                completions.push(c.text);
            },
            {
                now: () => time,
                wait: async (ms) => {
                    time += ms;
                    waits.push(ms);
                },
            },
        );
        const ack = await manager.start(target, context());
        await manager.settled(ack.watchID);
        expect(calls).toBe(auth ? 1 : 2);
        expect(waits).toEqual(auth ? [] : [6000]);
        expect(JSON.parse(completions[0]!).outcome).toBe(
            auth ? "monitoring_error" : "success",
        );
    }
});

test("discovery read failures still obey the two-minute discovery deadline", async () => {
    let time = 0;
    const completions: string[] = [];
    const manager = new WatchManager(
        new GitLabClient(async () => {
            const stderr = Buffer.from("HTTP 503 request failed");
            return {
                ...result(null),
                status: "exit_error",
                exitCode: 1,
                stderr,
                stderrBytes: stderr.length,
            };
        }),
        (c) => {
            completions.push(c.text);
        },
        {
            now: () => time,
            wait: async (ms) => {
                time += ms;
            },
        },
    );
    const ack = await manager.startDiscovery(
        { host: target.host, repo: target.repo, sha, ref: "main" },
        context(),
    );
    await manager.settled(ack.watchID);
    expect(time).toBeLessThanOrEqual(120000);
    expect(JSON.parse(completions[0]!).outcome).toBe("monitoring_error");
});

test("pending watch permission is cancellable", async () => {
    const { manager } = setup(["success"]);
    const controller = new AbortController();
    const entered = Promise.withResolvers<void>();
    const pending = manager.start(target, {
        ...context(),
        abort: controller.signal,
        ask: async () => {
            entered.resolve();
            return new Promise<void>(() => {});
        },
    });
    await entered.promise;
    controller.abort(new Error("aborted"));
    await expect(pending).rejects.toThrow("aborted");
    expect(manager.list("session").watches).toHaveLength(0);
});

test("duplicates reuse an active watch only within their session", async () => {
    const entered = Promise.withResolvers<void>();
    const release = Promise.withResolvers<void>();
    const manager = new WatchManager(
        new GitLabClient(async () => {
            entered.resolve();
            await release.promise;
            return result({
                id: 12,
                status: "success",
                sha,
                ref: "main",
                web_url: target.url,
            });
        }),
        () => {},
    );
    const first = await manager.start(target, context());
    await entered.promise;
    const duplicate = await manager.start(target, context());
    const separate = await manager.start(target, {
        ...context(),
        sessionID: "another",
    });
    expect(duplicate.watchID).toBe(first.watchID);
    expect(separate.watchID).not.toBe(first.watchID);
    release.resolve();
    await manager.dispose();
});
const target = normalizeTarget({
    host: "gitlab.test",
    repo: "org/repo",
    kind: "pipeline",
    id: "12",
});
function context(): ToolContext {
    return {
        sessionID: "session",
        messageID: "message",
        agent: "build",
        directory: process.cwd(),
        worktree: process.cwd(),
        abort: new AbortController().signal,
        ask: async () => {},
        metadata: () => {},
    };
}
function result(data: unknown): ProcessResult {
    const stdout = Buffer.from(JSON.stringify(data));
    return {
        status: "success",
        exitCode: 0,
        signal: null,
        stdout,
        stderr: Buffer.alloc(0),
        stdoutBytes: stdout.length,
        stderrBytes: 0,
        durationMs: 0,
    };
}
function setup(states: string[], kind: "pipeline" | "job" = "pipeline") {
    let time = 0;
    const commands: string[][] = [];
    const completions: string[] = [];
    const client = new GitLabClient(async (request) => {
        commands.push([...request.args]);
        if (request.args[0] === "ci") return result("trace");
        if (request.args[1]?.includes("/jobs?")) return result([]);
        const status = states.length > 1 ? states.shift()! : states[0]!;
        return result({
            id: 12,
            status,
            sha,
            commit: { id: sha },
            pipeline: { id: 13 },
            ref: "main",
            web_url: `https://gitlab.test/org/repo/-/${kind === "job" ? "jobs" : "pipelines"}/12`,
        });
    });
    const manager = new WatchManager(
        client,
        (completion) => {
            completions.push(completion.text);
        },
        {
            now: () => time,
            wait: async (ms, signal) => {
                signal.throwIfAborted();
                time += ms;
            },
        },
    );
    return { manager, commands, completions };
}

test("watch returns before checks finish and emits one exact result without progress", async () => {
    const { manager, commands, completions } = setup([
        "pending",
        "running",
        "success",
    ]);
    const ctx = context();
    const ack = await manager.start(target, ctx);
    expect(ack.watchID).toBeDefined();
    await manager.settled(ack.watchID);
    expect(completions).toHaveLength(1);
    expect(JSON.parse(completions[0]!).outcome).toBe("success");
    expect(commands).toHaveLength(3);
    expect(
        commands.every(
            (args) => args[1] === "projects/org%2Frepo/pipelines/12",
        ),
    ).toBe(true);
    expect(Buffer.byteLength(completions[0]!)).toBeLessThanOrEqual(1024);
    await manager.dispose();
});

test("native trace exit zero is followed by authoritative failed job lookup", async () => {
    const { manager, commands, completions } = setup(
        ["running", "failed"],
        "job",
    );
    const ack = await manager.start(
        normalizeTarget({ ...target, kind: "job", url: undefined }),
        context(),
    );
    await manager.settled(ack.watchID);
    expect(commands.some((args) => args.join(" ") === "ci trace 12")).toBe(
        true,
    );
    expect(JSON.parse(completions[0]!).outcome).toBe("failed");
});

test.each(["manual", "skipped", "canceled", "future_status"])(
    "%s never confirms success",
    async (status) => {
        const { manager, completions } = setup([status]);
        const ack = await manager.start(target, context());
        await manager.settled(ack.watchID);
        const completion = JSON.parse(completions[0]!);
        expect(completion.rawStatus).toBe(status);
        expect(completion.outcome).not.toBe("success");
    },
);

test("permission denial starts no watch", async () => {
    const { manager, commands } = setup(["success"]);
    const ctx = {
        ...context(),
        ask: async () => {
            throw new Error("denied");
        },
    };
    await expect(manager.start(target, ctx)).rejects.toThrow("denied");
    expect(commands).toHaveLength(0);
});

test("stop and session deletion cancel local work without remote cancellation or completion", async () => {
    let waiting!: () => void;
    const barrier = new Promise<void>((resolve) => {
        waiting = resolve;
    });
    const completions: string[] = [];
    const commands: string[][] = [];
    const manager = new WatchManager(
        new GitLabClient(async (request) => {
            commands.push([...request.args]);
            return result({
                id: 12,
                status: "running",
                sha,
                ref: "main",
                web_url: target.url,
            });
        }),
        (c) => {
            completions.push(c.text);
        },
        {
            now: Date.now,
            wait: (_ms, signal) =>
                new Promise<void>((resolve) => {
                    waiting();
                    signal.addEventListener("abort", () => resolve(), {
                        once: true,
                    });
                }),
        },
    );
    const ack = await manager.start(target, context());
    await barrier;
    const other = manager.get("other", ack.watchID);
    expect(other).toBeUndefined();
    manager.stop("session", ack.watchID);
    await manager.settled(ack.watchID);
    expect(completions).toHaveLength(0);
    expect(manager.get("session", ack.watchID)?.outcome).toBe("stopped");
    expect(commands.every((args) => args.includes("GET"))).toBe(true);
    await manager.dispose();
});

test("discovery pins only a matching SHA and ref", async () => {
    let time = 0;
    let lists = 0;
    const commands: string[][] = [];
    const completions: string[] = [];
    const client = new GitLabClient(async (request) => {
        commands.push([...request.args]);
        if (request.args[1]?.includes("pipelines?"))
            return result(
                ++lists === 1
                    ? []
                    : [
                          {
                              id: 99,
                              sha: "b".repeat(40),
                              ref: "main",
                              web_url: target.url.replace("12", "99"),
                          },
                          { id: 12, sha, ref: "main", web_url: target.url },
                      ],
            );
        return result({
            id: 12,
            status: "success",
            sha,
            ref: "main",
            web_url: target.url,
        });
    });
    const manager = new WatchManager(
        client,
        (c) => {
            completions.push(c.text);
        },
        {
            now: () => time,
            wait: async (ms) => {
                time += ms;
            },
        },
    );
    const ack = await manager.startDiscovery(
        { host: target.host, repo: target.repo, sha, ref: "main" },
        context(),
    );
    await manager.settled(ack.watchID);
    expect(JSON.parse(completions[0]!).target.id).toBe("12");
    expect(commands.some((args) => args[1]?.endsWith("pipelines/99"))).toBe(
        false,
    );
});

test("fork MR discovery accepts only the pushed or MR project and deduplicates matches", async () => {
    for (const project of ["org/repo", "unrelated/project"]) {
        const completions: string[] = [];
        const queries: string[] = [];
        const client = new GitLabClient(async (request) => {
            const endpoint = String(request.args[1]);
            queries.push(endpoint);
            if (endpoint.includes("pipelines?"))
                return result([
                    {
                        id: 12,
                        sha,
                        ref: "main",
                        web_url: `https://gitlab.test/${endpoint.includes("merge_requests/") ? project : "org/repo"}/-/pipelines/12`,
                    },
                ]);
            return result({
                id: 12,
                status: "success",
                sha,
                ref: "main",
                web_url: target.url,
            });
        });
        const manager = new WatchManager(client, (c) => {
            completions.push(c.text);
        });
        const ack = await manager.startDiscovery(
            {
                host: target.host,
                repo: target.repo,
                sha,
                ref: "main",
                mergeRequest: {
                    host: target.host,
                    repo: "upstream/repo",
                    iid: "4",
                },
            },
            context(),
        );
        await manager.settled(ack.watchID);
        expect(JSON.parse(completions[0]!).outcome).toBe(
            project === "org/repo" ? "success" : "monitoring_error",
        );
        if (project === "org/repo")
            expect(queries.at(-1)).toBe("projects/org%2Frepo/pipelines/12");
    }
});

test("discovery deadline and ambiguity produce explicit outcomes", async () => {
    for (const ambiguous of [false, true]) {
        let time = 0;
        const completions: string[] = [];
        const manager = new WatchManager(
            new GitLabClient(async () =>
                result(
                    ambiguous
                        ? [12, 13].map((id) => ({
                              id,
                              sha,
                              ref: "main",
                              web_url: target.url.replace("12", String(id)),
                          }))
                        : [],
                ),
            ),
            (c) => {
                completions.push(c.text);
            },
            {
                now: () => time,
                wait: async (ms) => {
                    time += ms;
                },
            },
        );
        const ack = await manager.startDiscovery(
            { host: target.host, repo: target.repo, sha, ref: "main" },
            context(),
        );
        await manager.settled(ack.watchID);
        expect(JSON.parse(completions[0]!).outcome).toBe(
            ambiguous ? "ambiguous" : "missing_pipeline",
        );
    }
});
