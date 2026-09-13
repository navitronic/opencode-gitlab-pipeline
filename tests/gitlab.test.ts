import { expect, test } from "bun:test";
import type { ToolContext, ToolResult } from "@opencode-ai/plugin";
import {
    GitLabClient,
    OutputCache,
    normalizeTarget,
    type PipelineDiscovery,
    type WatchStarter,
} from "../src/gitlab";
import type { ProcessRequest, ProcessResult } from "../src/process";
import { runProcess } from "../src/process";
import { mkdtemp, mkdir, writeFile, rm } from "node:fs/promises";
import { join } from "node:path";
import { tmpdir } from "node:os";
import { createOpencodeClient } from "@opencode-ai/sdk";
import plugin from "../src/index";

const context: ToolContext = {
    sessionID: "s",
    messageID: "m",
    agent: "build",
    directory: process.cwd(),
    worktree: process.cwd(),
    abort: new AbortController().signal,
    metadata() {},
    async ask() {},
};
function toolText(output: ToolResult): string {
    if (typeof output !== "string")
        throw new Error("Expected string tool output");
    return output;
}
function result(stdout = "", stderr = "", exitCode = 0): ProcessResult {
    return {
        status: exitCode ? "exit_error" : "success",
        exitCode,
        signal: null,
        stdout: Buffer.from(stdout),
        stderr: Buffer.from(stderr),
        stdoutBytes: Buffer.byteLength(stdout),
        stderrBytes: Buffer.byteLength(stderr),
        durationMs: 1,
    };
}

test("target normalization retains self-hosted nested identity and rejects ambiguous inputs", () => {
    expect(
        normalizeTarget({
            url: "https://git.example:8443/a/b/c/-/jobs/0042?x=1#log",
        }),
    ).toEqual({
        host: "git.example:8443",
        repo: "a/b/c",
        kind: "job",
        id: "42",
        url: "https://git.example:8443/a/b/c/-/jobs/42",
    });
    expect(
        normalizeTarget({
            kind: "pipeline",
            id: 42,
            host: "other.test",
            repo: "a/b/c",
        }).host,
    ).toBe("other.test");
    for (const input of [
        { kind: "job" as const, id: 1 },
        { kind: "job" as const, id: "1x", host: "h", repo: "a/b" },
        { url: "https://h/a/b/-/pipelines/1", kind: "job" as const },
        { url: "https://h/a/b/-/jobs/1/trace" },
    ])
        expect(() => normalizeTarget(input)).toThrow();
});

test("inspect queries only exact IDs with explicit API host and validates JSON identity", async () => {
    const calls: ProcessRequest[] = [];
    const input = { url: "https://self.test/a/b/c/-/pipelines/42" };
    const payload = {
        id: 42,
        status: "running",
        sha: "a".repeat(40),
        ref: "feature",
        web_url: input.url,
    };
    const client = new GitLabClient(async (request) => {
        calls.push(request);
        return result(JSON.stringify(payload));
    });
    expect(await client.inspect(input, context)).toMatchObject({
        target: { id: "42", host: "self.test" },
        rawStatus: "running",
        sha: payload.sha,
    });
    expect(calls[0]?.args).toEqual([
        "api",
        "projects/a%2Fb%2Fc/pipelines/42",
        "--method",
        "GET",
    ]);
    expect(calls[0]?.env).toMatchObject({
        GITLAB_HOST: "self.test",
        GITLAB_REPO: "self.test/a/b/c",
    });
    for (const output of [
        result("{"),
        result("[]"),
        result(JSON.stringify({ ...payload, id: 43 })),
        result(JSON.stringify({ ...payload, status: null })),
        result(JSON.stringify({ ...payload, ref: "\u0000".repeat(1024) })),
        { ...result(JSON.stringify(payload)), stdoutBytes: 9999 },
    ]) {
        await expect(
            new GitLabClient(async () => output).inspect(input, context),
        ).rejects.toThrow(/JSON|response|truncated/);
    }
    const failure = result("", "HTTP 403 Forbidden", 1);
    await expect(
        new GitLabClient(async () => failure).inspect(input, context),
    ).rejects.toMatchObject({
        result: { error: "authorization", stderr: failure.stderr },
    });
});

test("ID resolution uses repository context only when explicit identity is absent", async () => {
    const calls: ProcessRequest[] = [];
    const client = new GitLabClient(async (request) => {
        calls.push(request);
        return result("git@self.test:nested/team/repo.git\n");
    });
    expect(
        await client.resolveTarget({ kind: "job", id: 9 }, context),
    ).toMatchObject({ host: "self.test", repo: "nested/team/repo", id: "9" });
    expect(calls[0]?.args).toEqual(["remote", "get-url", "--", "origin"]);
    calls.length = 0;
    await client.resolveTarget(
        { kind: "job", id: 9, host: "explicit.test", repo: "a/b" },
        context,
    );
    expect(calls).toHaveLength(0);
    await expect(
        client.resolveTarget({ kind: "job", id: 9, repo: "a/b" }, context),
    ).rejects.toThrow(/host/);
});

test("job inspection preserves raw status and synthetic commit identity without claiming push association", async () => {
    const url = "https://self.test/upstream/nested/repo/-/jobs/7";
    const synthetic = "c".repeat(40);
    const payload = {
        id: 7,
        status: "manual",
        ref: "refs/merge-requests/12/merge",
        web_url: url,
        commit: { id: synthetic },
        pipeline: { id: 99 },
    };
    expect(
        await new GitLabClient(async () =>
            result(JSON.stringify(payload)),
        ).inspect({ url }, context),
    ).toEqual({
        target: normalizeTarget({ url }),
        rawStatus: "manual",
        ref: payload.ref,
        sha: synthetic,
        pipelineID: "99",
    });
    for (const patch of [
        { pipeline: null },
        { commit: { id: "invalid" } },
        { web_url: "https://other.test/upstream/nested/repo/-/jobs/7" },
    ]) {
        await expect(
            new GitLabClient(async () =>
                result(JSON.stringify({ ...payload, ...patch })),
            ).inspect({ url }, context),
        ).rejects.toThrow(/response/);
    }
});

test("push hands immutable discovery identity to a synchronous starter and preserves rejection", async () => {
    const calls: ProcessRequest[] = [];
    const discoveries: PipelineDiscovery[] = [];
    const sha = "a".repeat(40);
    let rejected = false;
    const client = new GitLabClient(async (request) => {
        calls.push(request);
        if (request.args[0] === "rev-parse") return result(sha + "\n");
        if (request.args[0] === "symbolic-ref") return result("feature\n");
        if (request.args[0] === "remote")
            return result("ssh://git@push.test/a/fork.git\n");
        if (request.args[0] === "push" && rejected)
            return result("", "rejected", 1);
        return result();
    });
    const watch: WatchStarter = {
        startDiscovery(input, ctx) {
            expect(ctx.sessionID).toBe("s");
            discoveries.push(input);
            return { watchID: "w1" };
        },
    };
    const mergeRequest = {
        host: "upstream.test",
        repo: "nested/upstream",
        iid: "12",
    };
    const pushed = await client.push({ mergeRequest }, context, watch);
    expect(pushed).toMatchObject({
        push: "success",
        ci: "discovering",
        watchID: "w1",
        target: {
            host: "push.test",
            repo: "a/fork",
            sha,
            ref: "feature",
            mergeRequest,
        },
    });
    expect(discoveries).toHaveLength(1);
    expect(calls.find((c) => c.args[0] === "push")?.args).toEqual([
        "push",
        "--",
        "ssh://git@push.test/a/fork.git",
        `${sha}:refs/heads/feature`,
    ]);
    expect(calls.some((c) => c.executable === "glab")).toBe(false);
    rejected = true;
    expect(await client.push({}, context, watch)).toMatchObject({
        push: "failed",
        ci: "not_started",
        result: { status: "exit_error" },
    });
    expect(discoveries).toHaveLength(1);
    rejected = false;
    expect(
        await client.push({}, context, {
            startDiscovery() {
                throw new Error("capacity");
            },
        }),
    ).toMatchObject({
        push: "success",
        ci: "not_started",
        watchError: "capacity",
    });
});

test("detached HEAD requires branch; multiple push destinations fail before pushing", async () => {
    const calls: string[] = [];
    let urls = "https://push.test/a/b.git\n";
    const client = new GitLabClient(async (request) => {
        calls.push(request.args[0]!);
        if (request.args[0] === "symbolic-ref") return result("", "", 1);
        if (request.args[0] === "rev-parse") return result("b".repeat(40));
        if (request.args[0] === "remote") return result(urls);
        return result();
    });
    const watch: WatchStarter = {
        startDiscovery() {
            return { watchID: "w" };
        },
    };
    await expect(client.push({}, context, watch)).rejects.toThrow(/Detached/);
    expect(calls).not.toContain("push");
    expect(
        await client.push({ branch: "explicit" }, context, watch),
    ).toMatchObject({ push: "success", target: { ref: "explicit" } });
    calls.length = 0;
    urls += "https://other.test/a/b.git\n";
    await expect(
        client.push({ branch: "explicit" }, context, watch),
    ).rejects.toThrow(/one push/);
    expect(calls).not.toContain("push");
});

test("real local bare remote receives captured SHA even when HEAD moves at push permission", async () => {
    const root = await mkdtemp(join(tmpdir(), "opencode-gitlab-push-"));
    const work = join(root, "work");
    const bare = join(root, "remote.git");
    await mkdir(work);
    const ctx = { ...context, directory: work, worktree: root };
    const git = async (...args: string[]) => {
        const output = await runProcess({ executable: "git", args }, ctx);
        expect(output.status).toBe("success");
        return output.stdout.toString().trim();
    };
    try {
        await git("init", "--initial-branch=feature");
        await git("init", "--bare", bare);
        await git(
            "-c",
            "user.name=Fixture",
            "-c",
            "user.email=fixture@example.test",
            "commit",
            "--allow-empty",
            "-m",
            "first",
        );
        const sha = await git("rev-parse", "HEAD");
        await git(
            "remote",
            "add",
            "origin",
            "https://fetch.test/wrong/project.git",
        );
        await git(
            "remote",
            "set-url",
            "--push",
            "origin",
            "https://push.test/nested/fork.git",
        );
        const permissions: string[] = [];
        const client = new GitLabClient((request, callContext) =>
            runProcess(
                {
                    ...request,
                    // Redirect only the transport to a local bare repository; resolution remains real git.
                    env: {
                        ...request.env,
                        ...(request.args[0] === "push"
                            ? {
                                  GIT_CONFIG_COUNT: "1",
                                  GIT_CONFIG_KEY_0: `url.file://${bare}.insteadOf`,
                                  GIT_CONFIG_VALUE_0:
                                      "https://push.test/nested/fork.git",
                              }
                            : {}),
                    },
                },
                callContext,
            ),
        );
        let discovery: PipelineDiscovery | undefined;
        const output = await client.push(
            {},
            {
                ...ctx,
                async ask(permission) {
                    permissions.push(String(permission.metadata.command));
                    if (permission.metadata.args?.[0] === "push")
                        await git(
                            "-c",
                            "user.name=Fixture",
                            "-c",
                            "user.email=fixture@example.test",
                            "commit",
                            "--allow-empty",
                            "-m",
                            "second",
                        );
                },
            },
            {
                startDiscovery(input) {
                    discovery = input;
                    return { watchID: "local" };
                },
            },
        );
        expect(output.push).toBe("success");
        expect(
            await git("--git-dir", bare, "rev-parse", "refs/heads/feature"),
        ).toBe(sha);
        expect(await git("rev-parse", "HEAD")).not.toBe(sha);
        expect(discovery).toMatchObject({
            host: "push.test",
            repo: "nested/fork",
            sha,
            ref: "feature",
        });
        expect(
            permissions.some((command) =>
                command.includes(`${sha}:refs/heads/feature`),
            ),
        ).toBe(true);
        expect(permissions.map((command) => command.split(" ")[1])).toEqual([
            "symbolic-ref",
            "check-ref-format",
            "rev-parse",
            "remote",
            "push",
        ]);
        let starts = 0;
        const watch: WatchStarter = {
            startDiscovery() {
                starts++;
                return { watchID: "unexpected" };
            },
        };
        await expect(
            client.push(
                {},
                {
                    ...ctx,
                    async ask(permission) {
                        if (permission.metadata.args?.[0] === "push")
                            throw new Error("deny push");
                    },
                },
                watch,
            ),
        ).rejects.toThrow("deny push");
        expect(
            await git("--git-dir", bare, "rev-parse", "refs/heads/feature"),
        ).toBe(sha);
        await git("push", bare, "HEAD:refs/heads/feature");
        await git("reset", "--hard", sha);
        expect(await client.push({}, ctx, watch)).toMatchObject({
            push: "failed",
            ci: "not_started",
            result: { status: "exit_error" },
        });
        expect(starts).toBe(0);
    } finally {
        await rm(root, { recursive: true, force: true });
    }
});

test("general client preserves commands and stdin, isolates explicit hosts without auth probes", async () => {
    const calls: ProcessRequest[] = [];
    const client = new GitLabClient(async (request) => {
        calls.push(request);
        return result("{}");
    });
    const args = [
        "api",
        "projects/:id/merge_requests/1",
        "--method",
        "PUT",
        "--input",
        "-",
    ];
    for (const host of ["gitlab.com", "gitlab.example.test"]) {
        await client.execute(
            {
                args,
                host,
                repo: "nested/group/project",
                stdin: '{"description":"`literal`\\nbody"}',
            },
            context,
        );
    }
    expect(calls).toHaveLength(2);
    expect(calls[0]).toMatchObject({
        executable: "glab",
        args,
        stdin: '{"description":"`literal`\\nbody"}',
        env: {
            GITLAB_HOST: "gitlab.com",
            GITLAB_REPO: "gitlab.com/nested/group/project",
            GLAB_NO_PROMPT: "1",
            GIT_TERMINAL_PROMPT: "0",
        },
    });
    expect(calls[1]?.env?.GITLAB_REPO).toBe(
        "gitlab.example.test/nested/group/project",
    );
});

test("normalizes login, authorization and request failures without retrying mutations", async () => {
    for (const [stderr, error] of [
        ["You are not logged in. Run glab auth login", "authentication"],
        ["HTTP 403 Forbidden", "authorization"],
        ["HTTP 502 Bad Gateway", "request_failed"],
    ] as const) {
        let calls = 0;
        const client = new GitLabClient(async () => {
            calls++;
            return result("", stderr, 1);
        });
        expect(
            (await client.execute({ args: ["mr", "merge", "1"] }, context))
                .error,
        ).toBe(error);
        expect(calls).toBe(1);
    }
});

test("cache reads are byte bounded, session scoped, and distinguish overflow from eviction", () => {
    const cache = new OutputCache(1024, 2);
    const output = result("é".repeat(400));
    output.stdoutBytes = 2000;
    const id = cache.store("s", output);
    const page = cache.read("s", id, "stdout", 2, 10);
    expect(page).toMatchObject({
        status: "available",
        data: "é".repeat(5),
        offset: 2,
        nextOffset: 12,
        retainedBytes: 800,
        discardedBytes: 1200,
        truncated: true,
    });
    expect(cache.read("other", id).status).toBe("unavailable");
    expect(cache.read("s", id, "stdout", 800).discardedBytes).toBe(1200);
    cache.store("s", result("x".repeat(800)));
    expect(cache.read("s", id).status).toBe("unavailable");
    expect(() => cache.read("s", id, "stdout", -1)).toThrow();
});

test("binary output is base64; terminal controls are stripped; serialized pages fit 8 KiB", () => {
    const cache = new OutputCache();
    const binary = result();
    binary.stdout = Buffer.from([0, 255, 1, 2]);
    binary.stdoutBytes = 4;
    const id = cache.store("s", binary);
    expect(cache.read("s", id)).toMatchObject({
        encoding: "base64",
        data: "AP8BAg==",
    });
    const text = cache.store(
        "s",
        result("\x1b[31mred\x1b[0m\x1b]0;title\x07\n" + '"'.repeat(20000)),
    );
    const page = cache.read("s", text, "stdout", 0, 8192);
    expect(page.data).toStartWith("red\n");
    expect(Buffer.byteLength(JSON.stringify(page))).toBeLessThanOrEqual(8192);
    expect(page.nextOffset).toBeLessThan(8192);
    cache.deleteSession("s");
    expect(cache.read("s", text).status).toBe("unavailable");
});

test("entry count and clear bound empty result retention", () => {
    const cache = new OutputCache(1024, 2);
    const id = cache.store("s", result());
    cache.store("s", result());
    cache.store("s", result());
    expect(cache.read("s", id).status).toBe("unavailable");
    const next = cache.store("s", result());
    cache.clear();
    expect(cache.read("s", next).status).toBe("unavailable");
});

test("partial UTF-8 pages preserve the original bytes as base64", () => {
    const cache = new OutputCache();
    const output = result("text é");
    output.stdout = output.stdout.subarray(0, -1);
    const id = cache.store("s", output);
    const partial = cache.read("s", id);
    expect(partial.encoding).toBe("base64");
    expect([...Buffer.from(partial.data!, "base64")]).toEqual([
        ...output.stdout,
    ]);
    const splitID = cache.store("s", result("aéz"));
    const first = cache.read("s", splitID, "stdout", 0, 2);
    const second = cache.read("s", splitID, "stdout", first.nextOffset, 2);
    expect(first.encoding).toBe("base64");
    expect(second.encoding).toBe("base64");
    expect(
        Buffer.concat([
            Buffer.from(first.data!, "base64"),
            Buffer.from(second.data!, "base64"),
        ]).toString(),
    ).toBe("aéz");
});

test("registered tools use a real fake CLI, enforce deny, bound output, and release lifecycle resources", async () => {
    const root = await mkdtemp(join(tmpdir(), "opencode-gitlab-tools-"));
    const oldPath = process.env.PATH;
    const hooks = await plugin({
        client: createOpencodeClient({ baseUrl: "http://unused" }),
        directory: root,
        worktree: root,
        project: {
            id: "fixture",
            worktree: root,
            time: { created: 0 },
        },
        experimental_workspace: { register() {} },
        serverUrl: new URL("http://unused"),
        $: Bun.$,
    });
    const ctx = { ...context, directory: root, worktree: root };
    const calls: Parameters<ToolContext["ask"]>[0][] = [];
    const started = Promise.withResolvers<number>();
    const ready = Bun.serve({
        port: 0,
        hostname: "127.0.0.1",
        async fetch(request) {
            started.resolve(Number(await request.text()));
            return new Response("ready");
        },
    });
    try {
        await writeFile(
            join(root, "glab"),
            `#!${process.execPath}\nconst args=process.argv.slice(2);\nif(args[0]==='hang') { await fetch(${JSON.stringify(ready.url.toString())},{method:'POST',body:String(process.pid)}); setInterval(()=>{},1000); }\nelse if(args[0]==='large') { await Bun.write(Bun.stdout,Buffer.alloc(2000000,34)); await Bun.write(Bun.stderr,Buffer.alloc(2000000,34)); }\nelse if(args[1]?.endsWith('/pipelines/42')) console.log(JSON.stringify({ id:42,status:'running',sha:'a'.repeat(40),ref:'main',web_url:'https://self.test/a/b/-/pipelines/42' }));\nelse if(args[1]?.endsWith('/pipelines/43')) { console.error('HTTP 403 Forbidden'); process.exit(1); }\nelse console.log(JSON.stringify({args,stdin:await Bun.stdin.text(),host:process.env.GITLAB_HOST,repo:process.env.GITLAB_REPO,prompt:process.env.GLAB_NO_PROMPT}));\n`,
            { mode: 0o755 },
        );
        process.env.PATH = root + ":" + oldPath;
        const inspected = toolText(
            await hooks.tool.gitlab_inspect.execute(
                { url: "https://self.test/a/b/-/pipelines/42" },
                ctx,
            ),
        );
        expect(JSON.parse(inspected)).toMatchObject({
            target: { id: "42", host: "self.test", repo: "a/b" },
            rawStatus: "running",
        });
        expect(Buffer.byteLength(inspected)).toBeLessThanOrEqual(4096);
        const denied = JSON.parse(
            toolText(
                await hooks.tool.gitlab_inspect.execute(
                    { url: "https://self.test/a/b/-/pipelines/43" },
                    ctx,
                ),
            ),
        );
        expect(denied).toMatchObject({
            status: "exit_error",
            error: "authorization",
            stderr: { data: "HTTP 403 Forbidden\n" },
        });
        expect(
            JSON.parse(
                toolText(
                    await hooks.tool.gitlab_output.execute(
                        { resultID: denied.resultID, stream: "stderr" },
                        ctx,
                    ),
                ),
            ).data,
        ).toBe("HTTP 403 Forbidden\n");
        const body = "'quotes' `backticks` $(literal)\nbody";
        const output = await hooks.tool.gitlab.execute(
            {
                args: ["api", "projects/:id", "--input", "-"],
                host: "one.test",
                repo: "nested/group/project",
                stdin: body,
            },
            {
                ...ctx,
                async ask(request) {
                    calls.push(request);
                },
            },
        );
        const first = JSON.parse(toolText(output));
        expect(first.status).toBe("success");
        const page = JSON.parse(
            toolText(
                await hooks.tool.gitlab_output.execute(
                    { resultID: first.resultID, limit: 8192 },
                    ctx,
                ),
            ),
        );
        expect(JSON.parse(page.data)).toMatchObject({
            stdin: body,
            host: "one.test",
            repo: "one.test/nested/group/project",
            prompt: "1",
        });
        expect(calls.at(-1)?.patterns).toContain(
            "glab api projects/:id --input -",
        );
        await expect(
            hooks.tool.gitlab.execute(
                { args: ["mr", "merge", "1"] },
                {
                    ...ctx,
                    async ask(request) {
                        expect(request.patterns).toContain("glab mr merge 1");
                        throw new Error("deny glab mr merge *");
                    },
                },
            ),
        ).rejects.toThrow("deny glab mr merge");
        const large = toolText(
            await hooks.tool.gitlab.execute({ args: ["large"] }, ctx),
        );
        expect(Buffer.byteLength(large)).toBeLessThanOrEqual(1024);
        const summary = JSON.parse(large);
        expect(
            summary.stdout.discardedBytes + summary.stderr.discardedBytes,
        ).toBe(4000000 - 1024 * 1024);
        expect(
            Buffer.byteLength(
                toolText(
                    await hooks.tool.gitlab_output.execute(
                        { resultID: summary.resultID, limit: 8192 },
                        ctx,
                    ),
                ),
            ),
        ).toBeLessThanOrEqual(8192);
        const flight = hooks.tool.gitlab.execute({ args: ["hang"] }, ctx);
        const pid = await started.promise;
        await hooks.event({
            event: {
                type: "session.deleted",
                properties: {
                    info: {
                        id: ctx.sessionID,
                        projectID: "fixture",
                        directory: root,
                        title: "fixture",
                        version: "1",
                        time: { created: 0, updated: 0 },
                    },
                },
            },
        });
        expect(JSON.parse(toolText(await flight)).status).toBe("canceled");
        expect(() => process.kill(pid, 0)).toThrow();
        expect(
            JSON.parse(
                toolText(
                    await hooks.tool.gitlab_output.execute(
                        { resultID: first.resultID },
                        ctx,
                    ),
                ),
            ).status,
        ).toBe("unavailable");
        const asked = Promise.withResolvers<void>();
        const permission = Promise.withResolvers<void>();
        const pending = hooks.tool.gitlab.execute(
            { args: ["api", "user"] },
            {
                ...ctx,
                sessionID: "other",
                async ask() {
                    asked.resolve();
                    return permission.promise;
                },
            },
        );
        await asked.promise;
        await hooks.dispose();
        permission.resolve();
        expect(JSON.parse(toolText(await pending)).status).toBe("canceled");
    } finally {
        await hooks.dispose();
        process.env.PATH = oldPath;
        ready.stop(true);
        await rm(root, { recursive: true, force: true });
    }
}, 10000);

test.skipIf(!Bun.which("glab"))(
    "installed glab routes explicit nested repo and host to a loopback API without credentials",
    async () => {
        const root = await mkdtemp(join(tmpdir(), "opencode-gitlab-host-"));
        const paths: string[] = [];
        const server = Bun.serve({
            port: 0,
            hostname: "127.0.0.1",
            fetch(request): Response {
                paths.push(new URL(request.url).pathname);
                return Response.json({
                    id: 1,
                    status: "success",
                    sha: "a".repeat(40),
                    ref: "main",
                    web_url: `${server.url}nested/group/project/-/pipelines/1`,
                });
            },
        });
        const client = new GitLabClient((request, ctx) =>
            runProcess(
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
            ),
        );
        try {
            const response = await client.inspect(
                {
                    kind: "pipeline",
                    id: 1,
                    host: server.url.host,
                    repo: "nested/group/project",
                    cwd: root,
                },
                { ...context, directory: root, worktree: root },
            );
            expect(response.rawStatus).toBe("success");
            expect(paths).toContain(
                "/api/v4/projects/nested%2Fgroup%2Fproject/pipelines/1",
            );
        } finally {
            server.stop(true);
            await rm(root, { recursive: true, force: true });
        }
    },
);
