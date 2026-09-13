import { expect, test } from "bun:test";
import { mkdtemp, mkdir, writeFile, rm } from "node:fs/promises";
import { join } from "node:path";
import { tmpdir } from "node:os";
import { createOpencodeClient } from "@opencode-ai/sdk";
import plugin from "../src/index";

test("inspect is registered, permission-gated, and disposal cancels pending permission", async () => {
    const hooks = await plugin({
        client: createOpencodeClient({ baseUrl: "http://unused" }),
        directory: process.cwd(),
        worktree: process.cwd(),
        project: {
            id: "fixture",
            worktree: process.cwd(),
            time: { created: 0 },
        },
        experimental_workspace: { register() {} },
        serverUrl: new URL("http://unused"),
        $: Bun.$,
    });
    expect(Object.keys(hooks.tool)).toContain("gitlab_inspect");
    expect(Object.keys(hooks.tool).sort()).toEqual([
        "gitlab",
        "gitlab_inspect",
        "gitlab_output",
        "gitlab_push",
        "gitlab_watch",
    ]);
    const ctx = {
        sessionID: "s",
        messageID: "m",
        agent: "build",
        directory: process.cwd(),
        worktree: process.cwd(),
        abort: new AbortController().signal,
        metadata() {},
        async ask() {
            throw new Error("permission denied");
        },
    };
    try {
        await expect(
            hooks.tool.gitlab_inspect.execute(
                { url: "https://self.test/a/b/-/jobs/1" },
                ctx,
            ),
        ).rejects.toThrow("permission denied");
        const asked = Promise.withResolvers<void>();
        const pending = hooks.tool.gitlab_inspect.execute(
            { url: "https://self.test/a/b/-/jobs/1" },
            {
                ...ctx,
                async ask(permission) {
                    expect(permission.metadata.args).toEqual([
                        "api",
                        "projects/a%2Fb/jobs/1",
                        "--method",
                        "GET",
                    ]);
                    expect(permission.metadata.env).toMatchObject({
                        GITLAB_HOST: "self.test",
                        GITLAB_REPO: "self.test/a/b",
                    });
                    asked.resolve();
                    return new Promise<void>(() => {});
                },
            },
        );
        await asked.promise;
        await hooks.dispose();
        expect(JSON.parse(String(await pending)).status).toBe("canceled");
    } finally {
        await hooks.dispose();
    }
});

test.skipIf(process.env.OPENCODE_RUNTIME_SMOKE !== "1")(
    "OpenCode 1.18.30: idle wake, busy tool completion, and disposal",
    async () => {
        const version = Bun.spawn(["opencode", "--version"], {
            stdout: "pipe",
        });
        expect((await new Response(version.stdout).text()).trim()).toBe(
            "1.18.30",
        );
        expect(await version.exited).toBe(0);
        const root = await mkdtemp(join(tmpdir(), "opencode-gitlab-runtime-"));
        const events: Array<{ kind: string; [key: string]: unknown }> = [];
        const listeners = new Set<() => void>();
        function waitFor(predicate: () => boolean): Promise<void> {
            if (predicate()) return Promise.resolve();
            return new Promise((resolve, reject) => {
                const timer = setTimeout(() => {
                    listeners.delete(check);
                    reject(
                        new Error(
                            `Timed out; events: ${JSON.stringify(events)}`,
                        ),
                    );
                }, 20000);
                function check() {
                    if (!predicate()) return;
                    clearTimeout(timer);
                    listeners.delete(check);
                    resolve();
                }
                listeners.add(check);
            });
        }
        let complete: ((value: Response) => void) | undefined;
        let release: ((value: Response) => void) | undefined;
        let pipelineReads = 0;
        async function sendCompletion(id: string, wait = false) {
            await waitFor(() => !!complete);
            const send = complete;
            complete = undefined;
            send?.(Response.json({ id, wait }));
        }
        const stub = Bun.serve({
            port: 0,
            idleTimeout: 0,
            hostname: "127.0.0.1",
            async fetch(request): Promise<Response> {
                const path = new URL(request.url).pathname;
                if (path === "/api/v4/projects/group%2Frepo/pipelines/12") {
                    pipelineReads++;
                    return Response.json({
                        id: 12,
                        status: pipelineReads === 1 ? "running" : "success",
                        sha: "a".repeat(40),
                        ref: "main",
                        web_url: `${stub.url}group/repo/-/pipelines/12`,
                    });
                }
                if (path === "/events") {
                    const event: { kind: string } = await request.json();
                    events.push(event);
                    for (const listener of listeners) listener();
                    return new Response("ok");
                }
                if (path === "/completion") {
                    return new Promise<Response>((resolve) => {
                        complete = resolve;
                        events.push({ kind: "source-ready" });
                        for (const listener of listeners) listener();
                    });
                }
                if (path === "/hold") {
                    return new Promise<Response>((resolve) => {
                        release = resolve;
                        events.push({ kind: "holding" });
                        for (const listener of listeners) listener();
                    });
                }
                const body: {
                    messages: Array<{ role: string; content: unknown }>;
                } = await request.json();
                const last = body.messages.at(-1);
                const watchTool =
                    last?.role === "user" &&
                    JSON.stringify(last.content).includes("watch-probe");
                const useTool =
                    watchTool ||
                    (last?.role === "user" &&
                        JSON.stringify(last.content).includes("busy-probe"));
                const delta = useTool
                    ? {
                          tool_calls: [
                              {
                                  index: 0,
                                  id: "call_probe",
                                  type: "function",
                                  function: watchTool
                                      ? {
                                            name: "gitlab_watch",
                                            arguments: JSON.stringify({
                                                action: "start",
                                                url: `${stub.url}group/repo/-/pipelines/12`,
                                            }),
                                        }
                                      : { name: "hold", arguments: "{}" },
                              },
                          ],
                      }
                    : { content: "probe-response" };
                const chunk = (value: unknown, finish: string | null) =>
                    `data: ${JSON.stringify({ id: "chatcmpl-probe", object: "chat.completion.chunk", created: 1, model: "probe", choices: [{ index: 0, delta: value, finish_reason: finish }] })}\n\n`;
                return new Response(
                    chunk(delta, null) +
                        chunk({}, useTool ? "tool_calls" : "stop") +
                        "data: [DONE]\n\n",
                    {
                        headers: { "content-type": "text/event-stream" },
                    },
                );
            },
        });
        const endpoint = stub.url.toString();
        const pluginPath = join(root, "probe.ts");
        await writeFile(
            pluginPath,
            `
import { tool } from ${JSON.stringify(join(process.cwd(), "node_modules/@opencode-ai/plugin/dist/index.js"))};
import type { Plugin } from ${JSON.stringify(join(process.cwd(), "node_modules/@opencode-ai/plugin/dist/index.js"))};
import { DeliveryQueue } from ${JSON.stringify(join(process.cwd(), "src/delivery.ts"))};
import { spawn } from "node:child_process";
export default (async ({ client, directory }) => {
  const controller = new AbortController();
  const delivery = new DeliveryQueue(client, directory);
  const child = spawn("/bin/cat", [], { stdio: "pipe" });
  const exited = new Promise<void>((resolve) => child.once("exit", () => resolve()));
  const timer = setInterval(() => {}, 60000);
  delivery.own("__plugin", () => { controller.abort(); clearInterval(timer); child.kill(); });
  const emit = (event: Record<string, unknown>) => fetch(${JSON.stringify(endpoint)} + "events", { method: "POST", body: JSON.stringify(event) });
  let count = 0;
  void (async () => {
    while (!controller.signal.aborted) {
      const response = await fetch(${JSON.stringify(endpoint)} + "completion", { signal: controller.signal });
      const { id, wait }: { id: string; wait?: boolean } = await response.json();
      const watchID = "runtime-" + (++count);
      delivery.own(id, () => { void emit({ kind: "session-released", id }); });
      delivery.enqueue({ watchID, sessionID: id, agent: "build", model: { providerID: "probe", modelID: "probe" }, text: "completion-probe" }, wait ? new Promise<void>(() => {}) : Promise.resolve());
      await delivery.flush(id);
      await emit({ kind: "queued", id, state: delivery.inspect(watchID)?.state, messageID: delivery.inspect(watchID)?.messageID });
    }
  })().catch(() => {});
  return {
    ...delivery.hooks,
    tool: { hold: tool({ description: "Hold a probe tool", args: {}, async execute(_, context) {
      await emit({ kind: "tool-start", id: context.sessionID });
      await fetch(${JSON.stringify(endpoint)} + "hold", { signal: context.abort });
      await emit({ kind: "tool-finished", aborted: context.abort.aborted });
      return "tool-acknowledged";
    } }) },
    async event({ event }) {
      await delivery.hooks.event?.({ event });
      if (event.type === "session.status") {
        await emit({ kind: "status", id: event.properties.sessionID, status: event.properties.status.type });
      }
      if (event.type === "message.updated" && event.properties.info.role === "user") {
        await emit({ kind: "user-message", id: event.properties.info.id });
      }
      if (event.type === "message.updated" && event.properties.info.role === "assistant" && event.properties.info.time.completed) {
        await emit({ kind: "assistant-finished", parentID: event.properties.info.parentID, error: event.properties.info.error });
      }
      if (event.type === "message.part.updated" && event.properties.part.type === "tool" && event.properties.part.state.status === "completed") {
        await emit({ kind: "tool-ack", output: event.properties.part.state.output });
      }
      if (event.type === "message.part.updated" && event.properties.part.type === "text" && event.properties.part.synthetic && event.properties.part.text.includes('"outcome"')) {
        await emit({ kind: "watch-completion", messageID: event.properties.part.messageID, text: event.properties.part.text });
      }
    },
    async dispose() {
      await delivery.hooks.dispose?.();
      await exited;
      await emit({ kind: "disposed", aborted: controller.signal.aborted, pending: delivery.inspect("runtime-" + count) ? 1 : 0, childExited: child.exitCode !== null || child.signalCode !== null });
    }
  };
}) satisfies Plugin;
`,
        );
        for (const name of ["config", "data", "cache", "state", "home", "work"])
            await mkdir(join(root, name));
        const config = {
            $schema: "https://opencode.ai/config.json",
            autoupdate: false,
            share: "disabled",
            snapshot: false,
            plugin: [join(process.cwd(), "dist/index.js"), pluginPath],
            model: "probe/probe",
            small_model: "probe/probe",
            enabled_providers: ["probe"],
            provider: {
                probe: {
                    npm: "@ai-sdk/openai-compatible",
                    name: "Probe",
                    options: { baseURL: endpoint + "v1", apiKey: "synthetic" },
                    models: {
                        probe: {
                            name: "Probe",
                            tool_call: true,
                            limit: { context: 32000, output: 1000 },
                        },
                    },
                },
            },
            permission: "allow",
        };
        await writeFile(join(root, "config.json"), JSON.stringify(config));
        const child = Bun.spawn(
            ["opencode", "serve", "--hostname", "127.0.0.1", "--port", "0"],
            {
                cwd: join(root, "work"),
                env: {
                    ...process.env,
                    HOME: join(root, "home"),
                    XDG_CONFIG_HOME: join(root, "config"),
                    XDG_DATA_HOME: join(root, "data"),
                    XDG_CACHE_HOME: join(root, "cache"),
                    XDG_STATE_HOME: join(root, "state"),
                    OPENCODE_CONFIG_DIR: join(root, "config"),
                    OPENCODE_CONFIG: join(root, "config.json"),
                    OPENCODE_CONFIG_CONTENT: "{}",
                    OPENCODE_DISABLE_DEFAULT_PLUGINS: "1",
                    OPENCODE_DISABLE_PROJECT_CONFIG: "1",
                    OPENCODE_DISABLE_EXTERNAL_SKILLS: "1",
                    OPENCODE_DISABLE_CLAUDE_CODE_SKILLS: "1",
                    OPENCODE_SERVER_PASSWORD: "",
                    GLAB_CONFIG_DIR: join(root, "config"),
                    GITLAB_TOKEN: "fixture-only",
                    GLAB_API_PROTOCOL: "http",
                    API_PROTOCOL: "http",
                },
                stdout: "pipe",
                stderr: "pipe",
            },
        );
        const stderr = new Response(child.stderr).text();
        let stdout = "";
        let serverURL = "";
        const reading = (async () => {
            for await (const chunk of child.stdout) {
                stdout += new TextDecoder().decode(chunk);
                serverURL =
                    stdout.match(/http:\/\/127\.0\.0\.1:\d+/)?.[0] ?? "";
                for (const listener of listeners) listener();
            }
        })();
        try {
            await waitFor(() => !!serverURL);
            const client = createOpencodeClient({
                baseUrl: serverURL,
                directory: join(root, "work"),
            });
            const session = await client.session.create({
                body: { title: "U1 isolated smoke" },
                throwOnError: true,
            });
            const id = session.data.id;
            await waitFor(() => !!complete);
            await sendCompletion(id);
            await waitFor(() =>
                events.some(
                    (e) => e.kind === "queued" && e.state === "delivered",
                ),
            );
            await waitFor(() =>
                events.some((e) => e.kind === "status" && e.status === "idle"),
            );
            const messages = await client.session.messages({
                path: { id },
                throwOnError: true,
            });
            expect(
                messages.data.filter((m) => m.info.role === "assistant"),
            ).toHaveLength(1);
            await client.session.promptAsync({
                path: { id },
                body: { parts: [{ type: "text", text: "busy-probe" }] },
                throwOnError: true,
            });
            await waitFor(() => !!release);
            await sendCompletion(id);
            await waitFor(
                () => events.filter((e) => e.kind === "queued").length === 2,
            );
            expect(
                events.filter((e) => e.kind === "user-message"),
            ).toHaveLength(2);
            release?.(new Response("release"));
            const messageID = events.filter((e) => e.kind === "queued")[1]
                ?.messageID;
            await waitFor(() =>
                events.some(
                    (e) => e.kind === "user-message" && e.id === messageID,
                ),
            );
            expect(
                events.find((e) => e.kind === "tool-finished")?.aborted,
            ).toBe(false);
            expect(
                events.findIndex((e) => e.kind === "tool-finished"),
            ).toBeLessThan(
                events.findIndex(
                    (e) => e.kind === "user-message" && e.id === messageID,
                ),
            );
            expect(events.find((e) => e.kind === "tool-ack")?.output).toBe(
                "tool-acknowledged",
            );
            expect(events.findIndex((e) => e.kind === "tool-ack")).toBeLessThan(
                events.findIndex(
                    (e) => e.kind === "user-message" && e.id === messageID,
                ),
            );
            await waitFor(() =>
                events.some(
                    (e) =>
                        e.kind === "assistant-finished" &&
                        e.parentID === messageID,
                ),
            );
            expect(
                events.filter(
                    (e) =>
                        e.kind === "assistant-finished" &&
                        e.parentID === messageID,
                ),
            ).toHaveLength(1);
            expect(
                events.some((e) => e.kind === "assistant-finished" && e.error),
            ).toBe(false);
            await client.session.promptAsync({
                path: { id },
                body: { parts: [{ type: "text", text: "watch-probe" }] },
                throwOnError: true,
            });
            await waitFor(() =>
                events.some((e) => e.kind === "watch-completion"),
            );
            const watchEvent = events.find(
                (e) => e.kind === "watch-completion",
            )!;
            const watchResult = JSON.parse(String(watchEvent.text));
            expect(watchResult.outcome).toBe("success");
            expect(watchResult.target.id).toBe("12");
            expect(pipelineReads).toBe(2);
            await waitFor(() =>
                events.some(
                    (e) =>
                        e.kind === "assistant-finished" &&
                        e.parentID === watchEvent.messageID,
                ),
            );
            expect(
                events.filter((e) => e.kind === "watch-completion"),
            ).toHaveLength(1);
            expect(
                events.filter(
                    (e) =>
                        e.kind === "tool-ack" &&
                        String(e.output).includes(watchResult.watchID),
                ),
            ).toHaveLength(1);
            const deleted = await client.session.create({
                body: { title: "U1 deletion" },
                throwOnError: true,
            });
            await sendCompletion(deleted.data.id, true);
            await waitFor(
                () => events.filter((e) => e.kind === "queued").length === 3,
            );
            await client.session.delete({
                path: { id: deleted.data.id },
                throwOnError: true,
            });
            await waitFor(() =>
                events.some(
                    (e) =>
                        e.kind === "session-released" &&
                        e.id === deleted.data.id,
                ),
            );
            await sendCompletion(id, true);
            await waitFor(
                () => events.filter((e) => e.kind === "queued").length === 4,
            );
            const response = await fetch(serverURL + "/instance/dispose", {
                method: "POST",
                headers: { "x-opencode-directory": join(root, "work") },
            });
            expect(response.ok).toBe(true);
            await waitFor(() => events.some((e) => e.kind === "disposed"));
            expect(events.find((e) => e.kind === "disposed")).toMatchObject({
                aborted: true,
                pending: 0,
                childExited: true,
            });
            const pendingIDs = events
                .filter((e) => e.kind === "queued")
                .slice(2)
                .map((e) => e.messageID);
            expect(
                events.some(
                    (e) =>
                        e.kind === "user-message" && pendingIDs.includes(e.id),
                ),
            ).toBe(false);
            console.log("REAL RUNTIME EVIDENCE", JSON.stringify(events));
        } finally {
            if (serverURL && !events.some((e) => e.kind === "disposed")) {
                await fetch(serverURL + "/instance/dispose", {
                    method: "POST",
                    headers: { "x-opencode-directory": join(root, "work") },
                    signal: AbortSignal.timeout(5000),
                }).catch(() => {});
            }
            complete?.(new Response("closed", { status: 410 }));
            release?.(new Response("closed"));
            child.kill();
            await child.exited;
            await reading;
            const errors = await stderr;
            stub.stop(true);
            await rm(root, { recursive: true, force: true });
            if (!events.some((e) => e.kind === "disposed"))
                console.error(stdout, errors);
        }
    },
    90000,
);
