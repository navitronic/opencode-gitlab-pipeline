import { expect, test } from "bun:test";
import type { ToolContext } from "@opencode-ai/plugin";
import { runProcess } from "../src/process";

const context = (ask: ToolContext["ask"] = async () => {}): ToolContext => ({
    sessionID: "session",
    messageID: "message",
    agent: "build",
    directory: process.cwd(),
    worktree: process.cwd(),
    abort: new AbortController().signal,
    metadata() {},
    ask,
});

test("argv and stdin are literal, permission describes the actual subprocess", async () => {
    const requests: Parameters<ToolContext["ask"]>[0][] = [];
    const body = "quotes '\" `code` $(touch nope)\nsecond line";
    const args = [
        "-e",
        "console.log(JSON.stringify(process.argv.slice(1))); process.stdin.pipe(process.stdout)",
        body,
    ];
    const result = await runProcess(
        { executable: process.execPath, args, stdin: body },
        context(async (request) => {
            requests.push(request);
        }),
    );
    expect(result.status).toBe("success");
    expect(result.stdout.toString()).toBe(JSON.stringify([body]) + "\n" + body);
    expect(requests.at(-1)).toMatchObject({
        permission: "bash",
        metadata: {
            executable: process.execPath,
            args,
            stdin: body,
            cwd: process.cwd(),
        },
    });
});

test("deny and abort during permission execute nothing", async () => {
    const request = { executable: "/missing-u2-executable", args: [] };
    await expect(
        runProcess(
            request,
            context(async () => {
                throw new Error("denied");
            }),
        ),
    ).rejects.toThrow("denied");
    const controller = new AbortController();
    const result = await runProcess(request, {
        ...context(async () => {
            controller.abort();
        }),
        abort: controller.signal,
    });
    expect(result.status).toBe("canceled");
});

test("missing executable is distinct from command failure", async () => {
    expect(
        (
            await runProcess(
                { executable: "/missing-u2-executable", args: [] },
                context(),
            )
        ).status,
    ).toBe("not_found");
    const result = await runProcess(
        {
            executable: process.execPath,
            args: ["-e", "console.error('request failed'); process.exit(7)"],
        },
        context(),
    );
    expect(result).toMatchObject({ status: "exit_error", exitCode: 7 });
    expect(result.stderr.toString()).toContain("request failed");
});

test("both pipes drain while combined retention stays bounded", async () => {
    const result = await runProcess(
        {
            executable: process.execPath,
            args: [
                "-e",
                "process.stdout.write(Buffer.alloc(2000000, 65)); process.stderr.write(Buffer.alloc(2000000, 66))",
            ],
            maxBytes: 1024,
        },
        context(),
    );
    expect(result.status).toBe("success");
    expect(result.stdout.length + result.stderr.length).toBe(1024);
    expect(result.stdoutBytes + result.stderrBytes).toBe(4000000);
});

test("timeout kills a process group including a child holding pipes open", async () => {
    const result = await runProcess(
        {
            executable: process.execPath,
            args: [
                "-e",
                "const {spawn}=require('node:child_process'); const c=spawn(process.execPath,['-e','setInterval(()=>{},1000)'],{stdio:['ignore',1,2]}); console.log(c.pid); setInterval(()=>{},1000)",
            ],
            timeoutMs: 500,
        },
        context(),
    );
    expect(result.status).toBe("timeout");
    const pid = Number(result.stdout.toString().trim());
    expect(pid).toBeGreaterThan(0);
    expect(() => process.kill(pid, 0)).toThrow();
}, 5000);

test("abort kills a live streamed process without waiting for its timeout", async () => {
    const controller = new AbortController();
    const ready = Bun.serve({
        port: 0,
        hostname: "127.0.0.1",
        fetch() {
            controller.abort();
            return new Response("ready");
        },
    });
    try {
        const running = runProcess(
            {
                executable: process.execPath,
                args: [
                    "-e",
                    `fetch(${JSON.stringify(ready.url.toString())}); setInterval(()=>process.stdout.write('x'),1)`,
                ],
                signal: controller.signal,
            },
            context(),
        );
        expect((await running).status).toBe("canceled");
    } finally {
        ready.stop(true);
    }
});

test("cancellation releases a pending permission without dispatching later", async () => {
    const entered = Promise.withResolvers<void>();
    const permission = Promise.withResolvers<void>();
    const controller = new AbortController();
    const running = runProcess(
        {
            executable: "/missing-u2-executable",
            args: [],
            signal: controller.signal,
        },
        context(async () => {
            entered.resolve();
            return permission.promise;
        }),
    );
    await entered.promise;
    controller.abort();
    try {
        const result = await Promise.race([
            running,
            new Promise<never>((_, reject) => {
                const timer = setTimeout(
                    () =>
                        reject(new Error("Cancellation blocked on permission")),
                    500,
                );
                timer.unref();
            }),
        ]);
        expect(result.status).toBe("canceled");
    } finally {
        permission.resolve();
        await running;
    }
});

test("large stdout cannot hide a final stderr error", async () => {
    const result = await runProcess(
        {
            executable: process.execPath,
            args: [
                "-e",
                "await Bun.write(Bun.stdout, Buffer.alloc(2000000,65)); console.error('HTTP 401 Unauthorized'); process.exitCode=1",
            ],
            maxBytes: 1024,
        },
        context(),
    );
    expect(result.stderr.toString()).toContain("HTTP 401 Unauthorized");
    expect(result.stdout.length + result.stderr.length).toBeLessThanOrEqual(
        1024,
    );
});

test("permission approval cannot be reused for argv changed while awaiting approval", async () => {
    const args = ["-e", "console.log('approved')"];
    const result = await runProcess(
        { executable: process.execPath, args },
        context(async () => {
            args[1] = "console.log('changed')";
        }),
    );
    expect(result.stdout.toString()).toBe("approved\n");
});

test("external working directory denial prevents subprocess permission and execution", async () => {
    const requests: string[] = [];
    await expect(
        runProcess(
            { executable: "/missing-u2-executable", args: [], cwd: "/" },
            context(async (request) => {
                requests.push(request.permission);
                expect(request.patterns).toContain("/*");
                throw new Error("external directory denied");
            }),
        ),
    ).rejects.toThrow("external directory denied");
    expect(requests).toEqual(["external_directory"]);
});
