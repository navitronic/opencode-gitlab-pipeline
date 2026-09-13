import { expect, test } from "bun:test";
import { createOpencodeClient } from "@opencode-ai/sdk";
import { DeliveryQueue } from "../src/delivery";

function fixture() {
    let busy = false;
    let rejection = 0;
    let uncertain = false;
    let inserted = false;
    const prompts: Array<{ messageID: string; agent: string }> = [];
    const paths: string[] = [];
    const client = createOpencodeClient({
        baseUrl: "http://fixture",
        fetch: async (request) => {
            const req =
                request instanceof Request ? request : new Request(request);
            const path = new URL(req.url).pathname;
            paths.push(path);
            if (path === "/session/status")
                return Response.json(busy ? { origin: { type: "busy" } } : {});
            if (path.endsWith("/prompt_async")) {
                const body: { messageID: string; agent: string } =
                    await req.json();
                prompts.push(body);
                if (uncertain) throw new TypeError("connection lost");
                return new Response(null, { status: rejection || 204 });
            }
            return inserted
                ? Response.json({
                      info: { id: prompts[0]?.messageID },
                      parts: [],
                  })
                : new Response(null, { status: 404 });
        },
    });
    const queue = new DeliveryQueue(client, "/origin");
    const result = {
        watchID: "watch-1",
        sessionID: "origin",
        agent: "build",
        text: "Pipeline passed",
    };
    return {
        queue,
        result,
        prompts,
        paths,
        setBusy: (value: boolean) => {
            busy = value;
        },
        reject: (value: number) => {
            rejection = value;
        },
        uncertain: () => {
            uncertain = true;
        },
        inserted: () => {
            inserted = true;
        },
    };
}

test("idle delivers once to the originating session and agent", async () => {
    const f = fixture();
    try {
        f.queue.enqueue(f.result);
        await f.queue.flush("origin");
        f.queue.enqueue(f.result);
        await f.queue.flush("origin");
        expect(f.prompts).toHaveLength(1);
        expect(f.prompts[0]?.agent).toBe("build");
        expect(f.paths).toContain("/session/origin/prompt_async");
        expect(f.queue.inspect("watch-1")?.state).toBe("delivered");
    } finally {
        f.queue.dispose();
    }
});

test("busy and unacknowledged results wait, including concurrent drains", async () => {
    const f = fixture();
    const ack = Promise.withResolvers<void>();
    try {
        f.setBusy(true);
        f.queue.enqueue(f.result, ack.promise);
        await f.queue.flush("origin");
        expect(f.prompts).toHaveLength(0);
        ack.resolve();
        await f.queue.flush("origin");
        expect(f.prompts).toHaveLength(0);
        f.setBusy(false);
        await Promise.all([f.queue.flush("origin"), f.queue.flush("origin")]);
        expect(f.prompts).toHaveLength(1);
    } finally {
        f.queue.dispose();
    }
});

test("status reconciliation recovers from a missed idle event", async () => {
    const f = fixture();
    try {
        f.setBusy(true);
        await f.queue.hooks.event?.({
            event: {
                type: "session.status",
                properties: { sessionID: "origin", status: { type: "busy" } },
            },
        });
        f.queue.enqueue(f.result);
        await f.queue.flush("origin");
        expect(f.prompts).toHaveLength(0);
        f.setBusy(false);
        await f.queue.flush("origin");
        expect(f.prompts).toHaveLength(1);
    } finally {
        f.queue.dispose();
    }
});

test("tool hooks block completion before acknowledgment even if status says idle", async () => {
    const f = fixture();
    const input = { sessionID: "origin", callID: "call", tool: "probe" };
    try {
        await f.queue.hooks["tool.execute.before"]?.(input, { args: {} });
        f.queue.enqueue(f.result);
        await f.queue.flush("origin");
        expect(f.prompts).toHaveLength(0);
        await f.queue.hooks["tool.execute.after"]?.(
            { ...input, args: {} },
            { title: "probe", output: "ack", metadata: {} },
        );
        await f.queue.flush("origin");
        expect(f.prompts).toHaveLength(1);
    } finally {
        f.queue.dispose();
    }
});

test("idle after a failed tool clears the guard even without an after hook", async () => {
    const f = fixture();
    try {
        await f.queue.hooks["tool.execute.before"]?.(
            { sessionID: "origin", callID: "failed-call", tool: "probe" },
            { args: {} },
        );
        f.queue.enqueue(f.result);
        await f.queue.flush("origin");
        expect(f.prompts).toHaveLength(0);
        await f.queue.hooks.event?.({
            event: {
                type: "session.status",
                properties: { sessionID: "origin", status: { type: "idle" } },
            },
        });
        await f.queue.flush("origin");
        expect(f.prompts).toHaveLength(1);
    } finally {
        f.queue.dispose();
    }
});

test("known rejection stays pending and retry uses the same message ID", async () => {
    const f = fixture();
    try {
        f.reject(400);
        f.queue.enqueue(f.result);
        await f.queue.flush("origin");
        expect(f.queue.inspect("watch-1")?.state).toBe("pending");
        f.reject(0);
        await f.queue.flush("origin");
        expect(f.prompts).toHaveLength(2);
        expect(f.prompts[0]?.messageID).toBe(f.prompts[1]?.messageID);
        expect(f.queue.inspect("watch-1")?.state).toBe("delivered");
    } finally {
        f.queue.dispose();
    }
});

test("uncertain acceptance reconciles without another insertion", async () => {
    const f = fixture();
    try {
        f.uncertain();
        f.queue.enqueue(f.result);
        await f.queue.flush("origin");
        expect(f.queue.inspect("watch-1")?.state).toBe("delivery_uncertain");
        await f.queue.flush("origin");
        expect(f.prompts).toHaveLength(1);
        f.inserted();
        await f.queue.flush("origin");
        expect(f.queue.inspect("watch-1")?.state).toBe("delivered");
        expect(f.prompts).toHaveLength(1);
    } finally {
        f.queue.dispose();
    }
});

test("separate sessions retain their own agent context", async () => {
    const f = fixture();
    try {
        f.queue.enqueue(f.result);
        f.queue.enqueue({
            ...f.result,
            watchID: "watch-2",
            sessionID: "other",
            agent: "plan",
        });
        await Promise.all([f.queue.flush("origin"), f.queue.flush("other")]);
        expect(f.paths).toContain("/session/origin/prompt_async");
        expect(f.paths).toContain("/session/other/prompt_async");
        expect(f.prompts.map((prompt) => prompt.agent).sort()).toEqual([
            "build",
            "plan",
        ]);
    } finally {
        f.queue.dispose();
    }
});

test("disposal during status reconciliation prevents the prompt", async () => {
    const started = Promise.withResolvers<void>();
    const status = Promise.withResolvers<Response>();
    let prompts = 0;
    let signal: AbortSignal | undefined;
    const client = createOpencodeClient({
        baseUrl: "http://fixture",
        fetch: async (request) => {
            const req =
                request instanceof Request ? request : new Request(request);
            signal = req.signal;
            if (req.url.endsWith("/session/status?directory=%2Forigin")) {
                started.resolve();
                return status.promise;
            }
            prompts++;
            return new Response(null, { status: 204 });
        },
    });
    const queue = new DeliveryQueue(client, "/origin");
    queue.enqueue({
        watchID: "watch",
        sessionID: "origin",
        agent: "build",
        text: "done",
    });
    const flight = queue.flush("origin");
    await started.promise;
    queue.dispose();
    expect(signal?.aborted).toBe(true);
    status.resolve(Response.json({}));
    await flight;
    expect(prompts).toBe(0);
});

test("pending capacity rejects excess work and text is byte bounded", () => {
    const f = fixture();
    const ack = new Promise<void>(() => {});
    try {
        expect(() =>
            f.queue.enqueue({ ...f.result, text: "é".repeat(2049) }, ack),
        ).toThrow("4 KiB");
        for (let i = 0; i < 256; i++)
            f.queue.enqueue({ ...f.result, watchID: String(i) }, ack);
        expect(() => f.queue.enqueue(f.result, ack)).toThrow("full");
    } finally {
        f.queue.dispose();
    }
});

test("session deletion and disposal cancel owned resources and pending acknowledgments", async () => {
    const f = fixture();
    const ack = Promise.withResolvers<void>();
    let released = 0;
    f.queue.own("origin", () => {
        released++;
    });
    f.queue.enqueue(f.result, ack.promise);
    f.queue.deleteSession("origin");
    ack.resolve();
    await f.queue.flush("origin");
    expect(f.prompts).toHaveLength(0);
    expect(released).toBe(1);
    f.queue.own("other", () => {
        released++;
    });
    f.queue.dispose();
    f.queue.dispose();
    expect(released).toBe(2);
    expect(() => f.queue.enqueue({ ...f.result, watchID: "late" })).toThrow(
        "disposed",
    );
});
