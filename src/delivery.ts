import type { Hooks, PluginInput } from "@opencode-ai/plugin";

export interface Completion {
    watchID: string;
    sessionID: string;
    agent: string;
    model?: { providerID: string; modelID: string };
    text: string;
}

type State = "pending" | "delivering" | "delivered" | "delivery_uncertain";
interface Entry {
    result: Completion;
    messageID: string;
    state: State;
    ready: boolean;
    attempts: number;
}

/** One queue per plugin instance/directory. Retention and pending work are bounded. */
export class DeliveryQueue {
    private readonly entries = new Map<string, Entry>();
    private readonly flights = new Map<string, Promise<void>>();
    private readonly timers = new Map<string, ReturnType<typeof setTimeout>>();
    private readonly sessions = new Map<string, AbortController>();
    private readonly resources = new Map<string, Set<() => void>>();
    private readonly busy = new Set<string>();
    private readonly tools = new Map<string, Set<string>>();
    private disposed = false;

    constructor(
        private readonly client: PluginInput["client"],
        private readonly directory: string,
    ) {}

    enqueue(
        result: Completion,
        acknowledged: Promise<void> = Promise.resolve(),
    ): string {
        if (this.disposed) throw new Error("Delivery queue disposed");
        const existing = this.entries.get(result.watchID);
        if (existing) return existing.messageID;
        if (new TextEncoder().encode(result.text).byteLength > 4096)
            throw new Error("Completion exceeds 4 KiB");
        if (this.entries.size >= 256) {
            const oldest = [...this.entries].find(
                ([, entry]) => entry.state === "delivered",
            );
            if (!oldest) throw new Error("Delivery queue full");
            this.entries.delete(oldest[0]);
            const sessionID = oldest[1].result.sessionID;
            if (
                ![...this.entries.values()].some(
                    (entry) => entry.result.sessionID === sessionID,
                )
            )
                this.sessions.delete(sessionID);
        }
        const entry: Entry = {
            result: { ...result, model: result.model && { ...result.model } },
            messageID: `msg_${BigInt.asUintN(48, BigInt(Date.now()) * 0x1000n)
                .toString(16)
                .padStart(
                    12,
                    "0",
                )}${crypto.randomUUID().replaceAll("-", "").slice(0, 14)}`,
            state: "pending",
            ready: false,
            attempts: 0,
        };
        this.entries.set(result.watchID, entry);
        if (!this.sessions.has(result.sessionID))
            this.sessions.set(result.sessionID, new AbortController());
        void acknowledged.then(
            () => {
                if (this.disposed || this.entries.get(result.watchID) !== entry)
                    return;
                entry.ready = true;
                void (
                    this.flights.get(result.sessionID) ?? Promise.resolve()
                ).then(() => this.flush(result.sessionID));
            },
            () => {
                this.entries.delete(result.watchID);
            },
        );
        return entry.messageID;
    }

    inspect(
        watchID: string,
    ):
        | Readonly<{ messageID: string; state: State; attempts: number }>
        | undefined {
        const entry = this.entries.get(watchID);
        return (
            entry && {
                messageID: entry.messageID,
                state: entry.state,
                attempts: entry.attempts,
            }
        );
    }

    /** Register synchronous cancellation (abort child, clear timer); unregister on normal completion. */
    own(sessionID: string, release: () => void): () => void {
        if (this.disposed) {
            release();
            return () => {};
        }
        const owned = this.resources.get(sessionID) ?? new Set<() => void>();
        owned.add(release);
        this.resources.set(sessionID, owned);
        return () => {
            owned.delete(release);
            if (!owned.size) this.resources.delete(sessionID);
        };
    }

    async flush(sessionID: string): Promise<void> {
        // Let an already-resolved acknowledgment settle before choosing pending work.
        await Promise.resolve();
        if (this.disposed) return;
        const running = this.flights.get(sessionID);
        if (running) return running;
        const flight = this.drain(sessionID).finally(() => {
            this.flights.delete(sessionID);
        });
        this.flights.set(sessionID, flight);
        return flight;
    }

    private async drain(sessionID: string): Promise<void> {
        const signal = this.sessions.get(sessionID)?.signal;
        if (!signal || signal.aborted) return;
        const query = { directory: this.directory };
        for (const entry of this.entries.values()) {
            if (
                entry.result.sessionID !== sessionID ||
                !entry.ready ||
                entry.state === "delivered"
            )
                continue;
            if (signal.aborted || this.disposed) return;
            if (entry.state === "delivery_uncertain") {
                try {
                    const response = await this.client.session.message({
                        path: { id: sessionID, messageID: entry.messageID },
                        query,
                        signal,
                    });
                    if (
                        !signal.aborted &&
                        response.data?.info.id === entry.messageID
                    )
                        entry.state = "delivered";
                } catch {
                    /* Unknown acceptance stays inspectable; never blindly insert again. */
                }
                continue;
            }
            if (this.tools.get(sessionID)?.size) return;
            // The SDK snapshot is authoritative; events guard changes during this request.
            this.busy.delete(sessionID);
            try {
                const status = await this.client.session.status({
                    query,
                    signal,
                });
                if (signal.aborted || this.disposed) return;
                if (!status.data) {
                    this.schedule(sessionID);
                    return;
                }
                if (
                    (status.data[sessionID]?.type ?? "idle") !== "idle" ||
                    this.busy.has(sessionID) ||
                    this.tools.get(sessionID)?.size
                )
                    return;
            } catch {
                if (!signal.aborted) this.schedule(sessionID);
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
                                synthetic: true,
                            },
                        ],
                    },
                });
                if (signal.aborted || this.disposed) return;
                if (response.response.status === 204) entry.state = "delivered";
                else if (
                    [400, 401, 403, 404, 429].includes(response.response.status)
                )
                    entry.state = "pending";
                else entry.state = "delivery_uncertain";
            } catch {
                entry.state = "delivery_uncertain";
            }
            if (
                entry.state !== "delivered" &&
                entry.attempts < 3 &&
                !signal.aborted
            )
                this.schedule(sessionID);
            // One accepted prompt starts an agent turn; wait for the next idle event.
            return;
        }
    }

    private schedule(sessionID: string) {
        if (this.disposed || this.timers.has(sessionID)) return;
        const timer = setTimeout(() => {
            this.timers.delete(sessionID);
            void this.flush(sessionID);
        }, 1000);
        timer.unref();
        this.timers.set(sessionID, timer);
    }

    deleteSession(sessionID: string): void {
        this.sessions.get(sessionID)?.abort();
        this.sessions.delete(sessionID);
        clearTimeout(this.timers.get(sessionID));
        this.timers.delete(sessionID);
        this.busy.delete(sessionID);
        this.tools.delete(sessionID);
        for (const [id, entry] of this.entries)
            if (entry.result.sessionID === sessionID) this.entries.delete(id);
        const owned = this.resources.get(sessionID);
        this.resources.delete(sessionID);
        for (const release of owned ?? []) {
            try {
                release();
            } catch {
                /* One failing owner must not prevent remaining cancellation. */
            }
        }
    }

    dispose(): void {
        if (this.disposed) return;
        this.disposed = true;
        for (const id of new Set([
            ...this.sessions.keys(),
            ...this.resources.keys(),
            ...this.tools.keys(),
            ...this.busy,
        ]))
            this.deleteSession(id);
    }

    readonly hooks: Hooks = {
        event: async ({ event }) => {
            if (this.disposed) return;
            if (event.type === "session.deleted")
                this.deleteSession(event.properties.info.id);
            if (event.type === "session.status") {
                const { sessionID, status } = event.properties;
                if (status.type === "idle") {
                    this.busy.delete(sessionID);
                    // Failed/aborted tools may never emit tool.execute.after.
                    this.tools.delete(sessionID);
                    void (
                        this.flights.get(sessionID) ?? Promise.resolve()
                    ).then(() => this.flush(sessionID));
                } else this.busy.add(sessionID);
            }
        },
        "tool.execute.before": async ({ sessionID, callID }) => {
            if (this.disposed) return;
            const calls = this.tools.get(sessionID) ?? new Set<string>();
            calls.add(callID);
            this.tools.set(sessionID, calls);
        },
        "tool.execute.after": async ({ sessionID, callID }) => {
            this.tools.get(sessionID)?.delete(callID);
            if (!this.tools.get(sessionID)?.size) this.tools.delete(sessionID);
        },
        dispose: async () => {
            this.dispose();
        },
    };
}
