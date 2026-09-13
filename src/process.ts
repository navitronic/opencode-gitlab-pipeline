import { spawn } from "node:child_process";
import { realpath } from "node:fs/promises";
import { isAbsolute, join, relative, resolve } from "node:path";
import type { ToolContext } from "@opencode-ai/plugin";

export const PROCESS_LIMITS = {
    bytes: 1024 * 1024,
    timeoutMs: 30000,
    maxTimeoutMs: 30 * 60 * 1000,
};

export interface ProcessRequest {
    executable: string;
    args: readonly string[];
    cwd?: string;
    env?: Record<string, string>;
    stdin?: string | Uint8Array;
    signal?: AbortSignal;
    timeoutMs?: number;
    maxBytes?: number;
}

export interface ProcessResult {
    status:
        | "success"
        | "exit_error"
        | "not_found"
        | "spawn_error"
        | "timeout"
        | "canceled";
    exitCode: number | null;
    signal: NodeJS.Signals | null;
    stdout: Buffer;
    stderr: Buffer;
    stdoutBytes: number;
    stderrBytes: number;
    durationMs: number;
}

export type ProcessRunner = (
    request: ProcessRequest,
    context: ToolContext,
) => Promise<ProcessResult>;

async function askPermission(
    context: ToolContext,
    input: Parameters<ToolContext["ask"]>[0],
    signal: AbortSignal,
): Promise<boolean> {
    if (signal.aborted) return false;
    return new Promise<boolean>((resolve, reject) => {
        const abort = () => resolve(false);
        signal.addEventListener("abort", abort, { once: true });
        void context
            .ask(input)
            .then(() => resolve(!signal.aborted), reject)
            .finally(() => signal.removeEventListener("abort", abort));
    });
}

/** Permissions are mandatory even for callers outside the general tool. Denials propagate. */
export const runProcess: ProcessRunner = async (request, context) => {
    request = {
        ...request,
        args: [...request.args],
        env: request.env && { ...request.env },
        stdin:
            typeof request.stdin === "object"
                ? Buffer.from(request.stdin)
                : request.stdin,
    };
    const started = Date.now();
    const signal = AbortSignal.any([
        context.abort,
        ...(request.signal ? [request.signal] : []),
    ]);
    const result: ProcessResult = {
        status: "canceled",
        exitCode: null,
        signal: null,
        stdout: Buffer.alloc(0),
        stderr: Buffer.alloc(0),
        stdoutBytes: 0,
        stderrBytes: 0,
        durationMs: 0,
    };
    if (signal.aborted) return result;
    const maxBytes = request.maxBytes ?? PROCESS_LIMITS.bytes;
    const timeoutMs = request.timeoutMs ?? PROCESS_LIMITS.timeoutMs;
    if (
        !Number.isInteger(maxBytes) ||
        maxBytes < 0 ||
        maxBytes > PROCESS_LIMITS.bytes
    )
        throw new Error("maxBytes must be between 0 and 1 MiB");
    if (
        !Number.isInteger(timeoutMs) ||
        timeoutMs < 1 ||
        timeoutMs > PROCESS_LIMITS.maxTimeoutMs
    )
        throw new Error("timeoutMs must be between 1 and 1800000");
    if (
        !request.executable ||
        [request.executable, ...request.args].some((arg) => arg.includes("\0"))
    )
        throw new Error("Executable and argv must not contain NUL");
    if (
        Buffer.byteLength(request.args.join("")) +
            Buffer.byteLength(request.stdin ?? "") >
        PROCESS_LIMITS.bytes
    )
        throw new Error("Arguments and stdin exceed 1 MiB");
    if (process.platform === "win32")
        throw new Error("Process-group cleanup requires POSIX");
    const cwd = await realpath(resolve(context.directory, request.cwd ?? "."));
    const worktree = await realpath(context.worktree);
    const outside = relative(worktree, cwd);
    if (outside === ".." || outside.startsWith("../") || isAbsolute(outside)) {
        if (
            !(await askPermission(
                context,
                {
                    permission: "external_directory",
                    patterns: [cwd, join(cwd, "*")],
                    always: [],
                    metadata: { cwd },
                },
                signal,
            ))
        )
            return result;
    }
    const argv = [request.executable, ...request.args];
    const command = argv
        .map((arg) =>
            /^[\w./:@%+=,-]+$/.test(arg)
                ? arg
                : "'" + arg.replaceAll("'", "'\\''") + "'",
        )
        .join(" ");
    // Keep the unquoted spelling too: quoting must not hide a deny such as `glab mr merge *`.
    if (
        !(await askPermission(
            context,
            {
                permission: "bash",
                patterns: [...new Set([argv.join(" "), command])],
                always: [],
                metadata: {
                    command,
                    executable: request.executable,
                    args: [...request.args],
                    cwd,
                    env: { ...request.env },
                    stdin:
                        typeof request.stdin === "string"
                            ? request.stdin
                            : undefined,
                    stdinBytes: Buffer.byteLength(request.stdin ?? ""),
                },
            },
            signal,
        ))
    )
        return result;
    if (signal.aborted)
        return {
            ...result,
            status: "canceled",
            durationMs: Date.now() - started,
        };

    result.status = "success";
    return new Promise<ProcessResult>((finish) => {
        // Reserve stderr space so a large response cannot hide the command's error.
        const stdout = Buffer.alloc(Math.floor((maxBytes * 3) / 4));
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
                "pipe",
            ],
        });
        const kill = () => {
            if (!child.pid) return;
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
        const capture = (stream: "stdout" | "stderr", chunk: Buffer) => {
            result[stream === "stdout" ? "stdoutBytes" : "stderrBytes"] +=
                chunk.length;
            const buffer = stream === "stdout" ? stdout : stderr;
            retained[stream] += chunk.copy(buffer, retained[stream]);
        };
        child.stdout?.on("data", (chunk: Buffer) => capture("stdout", chunk));
        child.stderr?.on("data", (chunk: Buffer) => capture("stderr", chunk));
        child.on("error", (error: NodeJS.ErrnoException) => {
            result.status =
                error.code === "ENOENT" ? "not_found" : "spawn_error";
        });
        // Noninteractive work must not leave descendants (or inherited pipes) behind.
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
        // A command may close stdin early. EPIPE must not crash the plugin.
        child.stdin?.on("error", () => {});
        child.stdin?.end(request.stdin);
        if (signal.aborted) cancel();
    });
};
