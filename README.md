# OpenCode GitLab background operations

An OpenCode plugin for compact GitLab operations and background pipeline/job monitoring. Push a commit or start a watch, continue working, and receive one result when the run finishes. No agent-written `sleep` loops or intermediate polling messages.

## Install locally

Requires OpenCode **1.18.30**, Bun **1.3.10**, Git, and an authenticated `glab` CLI. Tested with `glab` **1.115.0** on macOS. The subprocess runner uses POSIX process groups; Windows is not supported.

1. Install dependencies and build:

    ```sh
    bun install --frozen-lockfile
    bun run build
    ```

2. Authenticate `glab` for the GitLab hosts you use:

    ```sh
    glab auth login --hostname gitlab.com
    ```

3. Add the built plugin's absolute file URL to your OpenCode configuration:

    ```json
    {
        "$schema": "https://opencode.ai/config.json",
        "plugin": [
            "file:///absolute/path/to/opencode-gitlab-pipeline/dist/index.js"
        ]
    }
    ```

4. Quit and restart OpenCode. Ask it to watch a GitLab pipeline or job URL.

This package is currently a local build, not an npm release. `bun.lock` pins the dependency graph. The plugin reuses `glab` authentication and host settings; it does not manage tokens.

## Tools

| Tool             | Purpose                                                                                     |
| ---------------- | ------------------------------------------------------------------------------------------- |
| `gitlab`         | Execute noninteractive `glab` arguments once, including MR/issue operations and `glab api`. |
| `gitlab_push`    | Push the captured current commit and discover its pipeline in the background.               |
| `gitlab_inspect` | Read the status of one exact pipeline or job.                                               |
| `gitlab_watch`   | Start, list, inspect, or stop session-local background watches.                             |
| `gitlab_output`  | Read bounded cached command output or fetch a job trace on demand.                          |

### Push and await CI

Prepare and commit changes with OpenCode's existing Git tools. Then call `gitlab_push`:

```json
{ "remote": "origin", "branch": "my-feature" }
```

The remote defaults to `origin`; the branch defaults to the current branch. Detached HEAD requires an explicit destination branch. The plugin captures HEAD and pushes that exact SHA to the branch using the push remote URL. It does not force-push, stage files, or create commits. Use `gitlab` to create or update the MR.

The acknowledgment distinguishes **push success** from **CI success** and returns a watch ID. Discovery checks SHA and ref, allowing two minutes for pipeline creation. Optional `mergeRequest: { host, repo, iid }` supplies MR lookup context, including fork projects. Multiple matching pipelines return candidates for explicit selection. A merged-result pipeline with a synthetic SHA is not silently treated as proof for the original commit; watch its exact URL separately.

### Watch a referenced pipeline or job

```json
{
    "action": "start",
    "url": "https://gitlab.example.com/group/project/-/pipelines/123"
}
```

Alternatively supply `kind`, `id`, `host`, and `repo`. IDs are strings. With only `kind` and `id`, the plugin resolves the current repository's `origin`. Nested namespaces and self-managed hosts are supported. `cwd` is relative to the current session directory.

The default total deadline is 30 minutes; `timeoutMs` accepts 1–1800000 milliseconds. The agent does not choose polling intervals. After acknowledgment it can continue other work; completion automatically resumes the originating session when idle.

```json
{ "action": "get", "watchID": "the-returned-watch-id" }
```

```json
{ "action": "stop", "watchID": "the-returned-watch-id" }
```

`list` returns up to eight watch references with `nextOffset` for pagination. Stopping a watch ends local monitoring, not the GitLab pipeline. Retry or cancel GitLab work with an explicit `gitlab` operation. A retried job or newer branch pipeline does not replace a pinned watch.

### General GitLab operations

Pass argument arrays, not shell syntax:

```json
{
    "args": ["mr", "list", "--output", "json"],
    "host": "gitlab.example.com",
    "repo": "group/project"
}
```

`host`/`repo` provide CLI defaults; explicit CLI flags can override them. `stdin` supports long descriptions and API bodies, for example `glab api ... --input -` with the appropriate content type. Commands must supply noninteractive arguments. Editors and browsers are disabled; output is captured without a terminal.

Each invocation runs once. An uncertain mutation result is never automatically retried. The general command deadline is 30 seconds, configurable up to 30 minutes. Use `gitlab_watch` for background CI monitoring rather than `ci status --wait` through the general tool.

### Failure details and logs

Completion includes the exact target URL, SHA, raw GitLab status, and outcome. Pipeline failures include up to five failed job names/links and a count. The result stays a failure even if failed-job metadata cannot be fetched.

```json
{
    "jobURL": "https://gitlab.example.com/group/project/-/jobs/456",
    "offset": 0,
    "limit": 2048
}
```

`gitlab_output` fetches a bounded trace preview and returns a cached result ID. Subsequent reads use `resultID`, `stream` (`stdout` or `stderr`), raw-byte `offset`, and `limit` (up to 8192). Output identifies retained and discarded bytes. Discarded overflow is not available from the cache; use GitLab's job page for the full trace. Binary output and slices that split a UTF-8 character use base64 to preserve the bytes. Terminal control codes are stripped from text.

## Waiting and lifecycle

- Pipelines use exact-ID checks through `glab api`. `glab ci status --wait` in 1.115.0 re-resolves the branch and can switch pipelines, so it is unsuitable for this contract.
- Active jobs use native `glab ci trace` initially, then an authoritative status lookup. A 30-second watchdog covers native trace hangs on skipped/manual jobs; status checking takes over when tracing exits or times out. Trace exit code zero does not mean job success.
- Plugin-owned status checks normally run every three seconds. Transient read failures back off, bounded by the deadline. Authentication failures stop monitoring. No shell `sleep` process is used.
- Success, failure, canceled, skipped, manual action required, ambiguity, missing pipeline, timeout, unsupported state, and monitoring error are distinct. Only GitLab's explicit `success` confirms success. A watch reports that pipeline's status, not recursively aggregated downstream deployment status.
- Watches belong to the running plugin/server instance. Session deletion or instance disposal releases timers and subprocesses. Closing a client attached to a shared server may leave monitoring active until that server instance ends. Restart recovery is not provided.

Completion prompts use a per-session queue and stable message IDs. Busy sessions wait for idle. Uncertain prompt acceptance is reconciled by message ID; it is never blindly reinserted. `gitlab_watch get` exposes delivery state, including `delivery_uncertain` or a notification error. Delivery acceptance means OpenCode received the prompt, not that the agent finished responding.

## Permissions and limits

Watch start requests `gitlab_watch` permission for the exact target or pushed SHA. Each underlying `git`/`glab` subprocess also checks OpenCode's `bash` permissions; working outside the worktree checks `external_directory`. The background watcher retains these checks after its initiating tool returns. If your policy asks for each command, monitoring may pause for approval; configure appropriate read-command allowances through your normal OpenCode permissions. The plugin does not alter those rules.

Limits per instance:

- 16 active watches; 128 retained watch records. Identical active starts in one session reuse a watch.
- 8 foreground operations; 1 MiB retained per subprocess result (768 KiB stdout, 256 KiB stderr).
- Output cache: 16 MiB and 128 records, isolated by session. Eviction and instance restart expire references.
- Acknowledgments/success summaries target 1 KiB; failure/inspection summaries at most 4 KiB; output pages at most 8 KiB.
- Delivery queue: 256 retained results with explicit rejection if pending work fills it. Watch results remain inspectable if notification cannot be queued.

## Development and verification

```sh
bun run test
bun run typecheck
bun run build
bun run format
bun run lint
OPENCODE_RUNTIME_SMOKE=1 bun test tests/plugin.test.ts
```

The opt-in runtime smoke starts a separate OpenCode instance with isolated configuration and a local synthetic model provider. It proves idle wake-up, busy-tool ordering, session deletion, and disposal. It also runs the built `gitlab_watch` tool through the installed `glab` against a loopback GitLab fixture: the pipeline changes from running to success after acknowledgment, then one completion wakes the agent. No paid model or real GitLab mutation is involved.

Unit tests use injected clocks and fixture responses for exact-target selection, deadlines, native trace semantics, read backoff, permission denial, cancellation, and output limits. An installed-CLI test runs native job tracing against a loopback server and verifies that a failed job is reported as failed even when tracing exits successfully. A local bare Git repository proves that push sends the captured SHA even if HEAD changes during permission approval.

**Live GitLab integration is not yet verified.** The current evidence covers the real OpenCode/CLI runtime against local fixtures, not a production GitLab pipeline.
