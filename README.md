# Polling Monitor Pattern

A serverless durable polling workflow on **AWS Lambda**. Two versions are available — one using **Step Functions** (v1) and one using the **AWS Lambda Durable Execution SDK** (v2).

Both versions solve the same problem: poll a long-running process on a configurable interval, sleep between checks without consuming compute, and stop automatically when the target status is reached or max attempts are exhausted.

---

## Table of Contents

- [Polling Monitor Pattern](#polling-monitor-pattern)
  - [Table of Contents](#table-of-contents)
  - [The Problem](#the-problem)
  - [Why Not Just Use `time.sleep()` in Lambda?](#why-not-just-use-timesleep-in-lambda)
  - [Version Comparison](#version-comparison)
  - [v1 — Step Functions + Lambda](#v1--step-functions--lambda)
    - [v1 Architecture](#v1-architecture)
    - [v1 How It Works](#v1-how-it-works)
    - [v1 Project Structure](#v1-project-structure)
    - [Deploying v1](#deploying-v1)
    - [Triggering v1](#triggering-v1)
    - [Implementing Your Own Status Check (v1)](#implementing-your-own-status-check-v1)
  - [v2 — Lambda Durable Execution SDK](#v2--lambda-durable-execution-sdk)
    - [What Is Durable Execution?](#what-is-durable-execution)
    - [Key Concepts](#key-concepts)
      - [Checkpoint and Replay](#checkpoint-and-replay)
      - [Determinism](#determinism)
      - [`@durable_step`](#durable_step)
      - [`context.wait()`](#contextwait)
      - [SDK Logger](#sdk-logger)
      - [The Loop Pattern](#the-loop-pattern)
    - [v2 Architecture](#v2-architecture)
    - [v2 How It Works](#v2-how-it-works)
    - [v2 Project Structure](#v2-project-structure)
    - [Deploying v2](#deploying-v2)
    - [Triggering v2](#triggering-v2)
    - [Implementing Your Own Status Check (v2)](#implementing-your-own-status-check-v2)
  - [Execution Input](#execution-input)
  - [Table of Contents](#table-of-contents-1)
  - [The Problem](#the-problem-1)
  - [Why Not Just Use `time.sleep()` in Lambda?](#why-not-just-use-timesleep-in-lambda-1)
  - [The Solution: Durable Orchestration](#the-solution-durable-orchestration)
  - [Architecture](#architecture)
  - [How It Works](#how-it-works)
  - [Project Structure](#project-structure)
  - [Execution Input](#execution-input-1)
  - [Local Testing](#local-testing)
  - [Deploying to AWS](#deploying-to-aws)
    - [One-time pipeline setup](#one-time-pipeline-setup)
    - [Prerequisites](#prerequisites)
  - [Starting an Execution](#starting-an-execution)
  - [Implementing Your Own Status Check](#implementing-your-own-status-check)
  - [Key Concepts](#key-concepts-1)
    - [Amazon States Language (ASL)](#amazon-states-language-asl)
    - [Wait State (the sleep)](#wait-state-the-sleep)
    - [Choice State (branching)](#choice-state-branching)
    - [ResultSelector](#resultselector)
    - [Retry and Catch](#retry-and-catch)
    - [AWS SAM](#aws-sam)

---

## The Problem

You have a long-running process (e.g. a data pipeline, a third-party API call, an ECS task) and you want to:

- Check its status every N minutes.
- Do nothing between checks — no compute running, no cost.
- Automatically stop when the process reaches a target status (e.g. `COMPLETED`).
- Fail safely if the process never completes (max attempts guard).

---

## Why Not Just Use `time.sleep()` in Lambda?

```python
while True:
    status = get_process_status(process_id)
    if status == "COMPLETED":
        break
    time.sleep(120)  # 2 minutes
```

**This is an anti-pattern:**

| Problem | Explanation |
|---|---|
| Lambda has a 15-minute max timeout | A process taking longer will be killed mid-poll |
| You pay for idle time | Lambda bills for every millisecond, even while sleeping |
| No durability | If Lambda crashes, all state is lost |
| Not observable | You can't inspect the current state from outside |

---

## Version Comparison

| | v1 — Step Functions | v2 — Durable Execution SDK |
|---|---|---|
| Orchestration | AWS Step Functions state machine | Lambda Durable Execution Service |
| Sleep | Step Functions `Wait` state | `context.wait(Duration.from_seconds(...))` |
| Checkpointing | Step Functions execution history | SDK checkpoint API (automatic) |
| Lambda functions | 2 (`check_status`, `on_complete`) | 1 (`polling_monitor`) + 1 invoker |
| Observability | Step Functions console | Durable Executions console |
| Max execution time | 1 year (Step Functions limit) | Up to 1 year (configured via `ExecutionTimeout`) |
| Runtime | python3.12 | python3.13+ (required for `DurableConfig`) |

---

## v1 — Step Functions + Lambda

### v1 Architecture

```
+-------------------------------------------------------------+
|                    Step Functions                           |
|                                                             |
|  +-------------+     +------------+     +----------------+ |
|  | CheckStatus |---->| IsComplete?|---->| WaitForNextPoll| |
|  |  (Lambda)   |     |  (Choice)  | No  |  (Wait state)  | |
|  +-------------+     +------------+     +--------+-------+ |
|         ^                  | Yes                 |         |
|         +------------------+---------------------+         |
|                            v                               |
|                    +--------------+                        |
|                    |  OnComplete  |                        |
|                    |  (Lambda)    |                        |
|                    +------+-------+                        |
|                           |                               |
|                    +------v-------+                        |
|                    |     END      |                        |
|                    +--------------+                        |
+-------------------------------------------------------------+
```

### v1 How It Works

1. **Start**: Trigger the state machine with an input payload containing `process_id` and polling config.
2. **CheckStatus** (Lambda): Runs on each poll tick. Calls `get_process_status()`, increments the attempt counter, and returns the current status.
3. **IsComplete** (Choice state): Inspects the Lambda result.
   - `is_complete == true` → go to **OnComplete**
   - `attempts >= max_attempts` → go to **MaxAttemptsExceeded** (Fail)
   - Otherwise → go to **WaitForNextPoll**
4. **WaitForNextPoll** (Wait state): Step Functions pauses for `poll_interval_seconds`. **No Lambda runs during this time.**
5. **OnComplete** (Lambda): Runs once when the target status is met.

### v1 Project Structure

```
v1/
├── statemachine/
│   └── polling.asl.json          # Step Functions workflow (Amazon States Language)
├── src/
│   ├── check_status/
│   │   └── handler.py            # Lambda: runs on every poll tick
│   └── on_complete/
│       └── handler.py            # Lambda: runs once when target status is met
├── template.yaml                 # SAM template
├── buildspec.yml                 # CodeBuild spec
├── codepipeline.yaml             # CloudFormation CI/CD pipeline
└── start_execution.py            # Helper script to start executions
```

### Deploying v1

**One-time pipeline setup:**
```bash
aws cloudformation deploy \
  --template-file v1/codepipeline.yaml \
  --stack-name polling-monitor-pattern-v1-ci-pipeline \
  --capabilities CAPABILITY_IAM CAPABILITY_NAMED_IAM \
  --region ap-southeast-1
```

After that, every push to the configured branch triggers: **CodeBuild** (`sam package`) → **CloudFormation** (deploys `template.yaml`).

### Triggering v1

**AWS Console:**
1. Go to **Step Functions → State machines → `polling-monitor-pattern-polling`**
2. Click **Start execution** and paste the JSON input

**AWS CLI:**
```bash
aws stepfunctions start-execution \
  --state-machine-arn arn:aws:states:ap-southeast-1:<account-id>:stateMachine:polling-monitor-pattern-polling \
  --input '{
    "process_id": "my-process-123",
    "target_status": "COMPLETED",
    "poll_interval_seconds": 120,
    "max_attempts": 30
  }' \
  --region ap-southeast-1
```

**Script:**
```bash
cd v1 && python start_execution.py my-process-123 --interval 120 --max-attempts 30
```

### Implementing Your Own Status Check (v1)

Open `v1/src/check_status/handler.py` and replace `get_process_status()`:

```python
def get_process_status(process_id: str) -> str:
    # DynamoDB example
    table = boto3.resource("dynamodb").Table("MyTable")
    return table.get_item(Key={"process_id": process_id})["Item"]["status"]

    # AWS Glue example
    run = boto3.client("glue").get_job_run(JobName="my-job", RunId=process_id)
    return run["JobRun"]["JobRunState"]

    # ECS Task example
    tasks = boto3.client("ecs").describe_tasks(cluster="my-cluster", tasks=[process_id])
    return tasks["tasks"][0]["lastStatus"]
```

Add post-completion logic in `v1/src/on_complete/handler.py`.

---

## v2 — Lambda Durable Execution SDK

### What Is Durable Execution?

AWS Lambda Durable Execution lets a single Lambda function run for up to **1 year** across many short invocations, without you managing any orchestration infrastructure. The SDK handles checkpointing, sleeping, and replay automatically.

The key insight: there are two separate lifetimes:

- **Lambda invocation** — starts, runs, terminates. Max 15 minutes per invocation.
- **Durable execution** — a logical entity that outlives any single invocation. The durable functions service holds the checkpoint log and scheduler. When `context.wait()` fires, the service wakes Lambda after the duration and replays from the last checkpoint.

```
Durable execution "abc123" [ACTIVE — managed by AWS durable service]
  │
  ├── Lambda invocation #1  [runs, saves checkpoint, terminates at wait]
  │
  │     ... N seconds pass, no Lambda running, no cost ...
  │
  ├── Lambda invocation #2  [cold start, replays past checkpoints, runs new work]
  │
  │     ... N more seconds ...
  │
  └── Lambda invocation #N  [completes → durable execution ends]
```

### Key Concepts

#### Checkpoint and Replay

When `context.step()` executes a function, the SDK:
1. Runs your function
2. Serialises the result
3. Persists it to the checkpoint log (via the Lambda checkpoint API)
4. Returns the result

On the next invocation (after a `wait` or a crash recovery), the SDK replays the function from the top. For every `context.step()` call it finds an existing checkpoint for, it **returns the stored result immediately without calling your code again**. Execution resumes naturally at the first un-checkpointed operation.

#### Determinism

Because your handler runs again on every replay, it **must be deterministic** — given the same inputs and checkpoint log, it must take the same code path every time.

Avoid outside `context.step()`:
- `datetime.now()` / timestamps
- `random.uuid()` / random numbers
- External API calls or database queries
- File system operations

Wrap anything non-deterministic inside a `@durable_step`.

#### `@durable_step`

Decorates a function to make it a named, checkpointable unit of work. The first argument is always `ctx: StepContext`, which provides `ctx.logger` enriched with the step's execution context.

```python
@durable_step
def check_status(ctx: StepContext, process_id: str) -> str:
    ctx.logger.info("Checking %s", process_id)  # suppressed during replay
    return call_my_api(process_id)
```

Call it via `context.step(check_status(process_id))`. Pass `name=` to give each invocation a unique checkpoint key (required in loops).

#### `context.wait()`

Suspends the durable execution for a duration — the Lambda invocation terminates, and the service re-invokes Lambda after the duration elapses. **No compute runs and no cost is incurred** during the wait.

```python
context.wait(Duration.from_seconds(120))
context.wait(Duration.from_minutes(5))
context.wait(Duration.from_hours(1))
```

#### SDK Logger

Use `context.logger` (in `lambda_handler`) and `ctx.logger` (in `@durable_step` functions) instead of the standard Python logger. The SDK logger:
- **Suppresses duplicate logs during replay** — each log message appears only once even though the handler runs multiple times
- **Enriches log records** with `executionArn`, `operationName`, `operationId` — visible in the Durable Functions console log view

#### The Loop Pattern

The polling loop drives iteration — the SDK doesn't retry automatically. Each loop iteration uses a unique `name=` to create a distinct checkpoint:

```
Checkpoint log after attempt 1:
  "check-status-1"  →  "IN_PROGRESS"

On resume (invocation #2):
  attempt=1: "check-status-1" found → returns "IN_PROGRESS" instantly
  attempt=2: "check-status-2" not found → actually executes your code
```

### v2 Architecture

```
+--------------------------------------------------+
|              Invoke (async)                      |
|  InvokerFunction  ──────────>  PollingMonitor    |
|  (standard Lambda)            Function:live      |
|                               (durable Lambda)   |
+--------------------------------------------------+
         │                           │
         │                    ┌──────▼──────┐
         │                    │  Checkpoint │
         │                    │    Store    │
         │                    │  (managed   │
         │                    │   by AWS)   │
         │                    └──────┬──────┘
         │                           │
         │          resume after wait│
         │                    ┌──────▼──────────────────────┐
         │                    │  Replay → check-status-N    │
         │                    │  wait → Replay → ...        │
         │                    │  → on-complete → END        │
         │                    └─────────────────────────────┘
         │
         └──> Returns immediately (InvocationType=Event)
```

### v2 How It Works

1. **Invoke**: Call `InvokerFunction` with the polling config. It immediately returns — the durable execution runs asynchronously.
2. **Poll cycle**: `PollingMonitorFunction` runs `check_process_status()` as a checkpointed step, named `check-status-{attempt}`.
3. **Wait**: If not yet complete, `context.wait(Duration.from_seconds(poll_interval_seconds))` suspends the execution. Lambda terminates.
4. **Resume**: After the wait, Lambda is re-invoked. The SDK replays from the top, skipping all previous checkpoints, and runs the next poll cycle.
5. **Complete**: When `status == target_status`, `on_complete()` runs as a final checkpoint and the handler returns.
6. **Exhausted**: If `max_attempts` is reached without hitting the target status, an exception is raised and the durable execution ends in failure.

### v2 Project Structure

```
v2/
├── src/
│   ├── polling_monitor/
│   │   ├── handler.py            # Durable Lambda: polling loop with checkpoints
│   │   └── requirements.txt      # aws-durable-execution-sdk-python
│   └── invoker/
│       └── handler.py            # Standard Lambda: async trigger for the durable function
├── sample-events/
│   ├── v2-event.json             # Direct invocation event for polling_monitor
│   └── invoker-event.json        # Event for the invoker function
├── template.yaml                 # SAM template (DurableConfig, AutoPublishAlias)
├── buildspec.yml                 # CodeBuild spec (python3.13, sam build + sam package)
└── codepipeline.yaml             # CloudFormation CI/CD pipeline
```

### Deploying v2

> **Prerequisites:**
> - Python 3.13+ (required — `DurableConfig` is not supported on older runtimes)
> - AWS SAM CLI
> - A GitHub CodeStar Connection ARN

**One-time pipeline setup:**
```bash
aws cloudformation deploy \
  --template-file v2/codepipeline.yaml \
  --stack-name polling-monitor-pattern-v2-ci-pipeline \
  --capabilities CAPABILITY_IAM CAPABILITY_NAMED_IAM \
  --region ap-southeast-1
```

Every push triggers: **CodeBuild** (`sam build` + `sam package`) → **CloudFormation** (deploys `template.yaml`).

> **Note:** `DurableConfig` cannot be added to an existing Lambda function. If you are updating an existing stack that didn't previously have `DurableConfig`, delete the app stack first:
> ```bash
> aws cloudformation delete-stack --stack-name polling-monitor-pattern-v2 --region ap-southeast-1
> aws cloudformation wait stack-delete-complete --stack-name polling-monitor-pattern-v2 --region ap-southeast-1
> ```

### Triggering v2

Durable functions must be invoked via a **qualified ARN** (alias or version). The `InvokerFunction` handles this for you.

**Via InvokerFunction (recommended):**

```bash
aws lambda invoke \
  --function-name polling-monitor-pattern-v2-invoker \
  --payload '{
    "process_id": "my-process-123",
    "target_status": "COMPLETED",
    "poll_interval_seconds": 120,
    "max_attempts": 30
  }' \
  --region ap-southeast-1 \
  output.json && cat output.json
```

**Direct invocation (async, using the alias ARN):**

```bash
aws lambda invoke \
  --function-name arn:aws:lambda:ap-southeast-1:<account-id>:function:polling-monitor-pattern-v2-polling-monitor:live \
  --invocation-type Event \
  --payload '{
    "process_id": "my-process-123",
    "target_status": "COMPLETED",
    "poll_interval_seconds": 120,
    "max_attempts": 30
  }' \
  --region ap-southeast-1 \
  /dev/null
```

> Use `InvocationType=Event` (async). Synchronous invocations will time out for long-running executions.

**Sample event files:**

`v2/sample-events/invoker-event.json`:
```json
{
  "process_id": "test-process-1",
  "target_status": "COMPLETED",
  "poll_interval_seconds": 10,
  "max_attempts": 3
}
```

### Implementing Your Own Status Check (v2)

Open `v2/src/polling_monitor/handler.py` and replace the body of `check_process_status`:

```python
@durable_step
def check_process_status(ctx: StepContext, process_id: str, attempt: int, max_attempts: int) -> str:
    ctx.logger.info("Checking process status | process_id=%s attempt=%d/%d", process_id, attempt, max_attempts)

    # DynamoDB example
    table = boto3.resource("dynamodb").Table("MyTable")
    return table.get_item(Key={"process_id": process_id})["Item"]["status"]

    # AWS Glue example
    run = boto3.client("glue").get_job_run(JobName="my-job", RunId=process_id)
    return run["JobRun"]["JobRunState"]

    # ECS Task example
    tasks = boto3.client("ecs").describe_tasks(cluster="my-cluster", tasks=[process_id])
    return tasks["tasks"][0]["lastStatus"]
```

Add post-completion logic in `on_complete`. Add any required IAM permissions to `PollingMonitorFunction` in `v2/template.yaml`.

---

## Execution Input

Both versions use the same input shape:

| Field | Required | Description |
|---|---|---|
| `process_id` | Yes | Identifier of the process to poll |
| `target_status` | Yes | The status string that signals completion (e.g. `COMPLETED`) |
| `poll_interval_seconds` | Yes | Seconds to wait between each poll |
| `max_attempts` | Yes | Maximum number of polls before the execution fails |

```json
{
  "process_id": "my-process-123",
  "target_status": "COMPLETED",
  "poll_interval_seconds": 120,
  "max_attempts": 30
}
```


---

## Table of Contents

- [The Problem](#the-problem)
- [Why Not Just Use `time.sleep()` in Lambda?](#why-not-just-use-timesleep-in-lambda)
- [The Solution: Durable Orchestration](#the-solution-durable-orchestration)
- [Architecture](#architecture)
- [How It Works](#how-it-works)
- [Project Structure](#project-structure)
- [Execution Input](#execution-input)
- [Local Testing](#local-testing)
- [Deploying to AWS](#deploying-to-aws)
- [Starting an Execution](#starting-an-execution)
- [Implementing Your Own Status Check](#implementing-your-own-status-check)
- [Key Concepts](#key-concepts)

---

## The Problem

You have a long-running process (e.g. a data pipeline, a third-party API call, an ECS task) and you want to:

- Check its status every N minutes.
- Do nothing (not consume compute) between checks.
- Automatically stop once the process reaches a target status (e.g. `COMPLETED`).
- Fail safely if the process never completes (max attempts guard).

---

## Why Not Just Use `time.sleep()` in Lambda?

You might think of doing this:

```python
while True:
    status = get_process_status(process_id)
    if status == "COMPLETED":
        break
    time.sleep(120)  # sleep 2 minutes
```

**This is an anti-pattern for several reasons:**

| Problem | Explanation |
|---|---|
| Lambda has a 15-minute max timeout | A process taking longer than 15 minutes will be killed mid-poll |
| You pay for idle time | Lambda bills for every millisecond it is running, even while sleeping |
| No durability | If Lambda crashes or times out, all state is lost |
| Not observable | You cannot inspect the current state of the loop from outside |

---

## The Solution: Durable Orchestration

Instead, we delegate orchestration to **AWS Step Functions**, which is designed exactly for this:

- Step Functions manages the flow between states **durably** - state is persisted on AWS infrastructure, not in memory.
- The `Wait` state tells Step Functions to pause execution for N seconds **without running any compute**. Lambda is not invoked and you pay nothing during the wait.
- Each poll is a fresh, short-lived Lambda invocation.
- The full execution history is visible in the AWS console.

---

## Architecture

```
+-------------------------------------------------------------+
|                    Step Functions                           |
|                                                             |
|  +-------------+     +------------+     +----------------+ |
|  | CheckStatus |---->| IsComplete?|---->| WaitForNextPoll| |
|  |  (Lambda)   |     |  (Choice)  | No  |  (Wait state)  | |
|  +-------------+     +------------+     +--------+-------+ |
|         ^                  | Yes                 |         |
|         +------------------+---------------------+         |
|                            v                               |
|                    +--------------+                        |
|                    |  OnComplete  |                        |
|                    |  (Lambda)    |                        |
|                    +------+-------+                        |
|                           |                               |
|                    +------v-------+                        |
|                    |     END      |                        |
|                    +--------------+                        |
+-------------------------------------------------------------+
```

---

## How It Works

1. **Start**: Trigger the state machine with an execution input containing your `process_id` and polling config.

2. **CheckStatus** (Lambda): Runs on each poll tick. Calls `get_process_status()`, increments the attempt counter, and returns whether the status matches the target.

3. **IsComplete** (Choice state): Inspects the Lambda result.
   - If `is_complete == true` → go to **OnComplete**.
   - If `attempts >= max_attempts` → go to **MaxAttemptsExceeded** (Fail).
   - Otherwise → go to **WaitForNextPoll**.

4. **WaitForNextPoll** (Wait state): Step Functions pauses execution for `poll_interval_seconds`. **No Lambda runs during this time.**

5. **OnComplete** (Lambda): Runs once when the target status is met. Use this to send notifications, trigger downstream processes, update databases, etc.

6. **MaxAttemptsExceeded / PollFailed**: Terminal failure states for observability.

---

## Project Structure

```
polling-monitor-pattern/
├── statemachine/
│   └── polling.asl.json          # Step Functions workflow definition (Amazon States Language)
├── src/
│   ├── check_status/
│   │   └── handler.py            # Lambda: runs on every poll tick
│   └── on_complete/
│       └── handler.py            # Lambda: runs once when the target status is met
├── template.yaml                 # AWS SAM template (app infrastructure)
├── buildspec.yml                 # CodeBuild spec (sam package → packaged.yaml)
├── codepipeline.yaml             # CloudFormation template for the CI/CD pipeline
├── start_execution.py            # Script to kick off a new polling execution
├── event.check_status.json       # Sample event for local Lambda testing
└── event.on_complete.json        # Sample event for local Lambda testing
```

---

## Execution Input

All polling parameters are passed at execution time via the Step Functions input - no code or infrastructure changes needed.

| Field | Required | Description |
|---|---|---|
| `process_id` | Yes | Identifier of the process to poll |
| `target_status` | Yes | The status string that signals completion (e.g. `COMPLETED`) |
| `poll_interval_seconds` | Yes | Seconds to wait between each poll |
| `max_attempts` | Yes | Maximum number of polls before the workflow fails |

Example:
```json
{
  "process_id": "my-process-123",
  "target_status": "COMPLETED",
  "poll_interval_seconds": 120,
  "max_attempts": 30
}
```

---

## Local Testing

You can test individual Lambda functions locally using SAM + Docker.

> **Note:** Full Step Functions orchestration (Wait state, state transitions) cannot be run locally. Only individual Lambda functions can be invoked via SAM.

**1. Build the project**
```bash
sam build
```

**2. Test the CheckStatus function**
```bash
sam local invoke CheckStatusFunction --event event.check_status.json
```

**3. Test the OnComplete function**
```bash
sam local invoke OnCompleteFunction --event event.on_complete.json
```

`event.check_status.json` - simulates the first poll tick:
```json
{
  "process_id": "test-process-1",
  "target_status": "COMPLETED",
  "poll_interval_seconds": 2,
  "max_attempts": 3,
  "attempts": 0
}
```

`event.on_complete.json` - simulates successful completion:
```json
{
  "process_id": "test-process-1",
  "status": "COMPLETED",
  "is_complete": true,
  "attempts": 2,
  "poll_interval_seconds": 2,
  "max_attempts": 3,
  "target_status": "COMPLETED"
}
```

---

## Deploying to AWS

This project uses a **CodePipeline CI/CD pipeline**. Pushing to the `main` branch automatically builds and deploys the app.

### One-time pipeline setup

Deploy `codepipeline.yaml` once to create the pipeline infrastructure:

```bash
aws cloudformation deploy \
  --template-file codepipeline.yaml \
  --stack-name polling-monitor-pattern-pipeline \
  --capabilities CAPABILITY_IAM CAPABILITY_NAMED_IAM \
  --region ap-southeast-1
```

After that, every push to `main` triggers: **CodeBuild** (`sam package`) → **CloudFormation** (deploys `template.yaml`).

### Prerequisites

- [AWS SAM CLI](https://docs.aws.amazon.com/serverless-application-model/latest/developerguide/install-sam-cli.html) (for local testing)
- [Docker Desktop](https://www.docker.com/products/docker-desktop/) (required for `sam local invoke`)
- AWS CLI configured (`aws configure`)
- A GitHub CodeStar Connection ARN (set as `GitHubConnectionArn` in `codepipeline.yaml`)

---

## Starting an Execution

**AWS Console:**
1. Go to **Step Functions → State machines → `polling-monitor-pattern-polling`**
2. Click **Start execution** and paste the execution input JSON

**AWS CLI:**
```bash
aws stepfunctions start-execution \
  --state-machine-arn arn:aws:states:ap-southeast-1:<account-id>:stateMachine:polling-monitor-pattern-polling \
  --input '{"process_id":"my-process-123","target_status":"COMPLETED","poll_interval_seconds":120,"max_attempts":30}' \
  --region ap-southeast-1
```

**Script:**
```bash
python start_execution.py my-process-123 --interval 120 --max-attempts 30
```

---

## Implementing Your Own Status Check

Open `src/check_status/handler.py` and replace `get_process_status()` with your real logic:

```python
def get_process_status(process_id: str, attempts: int, max_attempts: int) -> str:
    # Example: query DynamoDB
    table = boto3.resource("dynamodb").Table("MyTable")
    return table.get_item(Key={"process_id": process_id})["Item"]["status"]
```

Other common examples:

```python
# AWS Glue
glue = boto3.client("glue")
run = glue.get_job_run(JobName="my-glue-job", RunId=process_id)
return run["JobRun"]["JobRunState"]

# ECS Task
ecs = boto3.client("ecs")
tasks = ecs.describe_tasks(cluster="my-cluster", tasks=[process_id])
return tasks["tasks"][0]["lastStatus"]

# External REST API
import urllib.request, json
with urllib.request.urlopen(f"https://api.example.com/jobs/{process_id}") as r:
    return json.loads(r.read())["status"]
```

Then add your post-completion logic in `src/on_complete/handler.py`.

---

## Key Concepts

### Amazon States Language (ASL)
The Step Functions workflow is defined in `statemachine/polling.asl.json`. Each state has a `Type` (Task, Choice, Wait, Fail) and transitions to the next state.

### Wait State (the sleep)
`WaitForNextPoll` uses `"SecondsPath": "$.poll_interval_seconds"` to read the interval from the execution input. While waiting, **no compute runs and no cost is incurred**.

### Choice State (branching)
`IsComplete` branches on `is_complete` and `attempts` - routing to `OnComplete`, `MaxAttemptsExceeded`, or back to `WaitForNextPoll`.

### ResultSelector
After Lambda returns, `ResultSelector` extracts fields from `$.Payload.*`, keeping only what the next state needs.

### Retry and Catch
Every Task state retries on transient Lambda errors (throttling, service exceptions) with exponential backoff, and catches unrecoverable errors to route them to `PollFailed`.

### AWS SAM
`template.yaml` is a SAM template. `sam package` (run by CodeBuild) zips the Lambda code, uploads it to S3, and produces a plain CloudFormation template (`packaged.yaml`) for the Deploy stage.
