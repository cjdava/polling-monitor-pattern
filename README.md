# Polling Monitor Pattern

A serverless durable polling workflow built with **AWS Lambda** + **AWS Step Functions**.
Polls a process status on a configurable interval, sleeps between checks (without keeping Lambda running), and shuts down automatically when the target status is met or the maximum number of attempts is exhausted.

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

- Step Functions manages the flow between states **durably** — state is persisted on AWS infrastructure, not in memory.
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

All polling parameters are passed at execution time via the Step Functions input — no code or infrastructure changes needed.

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

`event.check_status.json` — simulates the first poll tick:
```json
{
  "process_id": "test-process-1",
  "target_status": "COMPLETED",
  "poll_interval_seconds": 2,
  "max_attempts": 3,
  "attempts": 0
}
```

`event.on_complete.json` — simulates successful completion:
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
`IsComplete` branches on `is_complete` and `attempts` — routing to `OnComplete`, `MaxAttemptsExceeded`, or back to `WaitForNextPoll`.

### ResultSelector
After Lambda returns, `ResultSelector` extracts fields from `$.Payload.*`, keeping only what the next state needs.

### Retry and Catch
Every Task state retries on transient Lambda errors (throttling, service exceptions) with exponential backoff, and catches unrecoverable errors to route them to `PollFailed`.

### AWS SAM
`template.yaml` is a SAM template. `sam package` (run by CodeBuild) zips the Lambda code, uploads it to S3, and produces a plain CloudFormation template (`packaged.yaml`) for the Deploy stage.
