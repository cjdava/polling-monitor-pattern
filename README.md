# Lambda Durable — Polling / Monitor Pattern

A serverless durable polling workflow built with **AWS Lambda** + **AWS Step Functions**.  
Polls a job status on a configurable interval, sleeps between checks (without keeping Lambda running), and shuts down automatically when the target status is met or the maximum number of attempts is exhausted.

---

## Table of Contents

- [Lambda Durable — Polling / Monitor Pattern](#lambda-durable--polling--monitor-pattern)
  - [Table of Contents](#table-of-contents)
  - [The Problem](#the-problem)
  - [Why Not Just Use `time.sleep()` in Lambda?](#why-not-just-use-timesleep-in-lambda)
  - [The Solution: Durable Orchestration](#the-solution-durable-orchestration)
  - [Architecture](#architecture)
  - [How It Works](#how-it-works)
  - [Project Structure](#project-structure)
  - [Configuration](#configuration)
  - [Getting Started](#getting-started)
    - [Prerequisites](#prerequisites)
    - [Install Python dependencies](#install-python-dependencies)
  - [Local Testing](#local-testing)
    - [Sample event files](#sample-event-files)
  - [Deploying to AWS](#deploying-to-aws)
  - [Starting an Execution](#starting-an-execution)
  - [Implementing Your Own Status Check](#implementing-your-own-status-check)
  - [Key Concepts](#key-concepts)
    - [Amazon States Language (ASL)](#amazon-states-language-asl)
    - [Wait State (the sleep)](#wait-state-the-sleep)
    - [Choice State (branching)](#choice-state-branching)
    - [ResultSelector](#resultselector)
    - [Retry and Catch](#retry-and-catch)
    - [AWS SAM (Serverless Application Model)](#aws-sam-serverless-application-model)

---

## The Problem

You have a long-running job (e.g. a data pipeline, a third-party API call, an ECS task) and you want to:

- Check its status every 2 minutes.
- Do nothing (not consume compute) between checks.
- Automatically stop once the job reaches a target status (e.g. `COMPLETED`).
- Fail safely if the job never completes (max attempts guard).

---

## Why Not Just Use `time.sleep()` in Lambda?

You might think of doing this:

```python
while True:
    status = get_job_status(job_id)
    if status == "COMPLETED":
        break
    time.sleep(120)  # sleep 2 minutes
```

**This is an anti-pattern for several reasons:**

| Problem | Explanation |
|---|---|
| Lambda has a 15-minute max timeout | A job taking longer than 15 minutes will be killed mid-poll |
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
┌─────────────────────────────────────────────────────────────┐
│                    Step Functions                            │
│                                                             │
│  ┌─────────────┐     ┌────────────┐     ┌────────────────┐ │
│  │ CheckStatus │────▶│ IsComplete?│────▶│ WaitForNextPoll│ │
│  │  (Lambda)   │     │  (Choice)  │  No │  (Wait state)  │ │
│  └─────────────┘     └────────────┘     └────────┬───────┘ │
│         ▲                  │ Yes                  │        │
│         └──────────────────┼──────────────────────┘        │
│                            ▼                               │
│                    ┌──────────────┐                        │
│                    │  OnComplete  │                        │
│                    │  (Lambda)    │                        │
│                    └──────┬───────┘                        │
│                           │                               │
│                    ┌──────▼───────┐                        │
│                    │     END      │                        │
│                    └──────────────┘                        │
└─────────────────────────────────────────────────────────────┘
```

---

## How It Works

1. **Start**: You invoke `start_execution.py` with a `job_id`. It calls the Step Functions API to start a new execution, passing in all configuration (interval, max attempts, target status).

2. **CheckStatus** (Lambda): Runs on each poll tick. Calls `get_job_status()` with the `job_id`, increments the attempt counter, and returns whether the status matches the target.

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
lambda-durable/
├── statemachine/
│   └── polling.asl.json          # Step Functions workflow definition (Amazon States Language)
├── src/
│   ├── check_status/
│   │   └── handler.py            # Lambda: runs on every poll tick
│   └── on_complete/
│       └── handler.py            # Lambda: runs once when the target status is met
├── template.yaml                 # AWS SAM infrastructure definition
├── config.json                   # Default configuration values
├── start_execution.py            # Script to kick off a new polling execution
├── event.check_status.json       # Sample event for local Lambda testing
├── event.on_complete.json        # Sample event for local Lambda testing
└── requirements.txt              # Python dependencies
```

---

## Configuration

All polling parameters are configurable at runtime — no code changes needed.

| Parameter | Default | Description |
|---|---|---|
| `poll_interval_seconds` | `120` | Seconds to wait between each poll (e.g. `120` = 2 minutes) |
| `max_attempts` | `30` | Maximum number of polls before the workflow fails |
| `target_status` | `COMPLETED` | The status string that signals the job is done |
| `job_id` | _(required)_ | Identifier of the job to poll |

Edit `config.json` to change defaults, or pass overrides via CLI flags.

---

## Getting Started

### Prerequisites

- [AWS SAM CLI](https://docs.aws.amazon.com/serverless-application-model/latest/developerguide/install-sam-cli.html)
- [Docker Desktop](https://www.docker.com/products/docker-desktop/) (required for `sam local invoke`)
- Python 3.12+
- AWS credentials configured (`aws configure`)

### Install Python dependencies

```bash
pip install -r requirements.txt
```

---

## Local Testing

You can test individual Lambda functions locally using SAM + Docker.

> **Note:** Full Step Functions orchestration (Wait state, state transitions) cannot be run locally. Only individual Lambda functions can be invoked locally via SAM.

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

### Sample event files

`event.check_status.json` — simulates the first poll tick:
```json
{
  "job_id": "test-job-1",
  "target_status": "COMPLETED",
  "poll_interval_seconds": 2,
  "max_attempts": 3,
  "attempts": 0
}
```

`event.on_complete.json` — simulates the final state when the job is done:
```json
{
  "job_id": "test-job-1",
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

```bash
sam build
sam deploy --guided
```

Follow the prompts. After deployment, copy the `StateMachineArn` from the outputs and update `config.json`:

```json
{
  "state_machine_arn": "arn:aws:states:us-east-1:123456789012:stateMachine:YOUR_STACK_NAME-polling",
  ...
}
```

---

## Starting an Execution

```bash
# Use defaults from config.json
python start_execution.py my-job-123

# Override poll interval and max attempts
python start_execution.py my-job-123 --interval 60 --max-attempts 10

# Override the target status
python start_execution.py my-job-123 --target DONE
```

You can monitor the execution in the **AWS Step Functions console** under your state machine name.

---

## Implementing Your Own Status Check

Open `src/check_status/handler.py` and replace the `get_job_status()` function with your real logic:

```python
def get_job_status(job_id: str) -> str:
    # Example: query an external REST API
    import requests
    response = requests.get(f"https://api.example.com/jobs/{job_id}")
    return response.json()["status"]
```

Other common examples:

```python
# DynamoDB
table = boto3.resource("dynamodb").Table("Jobs")
return table.get_item(Key={"job_id": job_id})["Item"]["status"]

# AWS Glue
glue = boto3.client("glue")
run = glue.get_job_run(JobName="my-glue-job", RunId=job_id)
return run["JobRun"]["JobRunState"]

# ECS Task
ecs = boto3.client("ecs")
tasks = ecs.describe_tasks(cluster="my-cluster", tasks=[job_id])
return tasks["tasks"][0]["lastStatus"]
```

Then add your post-completion logic in `src/on_complete/handler.py`.

---

## Key Concepts

### Amazon States Language (ASL)
The Step Functions workflow is defined in `statemachine/polling.asl.json` using **Amazon States Language** — a JSON-based specification for describing state machines. Each state has a `Type` (Task, Choice, Wait, Fail, etc.) and transitions to the next state.

### Wait State (the sleep)
The `WaitForNextPoll` state uses `"SecondsPath": "$.poll_interval_seconds"` to read the interval dynamically from the execution input. While in this state, **no compute is running and no cost is incurred**.

### Choice State (branching)
`IsComplete` is a `Choice` state — it contains conditional rules that branch the workflow based on data in the state. It checks `is_complete` and `attempts` to decide what to do next.

### ResultSelector
After Lambda returns a response, the `ResultSelector` in `CheckStatus` extracts only the fields we care about from the Lambda response envelope (`$.Payload.*`), keeping the state clean and minimal.

### Retry and Catch
Every Task state includes `Retry` rules for transient Lambda errors (throttling, service exceptions) with exponential backoff, and a `Catch` block to route unrecoverable errors to the `PollFailed` terminal state.

### AWS SAM (Serverless Application Model)
`template.yaml` is a **SAM template** — a higher-level abstraction over AWS CloudFormation. It defines Lambdas, Step Functions state machines, IAM roles, and log groups in a concise YAML format. SAM transforms this into raw CloudFormation during deployment.
