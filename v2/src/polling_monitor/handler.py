import logging
from aws_durable_execution_sdk_python import DurableContext, StepContext, durable_execution, durable_step
from aws_durable_execution_sdk_python.config import Duration

logging.getLogger().setLevel(logging.INFO)


@durable_step
def check_process_status(ctx: StepContext, process_id: str, attempt: int, max_attempts: int) -> str:
    """
    Stub status check — replace with your real logic, e.g.:
        - Query DynamoDB for a job record
        - Call an external REST API
        - Check an AWS Glue job run state
        - Describe an ECS task status
    """
    ctx.logger.info(
        "Checking process status | process_id=%s attempt=%d/%d",
        process_id,
        attempt,
        max_attempts,
    )
    return "IN_PROGRESS"


@durable_step
def on_complete(ctx: StepContext, process_id: str, status: str, attempts: int) -> dict:
    """
    Runs once when the target status is reached — replace with your real logic, e.g.:
        - Publish to SNS
        - Start a downstream Step Functions execution
        - Update a DynamoDB record
        - Send a webhook
    """
    ctx.logger.info(
        "Process completed | process_id=%s status=%s attempts=%d",
        process_id,
        status,
        attempts,
    )
    return {
        "message": f"Process {process_id} completed with status {status}",
        "attempts": attempts,
    }


@durable_execution
def lambda_handler(event: dict, context: DurableContext) -> dict:
    """
    Durable polling monitor using AWS Lambda Durable Execution SDK.

    Replaces the Step Functions + Lambda orchestration pattern with a single
    Lambda function that checkpoints progress automatically. On failure or
    restart, completed steps are skipped and execution resumes from the last
    checkpoint.

    Expected event shape:
        {
            "process_id": "my-process-123",
            "target_status": "COMPLETED",
            "poll_interval_seconds": 120,
            "max_attempts": 30
        }
    """
    process_id: str = event["process_id"]
    target_status: str = event["target_status"]
    poll_interval_seconds: int = event["poll_interval_seconds"]
    max_attempts: int = event["max_attempts"]

    context.logger.info(
        "Starting polling monitor | process_id=%s target=%s max_attempts=%d",
        process_id,
        target_status,
        max_attempts,
    )

    for attempt in range(1, max_attempts + 1):
        # Each check is a named checkpoint — on replay, completed steps
        # return their stored result instantly without re-executing.
        status = context.step(
            check_process_status(process_id, attempt, max_attempts),
            name=f"check-status-{attempt}",
        )

        context.logger.info(
            "Poll result | process_id=%s attempt=%d/%d status=%s",
            process_id,
            attempt,
            max_attempts,
            status,
        )

        if status == target_status:
            result = context.step(
                on_complete(process_id, status, attempt),
                name="on-complete",
            )
            return {
                "process_id": process_id,
                "status": status,
                "attempts": attempt,
                "result": result,
            }

        if attempt < max_attempts:
            # Sleep without consuming compute. Lambda is recycled during the
            # wait and replayed from this checkpoint when resumed.
            context.wait(Duration.from_seconds(poll_interval_seconds))

    raise Exception(
        f"Max attempts reached | process_id={process_id} "
        f"attempts={max_attempts} last_status={status}"
    )

