import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def lambda_handler(event: dict, context) -> dict:
    """
    Checks the current status of a process and returns whether the target status has been met.

    Expected event shape (passed through from the Step Functions execution input):
        {
            "process_id": "my-process-123",
            "target_status": "COMPLETED",
            "poll_interval_seconds": 120,
            "max_attempts": 30,
            "attempts": 0          # incremented on each invocation
        }
    """
    process_id: str = event["process_id"]
    target_status: str = event["target_status"]
    poll_interval_seconds: int = event["poll_interval_seconds"]
    max_attempts: int = event["max_attempts"]
    attempts: int = event.get("attempts", 0) + 1  # always increment

    logger.info(
        "Checking status | process_id=%s attempt=%d/%d",
        process_id,
        attempts,
        max_attempts,
    )

    current_status = get_process_status(process_id, attempts, max_attempts)
    is_complete = current_status == target_status

    logger.info(
        "Status result | process_id=%s status=%s is_complete=%s",
        process_id,
        current_status,
        is_complete,
    )

    return {
        "process_id": process_id,
        "status": current_status,
        "is_complete": is_complete,
        "attempts": attempts,
        "poll_interval_seconds": poll_interval_seconds,
        "max_attempts": max_attempts,
        "target_status": target_status,
    }


def get_process_status(process_id: str, attempts: int, max_attempts: int) -> str:
    """
    Fake status check for local/demo testing.

    Returns:
        'IN_PROGRESS' for the first (max_attempts - 1) attempts.
        'COMPLETED'   on attempt >= max_attempts.

    Replace this function with your real status-check logic for production.
    """
    if attempts >= max_attempts:
        logger.info(
            "Simulated completion | process_id=%s attempts=%d",
            process_id,
            attempts,
        )
        return "COMPLETED"

    logger.info(
        "Simulated in-progress | process_id=%s attempts=%d (completes at attempt %d)",
        process_id,
        attempts,
        max_attempts,
    )
    return "IN_PROGRESS"
