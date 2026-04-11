import json
import logging
import os

import boto3

logger = logging.getLogger()
logger.setLevel(logging.INFO)

lambda_client = boto3.client("lambda")


def lambda_handler(event: dict, context) -> dict:
    """
    Invokes the durable polling monitor Lambda asynchronously.

    Expected event shape (all fields optional if environment variables are set):
        {
            "process_id": "my-process-123",
            "target_status": "COMPLETED",       # default: COMPLETED
            "poll_interval_seconds": 10,        # default: 10
            "max_attempts": 3                   # default: 3
        }

    Environment variables:
        DURABLE_FUNCTION_NAME  - ARN or name of the durable polling monitor Lambda
    """
    function_name = os.environ["DURABLE_FUNCTION_NAME"]

    payload = {
        "process_id": event["process_id"],
        "target_status": event.get("target_status", "COMPLETED"),
        "poll_interval_seconds": event.get("poll_interval_seconds", 10),
        "max_attempts": event.get("max_attempts", 3),
    }

    logger.info(
        "Invoking durable function | function=%s process_id=%s",
        function_name,
        payload["process_id"],
    )

    response = lambda_client.invoke(
        FunctionName=function_name,
        InvocationType="Event",  # async — fire and forget
        Payload=json.dumps(payload),
    )

    status_code = response["StatusCode"]
    logger.info("Invocation response | status_code=%d", status_code)

    return {
        "invoked_function": function_name,
        "status_code": status_code,
        "process_id": payload["process_id"],
    }
