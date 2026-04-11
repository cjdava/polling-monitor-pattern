"""
start_execution.py — Kicks off a new durable polling execution.

Usage:
    python start_execution.py <process_id>

    # Override defaults from config.json at the command line:
    python start_execution.py <process_id> --interval 60 --max-attempts 10 --target DONE
"""

import argparse
import json
import uuid
import sys

import boto3


def load_config(path: str = "config.json") -> dict:
    with open(path) as f:
        return json.load(f)


def start_polling(
    process_id: str,
    state_machine_arn: str,
    poll_interval_seconds: int,
    max_attempts: int,
    target_status: str,
) -> str:
    client = boto3.client("stepfunctions")

    execution_input = {
        "process_id": process_id,
        "target_status": target_status,
        "poll_interval_seconds": poll_interval_seconds,
        "max_attempts": max_attempts,
        "attempts": 0,
    }

    # Execution names must be unique within a state machine
    execution_name = f"poll-{process_id[:40]}-{uuid.uuid4().hex[:8]}"

    print(f"Starting execution: {execution_name}")
    print(f"  process_id        : {process_id}")
    print(f"  target_status     : {target_status}")
    print(f"  poll_interval     : {poll_interval_seconds}s")
    print(f"  max_attempts      : {max_attempts}")
    print()

    response = client.start_execution(
        stateMachineArn=state_machine_arn,
        name=execution_name,
        input=json.dumps(execution_input),
    )

    execution_arn = response["executionArn"]
    print(f"Execution started successfully.")
    print(f"  ARN: {execution_arn}")
    return execution_arn


def main():
    parser = argparse.ArgumentParser(description="Start a durable polling execution.")
    parser.add_argument("process_id", help="The process ID to poll")
    parser.add_argument(
        "--interval",
        type=int,
        default=None,
        help="Poll interval in seconds (overrides config.json)",
    )
    parser.add_argument(
        "--max-attempts",
        type=int,
        default=None,
        dest="max_attempts",
        help="Max polling attempts (overrides config.json)",
    )
    parser.add_argument(
        "--target",
        type=str,
        default=None,
        help="Target status string (overrides config.json)",
    )
    parser.add_argument(
        "--config",
        type=str,
        default="config.json",
        help="Path to config file (default: config.json)",
    )
    args = parser.parse_args()

    config = load_config(args.config)

    start_polling(
        process_id=args.process_id,
        state_machine_arn=config["state_machine_arn"],
        poll_interval_seconds=args.interval or config["poll_interval_seconds"],
        max_attempts=args.max_attempts or config["max_attempts"],
        target_status=args.target or config["target_status"],
    )


if __name__ == "__main__":
    main()
