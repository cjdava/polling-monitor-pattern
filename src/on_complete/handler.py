import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)



def lambda_handler(event: dict, context) -> dict:
    """
    Runs once when the target status has been met. Use this to trigger
    any downstream logic (notifications, cleanup, further processing, etc.).

    Receives the final polling state:
        {
            "process_id": "my-process-123",
            "status": "COMPLETED",
            "is_complete": true,
            "attempts": 5,
            "poll_interval_seconds": 120,
            "max_attempts": 30,
            "target_status": "COMPLETED"
        }
    """
    process_id: str = event["process_id"]
    final_status: str = event["status"]
    total_attempts: int = event["attempts"]

    logger.info(
        "Process complete | process_id=%s final_status=%s total_attempts=%d",
        process_id,
        final_status,
        total_attempts,
    )

    # TODO: Add your post-completion logic here.
    # Examples:
    #
    #   Send an SNS notification:
    #       sns = boto3.client("sns")
    #       sns.publish(
    #           TopicArn="arn:aws:sns:us-east-1:123456789012:MyTopic",
    #           Message=f"Process {process_id} finished with status {final_status}",
    #       )
    #
    #   Trigger a downstream Step Functions execution:
    #       sfn = boto3.client("stepfunctions")
    #       sfn.start_execution(
    #           stateMachineArn="arn:aws:states:...",
    #           input=json.dumps({"process_id": process_id}),
    #       )
    #
    #   Update a DynamoDB record:
    #       table = boto3.resource("dynamodb").Table("Processes")
    #       table.update_item(
    #           Key={"process_id": process_id},
    #           UpdateExpression="SET completion_status = :s",
    #           ExpressionAttributeValues={":s": final_status},
    #       )

    return {
        "message": f"Polling complete for process_id={process_id}",
        "final_status": final_status,
        "total_attempts": total_attempts,
    }
