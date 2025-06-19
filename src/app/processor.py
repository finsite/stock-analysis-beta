"""Processor module for computing beta signals from input messages.

This module validates incoming messages and computes a derived beta signal
based on the input data. All operations are logged for observability.
"""

from typing import Any, cast

from app.utils.setup_logger import setup_logger
from app.utils.types import ValidatedMessage
from app.utils.validate_data import validate_message_schema

logger = setup_logger(__name__)


def validate_input_message(message: dict[str, Any]) -> ValidatedMessage:
    """
    Validate the incoming raw message against the expected schema.

    Parameters:
        message (dict[str, Any]): The raw message payload.

    Returns:
        ValidatedMessage: A validated message object.
    """
    logger.debug("🔍 Validating message schema...")
    if not validate_message_schema(message):
        logger.error("❌ Message schema invalid: %s", message)
        raise ValueError("Invalid message format")

    if not all(k in message for k in ("symbol", "timestamp", "data")):
        logger.error("❌ Missing required keys in message: %s", message)
        raise ValueError("Message is missing required keys")

    return cast(ValidatedMessage, message)


def compute_beta_signal(message: ValidatedMessage) -> dict[str, Any]:
    """
    Compute a beta signal from the validated input message.

    This function is a placeholder for beta calculations, typically representing
    sensitivity to market returns.

    Parameters:
        message (ValidatedMessage): The validated message input.

    Returns:
        dict[str, Any]: Dictionary with beta-related data.
    """
    symbol = message["symbol"]
    logger.debug("📊 Computing beta signal for %s", symbol)

    # Replace this with actual beta computation logic
    beta_score = 0.85

    return {
        "symbol": symbol,
        "timestamp": message["timestamp"],
        "beta_score": beta_score,
    }


def process_message(raw_message: dict[str, Any]) -> ValidatedMessage:
    """
    Main entry point for processing a single message.

    Parameters:
        raw_message (dict[str, Any]): Raw input from the message queue.

    Returns:
        ValidatedMessage: Enriched and validated message ready for output.
    """
    logger.info("🚦 Processing new message...")
    validated = validate_input_message(raw_message)
    beta_data = compute_beta_signal(validated)

    enriched: ValidatedMessage = {
        "symbol": validated["symbol"],
        "timestamp": validated["timestamp"],
        "data": {**validated["data"], **beta_data},
    }
    logger.debug("✅ Final enriched message: %s", enriched)
    return enriched
