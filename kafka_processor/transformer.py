import hashlib
from producer import log_error_to_kafka


def transform_message(msg_dict, producer, error_topic):
    """
    Transforms the input message dictionary by hashing sensitive fields and formatting the timestamp.

    Args:
        msg_dict (dict): The original message dictionary containing user data.
        producer (Producer): The Kafka producer instance used for logging errors.
        error_topic (str): The Kafka topic where transformation errors are logged.

    Returns:
        dict or None: The transformed message dictionary, or None if an error occurs.
    """
    try:
        # Hash the 'ip' address to protect sensitive information
        msg_dict["ip"] = hash_value(msg_dict["ip"])
        # Hash the 'device_id' to protect sensitive information
        msg_dict["device_id"] = hash_value(msg_dict["device_id"])
        # Format the 'timestamp' from Unix time to a readable string format
        msg_dict["timestamp"] = format_timestamp(msg_dict["timestamp"])
    except Exception as e:
        # If an error occurs during transformation, log the error with relevant details
        user_id = msg_dict.get(
            "user_id", "Unknown"
        )  # Safely get user_id, default to 'Unknown' if not present
        log_error_to_kafka(
            producer, error_topic, user_id, f"Transformation error: {str(e)}", msg_dict
        )
        return None  # Return None to indicate failure

    return msg_dict  # Return the transformed message dictionary


def hash_value(value):
    """
    Hashes a given string value using SHA-256 for security.

    Args:
        value (str): The string value to be hashed.

    Returns:
        str: The hexadecimal representation of the hashed value.
    """
    return hashlib.sha256(
        value.encode()
    ).hexdigest()  # Encode and hash the value, returning the hex digest


def format_timestamp(unix_timestamp):
    """
    Formats a Unix timestamp into a human-readable string.

    Args:
        unix_timestamp (int): The Unix timestamp to be formatted.

    Returns:
        str: The formatted timestamp as a string.
    """
    from datetime import datetime

    return datetime.fromtimestamp(unix_timestamp).strftime(
        "%Y-%m-%d %H:%M:%S"
    )  # Convert and format the timestamp
