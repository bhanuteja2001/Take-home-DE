"""
This module provides functions to transform incoming messages.
"""

import hashlib

def hash_value(value: str) -> str:
    """
    Hash a given string value using SHA-256.

    Args:
        value (str): The string to be hashed.

    Returns:
        str: The hexadecimal representation of the hashed value.
    """
    return hashlib.sha256(value.encode()).hexdigest()

def transform_message(msg_dict: dict) -> tuple:
    """
    Transform the input message by hashing sensitive fields.

    Args:
        msg_dict (dict): The original message dictionary.

    Returns:
        tuple: A tuple containing the transformed message (or None if error)
               and an error message (or None if successful).
    """
    try:
        transformed_msg = msg_dict.copy()
        transformed_msg["ip"] = hash_value(msg_dict["ip"])
        transformed_msg["device_id"] = hash_value(msg_dict["device_id"])
        return transformed_msg, None
    except Exception as e:
        return None, str(e)