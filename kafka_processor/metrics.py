"""
Module for collecting and managing message processing metrics.
"""

import time
from typing import List, Dict, Any
from producer import publish_message

class MessageMetrics:
    """A class to collect and manage metrics for processed messages."""

    def __init__(self):
        """Initialize the MessageMetrics with an empty list of message details."""
        self.message_details: List[Dict[str, Any]] = []

    def record_message(self, original_timestamp: float, topic: str, message_id: str) -> None:
        """
        Record metrics for a processed message.

        Args:
            original_timestamp (float): The timestamp of when the message was originally produced.
            topic (str): The Kafka topic the message was published to.
            message_id (str): A unique identifier for the message.
        """
        current_time = time.time()
        processing_time = current_time - original_timestamp
        self.message_details.append({
            'message_id': message_id,
            'processing_time': processing_time,
            'topic': topic,
            'timestamp': current_time
        })

    def get_metrics(self) -> Dict[str, List[Dict[str, Any]]]:
        """
        Get all recorded message metrics.

        Returns:
            Dict[str, List[Dict[str, Any]]]: A dictionary containing all recorded message details.
        """
        return {"message_details": self.message_details}

    def clear_message_details(self) -> None:
        """Clear all recorded message details."""
        self.message_details = []

    def record_and_publish_metrics(self, metrics_producer, original_timestamp, topic, message_id):
        """Record and publish metrics."""
        self.record_message(original_timestamp, topic, message_id)
        metrics_data = self.get_metrics()
        publish_message(metrics_producer, 'metrics-output', metrics_data)
        self.clear_message_details()