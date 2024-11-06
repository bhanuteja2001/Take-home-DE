"""
This module provides a class to track and summarize message processing statistics.
(Persistent storage can be used to save the state)

"""

from collections import defaultdict

class SummaryPrinter:
    """A class to manage and summarize processed data statistics."""

    def __init__(self):
        """Initialize the SummaryPrinter with counters for various metrics."""
        self.total_received_count = 0
        self.processed_count = 0
        self.filtered_count = 0
        self.device_type_counts = defaultdict(int)
        self.locale_counts = defaultdict(int)

    def increment_received_count(self) -> None:
        """Increment the count of total received messages."""
        self.total_received_count += 1

    def increment_processed_count(self) -> None:
        """Increment the count of processed messages."""
        self.processed_count += 1

    def increment_filtered_count(self) -> None:
        """Increment the count of filtered messages."""
        self.filtered_count += 1

    def update_counts(self, device_type: str, locale: str) -> None:
        """
        Update the counts for device type and locale.

        Args:
            device_type (str): The type of device.
            locale (str): The locale of the message.
        """
        self.device_type_counts[device_type] += 1
        self.locale_counts[locale] += 1

    def get_summary_statistics(self) -> dict:
        """
        Get a summary of all tracked statistics.

        Returns:
            dict: A dictionary containing all the summary statistics.
        """
        return {
            "total_received_count": self.total_received_count,
            "processed_count": self.processed_count,
            "filtered_count": self.filtered_count,
            "device_type_counts": dict(self.device_type_counts),
            "locale_counts": dict(self.locale_counts)
        }