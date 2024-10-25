from collections import defaultdict


class SummaryPrinter:
    """
    A class to manage and summarize processed data statistics.
    """

    def __init__(self):
        """Initializes the SummaryPrinter with counters for various metrics."""
        self.filtered_count = 0
        self.processed_count = 0
        self.device_type_counts = defaultdict(int)
        self.locale_counts = defaultdict(int)

    def get_summary_statistics(self):
        """
        Returns a summary of the statistics including processed counts,
        device type counts, locale counts, and filtered record count.

        :return: A dictionary with summary statistics.
        """

        return {
            "processed_count": self.processed_count,
            "device_type_counts": dict(self.device_type_counts),
            "locale_counts": dict(self.locale_counts),
            "filtered_record_count": self.filtered_count,
        }

    def update_counts(self, device_type, locale):
        """
        Updates the counts of processed records for the given device type
        and locale.

        :param device_type: The type of device being processed.
        :param locale: The locale associated with the record.
        """

        self.device_type_counts[device_type] += 1
        self.locale_counts[locale] += 1
        self.processed_count += 1

    def increment_filtered_count(self):
        """Increments the count that is being filtered."""
        self.filtered_count += 1
