from collections import defaultdict

class SummaryPrinter:
    def __init__(self):
        self.filtered_count = 0
        self.processed_count = 0
        self.device_type_counts = defaultdict(int)
        self.locale_counts = defaultdict(int)
        self.missing_field_counts = defaultdict(int)

    def get_summary_statistics(self):
        return {
            'processed_count': self.processed_count,
            'device_type_counts': dict(self.device_type_counts),
            'locale_counts': dict(self.locale_counts)
        }

    def get_summary_errors(self):
        return {
            'missing_field_counts': dict(self.missing_field_counts),
            'filtered_count': self.filtered_count
        }

    def update_counts(self, device_type, locale):
        self.device_type_counts[device_type] += 1
        self.locale_counts[locale] += 1
        self.processed_count += 1

    def increment_filtered_count(self):
        self.filtered_count += 1

    def increment_missing_field_count(self, field):
        self.missing_field_counts[field] += 1