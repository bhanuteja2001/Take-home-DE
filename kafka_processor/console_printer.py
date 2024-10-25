from collections import defaultdict

class ConsolePrinter:
    def __init__(self):
        self.filtered_count = 0  # Counter for filtered records
        self.processed_count = 0  # Counter for processed records
        self.device_type_counts = defaultdict(int)
        self.locale_counts = defaultdict(int)
        self.missing_field_counts = defaultdict(int)  # Counts for missing fields

    def summarize_missing_fields(self):
        """
        Summarize missing fields after processing a batch of messages.
        :return: A summary string of missing fields and their counts.
        """
        summaries = [f'In {count} records, {field} is missing.' for field, count in self.missing_field_counts.items() if count > 0]
        return '\n'.join(summaries)

    def format_counts(self):
        """
        Format devices and locales into a readable string output.
        :return: Formatted string of device type counts and locale counts.
        """
        device_summary = ', '.join([f'{device_type}: {count}' for device_type, count in sorted(self.device_type_counts.items())])
        locale_summary = ', '.join([f'{locale}: {count}' for locale, count in sorted(self.locale_counts.items())])
        formatted_output = f'Device type counts: {device_summary}\nLocale counts: {locale_summary}\n'
        return formatted_output

    def print_summary(self):
        """
        Print the summary of counts and missing field statistics.
        """
        print(f'Summary after {self.processed_count} records:')
        print(self.format_counts())
        print(f'Total filtered out records so far: {self.filtered_count}')
        print(self.summarize_missing_fields())
        print('\n' + '-'*50 + '\n')  # Add separator

    def update_counts(self, device_type, locale):
        """
        Update device type and locale counts with new records.
        :param device_type: The device type to count.
        :param locale: The locale to count.
        """
        self.device_type_counts[device_type] += 1
        self.locale_counts[locale] += 1
        self.processed_count += 1

    def increment_filtered_count(self):
        """
        Increment the counter for filtered-out records.
        """
        self.filtered_count += 1

    def increment_missing_field_count(self, field):
        """
        Increment the counter for missing fields.
        
        :param field: The field that is missing.
        """
        self.missing_field_counts[field] += 1