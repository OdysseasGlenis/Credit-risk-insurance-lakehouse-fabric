# 04_data_quality_checks.py

from datetime import datetime
import uuid

run_id = str(uuid.uuid4())
results = []

def add_result(table_name, check_name, status, failed_records, total_records, threshold=None, error_message=None):
    results.append((run_id, table_name, check_name, status, int(failed_records), int(total_records), threshold, datetime.utcnow().isoformat(), error_message))
