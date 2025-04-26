import os
import datetime

def append_audit(output_path, text):
    """
    Append a message to the audit log file locally.
    
    Args:
        output_path (str): Path to the database directory
        text (str): Text message to append to the audit log
    """
    # Create the path for the log file
    log_file_path = os.path.join(output_path, "audit_log.txt")
    
    # Format timestamp and log entry
    timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    log_entry = f"[{timestamp}] {text}\n"
    
    # Create directory if it doesn't exist
    os.makedirs(os.path.dirname(log_file_path), exist_ok=True)
    
    # Append to file (will create if doesn't exist)
    with open(log_file_path, "a") as f:
        f.write(log_entry)