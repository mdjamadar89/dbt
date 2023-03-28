import time, datetime, json, pytz
from pytz import timezone

class Logging():
    def __init__(self, job_name, job_completion_status, error_level,
                 error_message, actions, start_time, file_name,
                 success_message=None, validation_status=None):
        self.job_name = job_name
        self.job_completion_status = job_completion_status
        self.error_level = f"P{error_level}"
        self.error_message = error_message
        self.actions = actions
        self.seconds = round(time.time() - start_time, 2)
        self.validation_status = validation_status
        self.file_name = file_name
        self.success_message = success_message
        mountain = timezone('US/Mountain')
        self.timestamp = pytz.utc.localize(datetime.datetime.utcnow()).astimezone(mountain)

    def logs(self):
        log = {
            "timestamp": self.timestamp.strftime("%Y-%m-%d %-I:%M:%S %p 'US/Mountain'"),
            "job_name": self.job_name,
            "repo_name": 'dbt',
            "file_name": self.file_name,
            "job_completion_status": self.job_completion_status,
            "priority_level": self.error_level,
            "error_message": self.error_message,
            "actions_to_fix": self.actions,
            "seconds_to_run_job": self.seconds
        }
        if self.validation_status is not None:
            log["validation_status"] = self.validation_status

        if self.success_message is not None:
            log["success_message"] = self.success_message

        return json.dumps(log)