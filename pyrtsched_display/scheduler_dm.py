from . import Scheduler
from . import Resource

class SchedulerDM(Scheduler):

    def _job_priority(self, job_name: str) -> int: # Job relative deadline.
        task_name = self._schedule_current.loc[job_name]["Task"]
        task = self._taskset.get_task(task_name)
        job_deadline = task.deadline
        return job_deadline