from . import Scheduler
from . import Resource

class SchedulerEDF(Scheduler):
    
    def _job_priority(self, job_name: str) -> int: # Job absolute deadline.
        task_name = self._schedule_current.loc[job_name]["Task"]
        task = self._taskset.get_task(task_name)
        job_activation = self._schedule_current.loc[job_name]["Activation"]
        job_deadline = job_activation + task.deadline
        return job_deadline