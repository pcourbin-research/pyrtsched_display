from . import Scheduler
from . import Resource

class SchedulerDM(Scheduler):
    def _next_job_scheduler(self, resource: Resource, current_time: int, jobs_to_execute: list[str]) -> str: # Job name selected for execution on the resource
        resource_type = resource.type
        min_deadline = -1
        job_selected = None
        job_selected_activation = -1
        for job_name in jobs_to_execute:
            task_name = self._schedule_current.loc[job_name]["Task"]
            task = self._taskset.get_task(task_name)
            job_current_phase = self._schedule_current.loc[job_name]["Phase"]
            job_activation = self._schedule_current.loc[job_name]["Activation"]
            job_resource_type = task.phases[job_current_phase].ressource_type
            if ((min_deadline == -1 or task.deadline < min_deadline or (task.deadline == min_deadline and job_activation < job_selected_activation)) and job_resource_type == resource_type):
                min_deadline = task.deadline
                job_selected = job_name
                job_selected_activation = job_activation
        return job_selected