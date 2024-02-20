from scheduler import Scheduler
from resource import Resource


class SchedulerDM(Scheduler):
    def _next_task_scheduler(self, resource: Resource, tasks_to_execute: list[str]) -> str: # Task name selected for execution on the resource
        resource_type = resource.type
        min_deadline = -1
        task_selected = None
        for task_name in tasks_to_execute:
            task = self._taskset.get_task(task_name)
            task_phase = self._schedule_current.loc[task_name]["Phase"]
            task_resource_type = task.phases[task_phase].ressource_type
            if ((min_deadline == -1 or task.deadline < min_deadline) and task_resource_type == resource_type):
                min_deadline = task.deadline
                task_selected = task_name
        return task_selected