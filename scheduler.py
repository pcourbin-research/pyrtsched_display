import pandas as pd
from taskset import TaskSet
from task import Task
from resource import ResourceType, Resource
from resourceset import ResourceSet

class Scheduler:
    _taskset = None
    _resourceset = None
    _schedule_result = None
    _premption_processor = True
    _premption_memory = False
    _schedule_current = None

    def __init__(self, taskset, resourceset, premption_processor=True, premption_memory=False):
        self._taskset = taskset
        self._resourceset = resourceset
        self._premption_processor = premption_processor
        self._premption_memory = premption_memory

        schema_schedule_current={'Task': 'string', 'Phase': 'int', 'Request': 'int', 'Executed': 'bool'}
        self._schedule_current = pd.DataFrame(columns=schema_schedule_current.keys()).astype(schema_schedule_current)
        
        schema_schedule_result={'Task': 'string', 'Start': 'int', 'Finish': 'int', 'Resource': 'string', 'Missed': 'string'}
        self._schedule_result = pd.DataFrame(columns=schema_schedule_result.keys()).astype(schema_schedule_result)


    def _next_task(self, resource_type) -> Task:
        #DM
        tasks_to_execute = self._schedule_current[(self._schedule_current["Request"] > 0) & (self._schedule_current["Executed"] == False)].index.tolist()

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
    
    def schedule(self, max_time=40):
        self._schedule_current = self._schedule_current.iloc[0:0]
        for task in self._taskset.tasks:
            self._schedule_current = pd.concat([self._schedule_current, pd.DataFrame([dict(Task=task.name, Phase=0, Request=0, Executed=False)])])
        self._schedule_current.set_index('Task', inplace=True)
        
        self._schedule_result = self._schedule_result.iloc[0:0]

        for t in range(max_time):
            self._schedule_current["Executed"] = False
            # Update schedule current with new task activations
            for task in self._taskset.tasks:
                if (t - task.first_activation) % task.period == 0:
                    self._schedule_current.loc[task.name, "Phase"] = 0
                    self._schedule_current.loc[task.name, "Request"] += task.phases[0].duration
                    self._schedule_current.loc[task.name, "Executed"] = False

            # Get list of tasks to execute
            for resource in self._resourceset.resources:
                task_selected = self._next_task(resource.type)
                if task_selected is not None:
                    task = self._taskset.get_task(task_selected)
                    task_phase = self._schedule_current.loc[task_selected]["Phase"]
                    task_resource_type = task.phases[task_phase].ressource_type
                    if task_resource_type == resource.type:
                        self._schedule_current.loc[task_selected, "Request"] -= 1
                        self._schedule_current.loc[task_selected, "Executed"] = True
                        self._schedule_result = pd.concat([self._schedule_result, pd.DataFrame([dict(Task=task_selected, Start=t, Finish=t+1, Resource=resource.name, Missed="")])])
                        
                        # Update schedule current with next task phase if task phase is completed
                        if self._schedule_current.loc[task.name, "Request"] == 0 and (task_phase + 1) < len(task.phases):
                            self._schedule_current.loc[task.name, "Phase"] = (task_phase + 1)
                            self._schedule_current.loc[task.name, "Request"] = task.phases[(task_phase + 1)].duration
                            

            # Check for missed deadlines
            for task in self._taskset.tasks:
                if (t + 1 - task.first_activation - task.deadline) % task.period == 0:
                    if self._schedule_current.loc[task.name, "Request"] > 0:
                        self._schedule_result = pd.concat([self._schedule_result, pd.DataFrame([dict(Task=task.name, Start=t+1, Finish=t+1, Resource="", Missed="Missed")])])
    
    def __str__(self):
        return self._schedule_result.__str__()
    
    @property
    def schedule_result(self):
        return self._schedule_result