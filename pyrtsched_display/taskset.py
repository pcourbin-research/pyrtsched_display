import json
import pandas as pd
from . import ResourceType
from . import Task
from typing import Optional

class TaskSet:
    _tasks = []

    def __init__(self, data):
        self._tasks = []
        if (isinstance(data, str)):
            data = json.loads(data)

        for task_data in data:
            task = Task(task_data["Name"], task_data["O"], task_data["D"], task_data["T"])
            if ("C" in task_data.keys()):
                task.add_phase(ResourceType.Processor, task_data["C"], True)
            elif ("Phases" in task_data.keys()):
                for phase in task_data["Phases"]:
                    type = ResourceType[phase["Type"]]
                    duration = phase["Duration"]
                    premption = True
                    if ("Premption" in phase.keys()):
                        premption = phase["Premption"]
                    resumable = True
                    if ("Resumable" in phase.keys()):
                        resumable = phase["Resumable"]
                    task.add_phase(type, duration, premption, resumable)
            elif ("R" in task_data.keys() and "E" in task_data.keys() and "W" in task_data.keys()):
                task.add_phase(ResourceType.Memory, task_data["R"], True)
                task.add_phase(ResourceType.Processor, task_data["E"], True)
                task.add_phase(ResourceType.Memory, task_data["W"], True)

            self._tasks.append(task)

    def __str__(self):
        string = "Tasks:\n"
        for task in self._tasks:
            string += task.__str__() + "\n"
        return string

    @property
    def tasks(self) -> list[Task]:
        return self._tasks

    def get_task(self, name: str) -> Optional[Task]:
        for task in self._tasks:
            if task.name == name:
                return task
        return None
    
    def get_taskset_as_dataframe(self):
        schema_taskset={'Name': 'string', 'O': 'int', 'D': 'int', 'T': 'int'}
        df_taskset = pd.DataFrame(columns=list(schema_taskset.keys())).astype(schema_taskset)
        for task in self._tasks:
            df_taskset = pd.concat([df_taskset, pd.DataFrame([dict(Name=task.name, O=task.first_activation, D=task.deadline, T=task.period)])])
        return df_taskset
    