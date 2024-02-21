import pandas as pd
from abc import ABC, abstractmethod
from . import ResourceType, Resource, TaskSet, ResourceSet

class Scheduler(ABC):
    _taskset = None
    _resourceset = None
    _schedule_result = None
    _premption = {
        ResourceType.Processor : True,
        ResourceType.Memory: True
    }
    _schedule_current = None
    _current_time = 0

    def __init__(self):
        self._taskset = None
        self._resourceset = None
        self._premption[ResourceType.Processor] = True
        self._premption[ResourceType.Memory] = True

        schema_schedule_current={'Job': 'string', 'Task': 'string', 'Activation': 'int', 'Phase': 'int', 'Request': 'int', 'Executed': 'bool', 'NonPreemptiveResource': 'string'}
        self._schedule_current = pd.DataFrame(columns=schema_schedule_current.keys()).astype(schema_schedule_current)
        self._schedule_current.set_index('Job', inplace=True)

        schema_schedule_result={'Task': 'string', 'Job': 'string', 'Start': 'int', 'Finish': 'int', 'Resource': 'string', 'Missed': 'string', 'Phase': 'int', 'RequestPhaseRemaining': 'int', 'TotalPhase': 'int', 'TotalRequestPhase': 'int'}
        self._schedule_result = pd.DataFrame(columns=schema_schedule_result.keys()).astype(schema_schedule_result)

    
    def __str__(self):
        return self._schedule_result.__str__()
    
    @property
    def schedule_result(self):
        return self._schedule_result
    
    @property
    def premption_processor(self):
        return self._premption[ResourceType.Processor]
    
    @premption_processor.setter
    def premption_processor(self, value: bool):
        self._premption[ResourceType.Processor] = value

    @property
    def premption_memory(self):
        return self._premption[ResourceType.Memory]
    
    @premption_memory.setter
    def premption_memory(self, value: bool):
        self._premption[ResourceType.Memory] = value
    
    @property
    def taskset(self):
        return self._taskset
    
    @taskset.setter
    def taskset(self, value):
        self._taskset = value
        self._restart_schedule()
    
    @property
    def resourceset(self):
        return self._resourceset
    
    @resourceset.setter
    def resourceset(self, value):
        self._resourceset = value
        self._restart_schedule()

    def configure(self, taskset, resourceset, premption_processor=True, premption_memory=True):
        self._taskset = taskset
        self._resourceset = resourceset
        self._premption[ResourceType.Processor] = premption_processor
        self._premption[ResourceType.Memory] = premption_memory
        self._restart_schedule()
    
    def configure_json(self, data_json: dict):
        self._taskset = TaskSet(data_json["tasks"])
        self._resourceset = ResourceSet(data_json["resources"])
        self._premption[ResourceType.Processor] = data_json["premption_processor"]
        self._premption[ResourceType.Memory] = data_json["premption_memory"]
        self._restart_schedule()

    @abstractmethod
    def _next_job_scheduler(self, resource: Resource, current_time: int, jobs_to_execute: list[str]) -> str: # Job name selected for execution on the resource
        pass

    def _next_job(self, resource: Resource, current_time: int) -> str: # Task name selected for execution on the resource
        job_selected = None
        jobs_to_execute = self._schedule_current[(self._schedule_current["Request"] > 0) & (self._schedule_current["Executed"] == False)].index.tolist()

        # Check if non-preemptive resource
        job_selected_non_premptive = self._schedule_current[(self._schedule_current["NonPreemptiveResource"] == resource.name) & (self._schedule_current["Request"] > 0) & (self._schedule_current["Executed"] == False)].index.tolist()
        if (len(job_selected_non_premptive) > 0):
            job_selected = job_selected_non_premptive[0]
        else:
            # Call abstract method, according to the scheduler algorithm
            job_selected = self._next_job_scheduler(resource, current_time, jobs_to_execute)
        return job_selected
    
    def _restart_schedule(self):
        self._schedule_result = self._schedule_result.iloc[0:0]
        self._schedule_current = self._schedule_current.iloc[0:0]
        self._current_time = 0

    def _schedule_next(self):
        self._schedule_current["Executed"] = False

        # Update schedule current with new task activations.
        for task in self._taskset.tasks:
            if (self._current_time - task.first_activation) % task.period == 0:
                self._schedule_current = pd.concat([self._schedule_current, pd.DataFrame([dict(Task=task.name, Activation=self._current_time, Phase=0, Request=task.phases[0].duration, Executed=False, NonPreemptiveResource="")],index=[task.name+"_"+str(self._current_time)])])

        # Get list of tasks to execute on each resource
        for resource in self._resourceset.resources:
            
            job_selected = self._next_job(resource, self._current_time)

            if job_selected is not None:
                task_name = self._schedule_current.loc[job_selected]["Task"]
                job_current_phase = self._schedule_current.loc[job_selected]["Phase"]
                task = self._taskset.get_task(task_name)
                task_resource_type = task.phases[job_current_phase].ressource_type
                if task_resource_type == resource.type:
                    self._schedule_current.loc[job_selected, "Request"] -= 1
                    self._schedule_current.loc[job_selected, "Executed"] = True
                    
                    if (task.phases[job_current_phase].premption == False or (self._premption[task.phases[job_current_phase].ressource_type] == False)):
                        self._schedule_current.loc[job_selected, "NonPreemptiveResource"] = resource.name

                    # Save schedule result
                    self._schedule_result = pd.concat([self._schedule_result, pd.DataFrame([dict(Task=task_name, Job=job_selected, Start=self._current_time, Finish=self._current_time+1, Resource=resource.name, Missed="", Phase=job_current_phase+1, RequestPhaseRemaining=self._schedule_current.loc[job_selected, "Request"], TotalPhase=len(task.phases), TotalRequestPhase=task.phases[job_current_phase].duration, NonPreemptiveResource=self._schedule_current.loc[job_selected, "NonPreemptiveResource"])])])

                    # Update schedule current with next task phase if task phase is completed
                    if self._schedule_current.loc[job_selected, "Request"] == 0:
                        if (job_current_phase + 1) < len(task.phases):
                            self._schedule_current.loc[job_selected, "Phase"] = (job_current_phase + 1)
                            self._schedule_current.loc[job_selected, "Request"] = task.phases[(job_current_phase + 1)].duration
                            self._schedule_current.loc[job_selected, "NonPreemptiveResource"] = ""
                        else:
                            self._schedule_current.drop([job_selected], inplace=True)

        # Check for missed deadlines
        for job in self._schedule_current.index.tolist():
            task = self._taskset.get_task(self._schedule_current.loc[job]["Task"])
            job_deadline = self._schedule_current.loc[job]["Activation"] + task.deadline
            job_current_phase = self._schedule_current.loc[job]["Phase"]
                
            if (self._current_time+1 >= job_deadline and self._schedule_current.loc[job, "Request"] > 0):
                self._schedule_result = pd.concat([self._schedule_result, pd.DataFrame([dict(Task=task.name,Job=job, Start=self._current_time+1, Finish=self._current_time+1, Resource="", Missed="Missed", Phase=job_current_phase+1, RequestPhaseRemaining=self._schedule_current.loc[job, "Request"], TotalPhase=len(task.phases), TotalRequestPhase=task.phases[job_current_phase].duration, NonPreemptiveResource=self._schedule_current.loc[job, "NonPreemptiveResource"])])])

        
        self._current_time += 1
    
    def schedule(self, max_time: int=40):
        self._restart_schedule()

        for t in range(max_time):
            self._current_time = t
            self._schedule_next()