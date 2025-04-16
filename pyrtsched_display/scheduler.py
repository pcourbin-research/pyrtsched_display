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
        self._memory_use_processor = False

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
    def memory_use_processor(self):
        return self._memory_use_processor
    
    @memory_use_processor.setter
    def memory_use_processor(self, value: bool):
        self._memory_use_processor = value

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

    def configure(self, taskset, resourceset, premption_processor=True, premption_memory=True, memory_use_processor=False):
        self._taskset = taskset
        self._resourceset = resourceset
        self._premption[ResourceType.Processor] = premption_processor
        self._premption[ResourceType.Memory] = premption_memory
        self._memory_use_processor = memory_use_processor
        self._restart_schedule()
    
    def configure_json(self, data_json: dict):
        self._taskset = TaskSet(data_json["tasks"])
        self._resourceset = ResourceSet(data_json["nb_processors"])
        if (isinstance(data_json["premption_processor"], str)):
            data_json["premption_processor"] = data_json["premption_processor"]=="True"
        if (isinstance(data_json["premption_memory"], str)):
            data_json["premption_memory"] = data_json["premption_memory"]=="True"
        self._premption[ResourceType.Processor] = data_json["premption_processor"]
        self._premption[ResourceType.Memory] = data_json["premption_memory"]
        if (isinstance(data_json["memory_use_processor"], str)):
            data_json["memory_use_processor"] = data_json["memory_use_processor"]=="True"
        self._memory_use_processor = data_json["memory_use_processor"]
        self._restart_schedule()

    @abstractmethod
    def _job_priority(self, job_name: str) -> int: # Job priority. Lower value = higher priority.
        pass

    def _sort_jobs_by_priority(self) -> list[str]: # Sort jobs by priority
        jobs_to_execute = self._schedule_current[(self._schedule_current["Request"] > 0) & (self._schedule_current["Executed"] == False)].index.tolist()
        # Call abstract method _job_priority, according to the scheduler algorithm
        jobs_sorted = sorted(jobs_to_execute, key=lambda job_name: self._job_priority(job_name))
        return jobs_sorted  
    
    def _schedule_job_on_resources(self, resources: list[Resource], job: str):
        task_name = self._schedule_current.loc[job]["Task"]
        job_current_phase = self._schedule_current.loc[job]["Phase"]
        task = self._taskset.get_task(task_name)
        task_resource_type = task.phases[job_current_phase].ressource_type

        self._schedule_current.loc[job, "NonPreemptiveResource"] = ""
        for resource in resources:
            assert(task_resource_type == resource.type or (task_resource_type == ResourceType.Memory and resource.type == ResourceType.Processor and self._memory_use_processor == True)), f"Task {task_name} phase {job_current_phase} resource type {task_resource_type} is not compatible with resource {resource.name} type {resource.type} and memory_use_processor is {self._memory_use_processor}."
            
            if (task.phases[job_current_phase].premption == False or (self._premption[task.phases[job_current_phase].ressource_type] == False)):
                if (self._schedule_current.loc[job, "NonPreemptiveResource"] == ""):
                    self._schedule_current.loc[job, "NonPreemptiveResource"] = resource.name
                else:
                    self._schedule_current.loc[job, "NonPreemptiveResource"] += ", " + resource.name

            # Save schedule result
            self._schedule_result = pd.concat([self._schedule_result, pd.DataFrame([dict(Task=task_name, Job=job, Start=self._current_time, Finish=self._current_time+1, Resource=resource.name, Missed="", Phase=job_current_phase+1, RequestPhaseRemaining=self._schedule_current.loc[job, "Request"], TotalPhase=len(task.phases), TotalRequestPhase=task.phases[job_current_phase].duration, NonPreemptiveResource=self._schedule_current.loc[job, "NonPreemptiveResource"])])])
            

        self._schedule_current.loc[job, "Request"] -= 1
        self._schedule_current.loc[job, "Executed"] = True

        # Update schedule current with next task phase if task phase is completed
        if self._schedule_current.loc[job, "Request"] == 0:
            if (job_current_phase + 1) < len(task.phases):
                self._schedule_current.loc[job, "Phase"] = (job_current_phase + 1)
                self._schedule_current.loc[job, "Request"] = task.phases[(job_current_phase + 1)].duration
                self._schedule_current.loc[job, "NonPreemptiveResource"] = ""
            else:
                self._schedule_current.drop([job], inplace=True)
    
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


        available_resources = self._resourceset.resources.copy()

        # Schedule non-preemptive jobs/ressources
        jobs_selected_non_premptive = self._schedule_current[(self._schedule_current["NonPreemptiveResource"] != "") & (self._schedule_current["Request"] > 0) & (self._schedule_current["Executed"] == False)].index.tolist()

        for job_selected in jobs_selected_non_premptive:
            resources_names = self._schedule_current.loc[job_selected]["NonPreemptiveResource"].split(",")
            resources_names = [x.strip() for x in resources_names]
            resources = []
            for resource_name in resources_names:
                resource = self._resourceset.get_resource(resource_name)
                resources.append(resource)
            self._schedule_job_on_resources(resources, job_selected)
            for resource in resources:
                available_resources.remove(resource)
        
        # Try to schedule next jobs on resources
        jobs_sorted = self._sort_jobs_by_priority()
        for job in jobs_sorted:
            if (available_resources == []):
                break

            task_name = self._schedule_current.loc[job]["Task"]
            job_current_phase = self._schedule_current.loc[job]["Phase"]
            task = self._taskset.get_task(task_name)
            task_resource_types = []
            task_resource_types.append(task.phases[job_current_phase].ressource_type)
            if (self._memory_use_processor == True and task.phases[job_current_phase].ressource_type == ResourceType.Memory):
                task_resource_types.append(ResourceType.Processor)

            # Check if resource is available
            selected_resource = []
            task_resource_types_temp = task_resource_types.copy()
            for resource in available_resources:
                if (resource.type in task_resource_types_temp):
                    selected_resource.append(resource)
                    task_resource_types_temp.remove(resource.type)
                    if (task_resource_types_temp == []):
                        break

            # If all resources are available, schedule the job
            # Schedule job on resource
            if (task_resource_types_temp == []):
                self._schedule_job_on_resources(selected_resource, job)
                for ressource in selected_resource:
                    available_resources.remove(ressource)

                
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