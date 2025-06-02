import math
import pandas as pd
import logging  # Import logging
from abc import ABC, abstractmethod
from typing import Optional
from . import ResourceType, Resource, TaskSet, ResourceSet
import json

# Configurer le logger
logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,  # Niveau de log par défaut
    format="%(asctime)s - %(levelname)s - %(name)s - %(message)s"
)

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
    _previous_states = []  # List to store previous states
    _repeated_states = []  # List to store repeated states

    def __init__(self):
        self._taskset = None
        self._resourceset = None
        self._premption[ResourceType.Processor] = True
        self._premption[ResourceType.Memory] = True
        self._memory_use_processor = False

        schema_schedule_current={'Job': 'string', 'Task': 'string', 'Activation': 'int', 'Phase': 'int', 'Request': 'int', 'Executed': 'bool', 'NonPreemptiveResource': 'string', 'AbsoluteDeadline': 'int'}
        self._schedule_current = pd.DataFrame(columns=list(schema_schedule_current.keys())).astype(schema_schedule_current)
        self._schedule_current.set_index('Job', inplace=True)

        schema_schedule_result={'Task': 'string', 'Job': 'string', 'Start': 'int', 'Finish': 'int', 'Resource': 'string', 'Missed': 'string', 'Phase': 'int', 'RequestPhaseRemaining': 'int', 'TotalPhase': 'int', 'TotalRequestPhase': 'int'}
        self._schedule_result = pd.DataFrame(columns=list(schema_schedule_result.keys())).astype(schema_schedule_result)
        #self._previous_states = pd.DataFrame(columns=["Clock", "Remaining", "TaskScheduled", "RemainingMem"])
        self._previous_states = []  # List to store previous states
        self._repeated_states = []

    
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

    @property
    def repeated_states(self):
        return self._repeated_states
    
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
        assert self._schedule_current is not None, "Schedule current is not set. Cannot sort jobs by priority."
        jobs_to_execute = self._schedule_current[(self._schedule_current["Request"] > 0) & (self._schedule_current["Executed"] == False)].index.tolist()
        # Call abstract method _job_priority, according to the scheduler algorithm
        jobs_sorted = sorted(jobs_to_execute, key=lambda job_name: self._job_priority(job_name))
        return jobs_sorted  
    
    def _schedule_job_on_resources(self, resources: list[Resource], job: str):
        assert self._schedule_current is not None, "Schedule current is not set. Cannot schedule job on resources."
        assert self._taskset is not None, "TaskSet is not set. Cannot schedule job on resources."
        task_name = self._schedule_current.loc[job]["Task"]
        job_current_phase = self._schedule_current.loc[job]["Phase"]
        if isinstance(job_current_phase, pd.Series):
            job_current_phase = job_current_phase.iloc[0]
        task = self._taskset.get_task(str(task_name))
        task_resource_type = task.phases[int(job_current_phase)].ressource_type 

        self._schedule_current.loc[job, "NonPreemptiveResource"] = ""
        for resource in resources:
            assert(task_resource_type == resource.type or (task_resource_type == ResourceType.Memory and resource.type == ResourceType.Processor and self._memory_use_processor == True)), f"Task {task_name} phase {job_current_phase} resource type {task_resource_type} is not compatible with resource {resource.name} type {resource.type} and memory_use_processor is {self._memory_use_processor}."
            
            if (task.phases[job_current_phase].premption == False or (self._premption[task.phases[job_current_phase].ressource_type] == False)):
                if (self._schedule_current.loc[job, "NonPreemptiveResource"] == ""):
                    self._schedule_current.loc[job, "NonPreemptiveResource"] = resource.name
                else:
                    self._schedule_current.loc[job, "NonPreemptiveResource"] = str(self._schedule_current.loc[job, "NonPreemptiveResource"]) + ", " + str(resource.name)

            # Save schedule result
            self._schedule_result = pd.concat([self._schedule_result, pd.DataFrame([dict(Task=task_name, Job=job, Start=self._current_time, Finish=self._current_time+1, Resource=resource.name, Missed="", Phase=job_current_phase+1, RequestPhaseRemaining=self._schedule_current.loc[job, "Request"], TotalPhase=len(task.phases), TotalRequestPhase=task.phases[job_current_phase].duration, NonPreemptiveResource=self._schedule_current.loc[job, "NonPreemptiveResource"])])])
        
        self._schedule_current.loc[job, "Request"] -= 1 # type: ignore
        self._schedule_current.loc[job, "Executed"] = True

        # Update schedule current with next task phase if task phase is completed
        if self._schedule_current.loc[job, "Request"] == 0:
            if (job_current_phase + 1) < len(task.phases):
                self._schedule_current.loc[job, "Phase"] = (job_current_phase + 1)
                self._schedule_current.loc[job, "Request"] = task.phases[(job_current_phase + 1)].duration
                self._schedule_current.loc[job, "NonPreemptiveResource"] = ""
            #else:
            #    self._schedule_current.drop([job], inplace=True)
    
    def _restart_schedule(self):
        self._schedule_result = self._schedule_result.iloc[0:0] # type: ignore
        self._schedule_current = self._schedule_current.iloc[0:0] # type: ignore
        self._current_time = 0

    def _capture_current_state(self):
        """Capture the current state of tasks and processors."""
        # Define the schema for the DataFrame with explicit types
        schema = {
            "Type": "string",  # "Task" or "Processor"
            "Name": "string",  # Name of the task or processor
            "Clock": "float",  # Use float to allow -1 and NaN
            "Remaining": "float",  # Use float to allow NaN
            "TaskScheduled": "string",  # Use string for task names
            "RemainingMem": "float",  # Use float to allow NaN
        }
        current_state = pd.DataFrame(columns=list(schema.keys())).astype(schema)

        # Capture task states
        assert self._taskset is not None, "TaskSet is not set. Cannot capture task states."
        for task in self._taskset.tasks:
            first_activation = task.first_activation
            period = task.period
            last_activation = first_activation + ((self._current_time - first_activation) // period) * period
            if (self._current_time >= first_activation):
                clock = self._current_time - last_activation
            else:
                clock = -1
            
            job_rows = self._schedule_current[self._schedule_current["Task"] == task.name] # type: ignore
            if job_rows.empty:
                # Task has not been activated yet or all jobs are finished
                remaining = 0
            else:
                # Task is active
                current_phase = job_rows["Phase"].iloc[0]
                total_execution_time = sum(phase.duration for phase in task.phases)
                executed_time = sum(task.phases[phase_index].duration for phase_index in range(current_phase))
                remaining = total_execution_time - executed_time - task.phases[current_phase].duration + job_rows["Request"].sum()

            current_state = pd.concat([
                current_state,
                pd.DataFrame([{
                    "Type": "Task",
                    "Name": task.name,
                    "Clock": clock,
                    "Remaining": remaining,
                    "TaskScheduled": "",
                    "RemainingMem": ""
                }])
            ], ignore_index=True)

        # Capture processor states
        assert self._resourceset is not None, "ResourceSet is not set. Cannot capture processor states."
        assert self._schedule_result is not None, "Schedule result is not set. Cannot capture processor states."
        assert self._schedule_current is not None, "Schedule current is not set. Cannot capture processor states."
        for processor in [res for res in self._resourceset.resources if res.type == ResourceType.Processor]:
            task_scheduled = ""
            remaining_mem = ""

            # Check if a task is scheduled on this processor
            scheduled_jobs = self._schedule_result[
                (self._schedule_result["Resource"] == processor.name) &
                (self._schedule_result["Finish"] == self._current_time)
            ]
            if not scheduled_jobs.empty:
                task_scheduled = scheduled_jobs["Task"].iloc[0]
                job_scheduled = scheduled_jobs["Job"].iloc[0]
                task_name = self._schedule_current.loc[job_scheduled]["Task"]
                job_current_phase = self._schedule_current.loc[job_scheduled]["Phase"]
                task = self._taskset.get_task(str(task_name))
                if task.phases[job_current_phase].ressource_type == ResourceType.Memory:
                    remaining_mem = self._schedule_current.loc[job_scheduled, "Request"]
            #if (task_scheduled is not None) or (remaining_mem is not None):
            new_current_state = pd.DataFrame([{
                "Type": "Processor",
                "Name": processor.name,
                "Clock": math.nan,
                "Remaining": math.nan,
                "TaskScheduled": task_scheduled,
                "RemainingMem": remaining_mem
            }])
            current_state = pd.concat([
                current_state, new_current_state
            ], ignore_index=True)

        return current_state
    
    def _is_repeated_state(self, state: Optional[pd.DataFrame] = None) -> bool:
        """Check if the current state matches any previous state."""
        # Ensure the state is valid for comparison
        if state is None or state.empty:
            return False

        # Check if any "Clock" value is -1 in the current state
        if (state["Clock"] == -1).any():
            return False

        # Compare the current state with previous states
        for previous_state, previous_time in self._previous_states:
            # Ensure the previous state is valid for comparison
            if (previous_state["Clock"] == -1).any():
                continue

            # Compare states without considering the index
            if state.equals(previous_state): # or (state.empty and previous_state.empty):
                self._repeated_states.append({
                    "PreviousTime": previous_time,
                    "CurrentTime": self._current_time,
                    "PreviousState": previous_state.copy(),
                    "CurrentState": state.copy()
                })
                return True

        return False
    
    def export_repeated_states_to_excel(self, filename="repeated_states.xlsx"):
        """Export repeated states to an Excel file."""
        if not self._repeated_states:
            logger.info("No repeated states to export.")
            return

        with pd.ExcelWriter(filename) as writer:
            for repeated_state in self._repeated_states:
                sheet_name = f"{repeated_state['PreviousTime']}_{repeated_state['CurrentTime']}"
                # Combine CurrentState and PreviousState side by side
                combined_state = pd.concat(
                    [repeated_state["PreviousState"].add_prefix(f"{repeated_state['PreviousTime']}"+"_"), 
                    repeated_state["CurrentState"].add_prefix(f"{repeated_state['CurrentTime']}"+"_")], 
                    axis=1
                )
                combined_state.to_excel(writer, sheet_name=sheet_name, index=False)
        logger.info(f"Repeated states exported to {filename}")

    def export_configuration_to_json(self, filename="configuration.json"):
        """
        Export the current configuration to a JSON file.
        
        Parameters:
        - filename (str): The name of the file to save the configuration.
        """
        assert self._taskset is not None, "TaskSet is not set. Cannot export configuration."
        assert self._resourceset is not None, "ResourceSet is not set. Cannot export configuration."
        configuration = {
            "tasks": [task.to_dict() for task in self._taskset.tasks],
            "nb_processors": len(self._resourceset.resources),
            "premption_processor": self._premption[ResourceType.Processor],
            "premption_memory": self._premption[ResourceType.Memory],
            "memory_use_processor": self._memory_use_processor,
        }

        with open(filename, "w") as json_file:
            json.dump(configuration, json_file, indent=4)
        logger.info(f"Configuration exported to {filename}")

    def load_from_files(self, json_filename: str, excel_filename: str):
        """
        Reload the scheduler from a JSON configuration file and an Excel schedule file.
        
        Parameters:
        - json_filename (str): The name of the JSON file containing the configuration.
        - excel_filename (str): The name of the Excel file containing the schedule results.
        """
        # Load configuration from JSON
        with open(json_filename, "r") as json_file:
            configuration = json.load(json_file)
        
        # Reconfigure the scheduler
        self.configure_json(configuration)
        logger.info(f"Configuration loaded from {json_filename}")

        # Load schedule results from Excel
        self._schedule_result = pd.read_excel(excel_filename)
        logger.info(f"Schedule results loaded from {excel_filename}")

        # Reset the current time to the maximum time in the schedule results
        if not self._schedule_result.empty:
            self._current_time = self._schedule_result["Finish"].max()
        else:
            self._current_time = 0

        logger.info(f"Scheduler reloaded. Current time set to {self._current_time}.")

    def _schedule_next(self):
        assert self._taskset is not None, "TaskSet is not set. Cannot schedule tasks."
        assert self._resourceset is not None, "ResourceSet is not set. Cannot schedule resources."
        assert self._schedule_current is not None, "Schedule current is not set. Cannot schedule tasks."
        self._schedule_current["Executed"] = False

        # Update schedule current with new task activations
        for task in self._taskset.tasks:
            if (self._current_time - task.first_activation) % task.period == 0:
                absolute_deadline = self._current_time + task.deadline
                self._schedule_current = pd.concat([
                    self._schedule_current,
                    pd.DataFrame([dict(
                        Task=task.name,
                        Activation=self._current_time,
                        Phase=0,
                        Request=task.phases[0].duration,
                        Executed=False,
                        NonPreemptiveResource="",
                        AbsoluteDeadline=absolute_deadline  # Store the absolute deadline
                    )], index=[task.name + "_" + str(self._current_time)])
                ])

        # Check for repeated state before scheduling
        current_state = self._capture_current_state()
        self._is_repeated_state(current_state)

        # List jobs where Request is 0 and current_time equals the last activation plus the task period
        jobs_to_remove = []
        for job in self._schedule_current.index:
            task = self._taskset.get_task(self._schedule_current.loc[job]["Task"])
            last_activation = self._schedule_current.loc[job]["Activation"]
            if self._schedule_current.loc[job]["Request"] == 0 and self._current_time >= last_activation + task.period:
                jobs_to_remove.append(job)

        # Process the listed jobs (if needed, you can add specific logic here)
        for job in jobs_to_remove:
            self._schedule_current.drop([job], inplace=True)

        # Save the current state and time to the history
        self._previous_states.append((current_state, self._current_time))

        available_resources = self._resourceset.resources.copy()

        # Schedule non-preemptive jobs/resources
        jobs_selected_non_premptive = self._schedule_current[
            (self._schedule_current["NonPreemptiveResource"] != "") &
            (self._schedule_current["Request"] > 0) &
            (self._schedule_current["Executed"] == False)
        ].index.tolist()

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
            if isinstance(job_current_phase, pd.Series):
                job_current_phase = job_current_phase.iloc[0]
            task = self._taskset.get_task(str(task_name))
            task_resource_types = [task.phases[job_current_phase].ressource_type]
            if self._memory_use_processor and task.phases[job_current_phase].ressource_type == ResourceType.Memory:
                task_resource_types.append(ResourceType.Processor)

            # Check if resource is available
            selected_resource = []
            task_resource_types_temp = task_resource_types.copy()
            for resource in available_resources:
                if resource.type in task_resource_types_temp:
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
            if self._current_time + 1 >= self._schedule_current.loc[job, "AbsoluteDeadline"] and self._schedule_current.loc[job, "Request"] > 0: # type: ignore
                task = self._taskset.get_task(self._schedule_current.loc[job]["Task"])
                job_current_phase = self._schedule_current.loc[job]["Phase"]
                self._schedule_result = pd.concat([
                    self._schedule_result,
                    pd.DataFrame([dict(
                        Task=task.name,
                        Job=job,
                        Start=self._current_time + 1,
                        Finish=self._current_time + 1,
                        Resource="",
                        Missed="Missed",
                        Phase=job_current_phase + 1,
                        RequestPhaseRemaining=self._schedule_current.loc[job, "Request"],
                        TotalPhase=len(task.phases),
                        TotalRequestPhase=task.phases[job_current_phase].duration,
                        NonPreemptiveResource=self._schedule_current.loc[job, "NonPreemptiveResource"]
                    )])
                ])

        self._current_time += 1
    
    def schedule(self, max_time: int = 40, stop_on_repeated_state: bool = False, stop_on_missed_deadline: bool = False):
        """
        Run the scheduling process up to max_time with optional stop conditions.
        
        Parameters:
        - max_time (int): Maximum time to run the scheduler.
        - stop_on_repeated_state (bool): Stop scheduling if a repeated state is detected.
        - stop_on_missed_deadline (bool): Stop scheduling if a missed deadline is detected.
        """
        assert self._schedule_current is not None, "Schedule current is not set. Cannot schedule tasks."
        self._restart_schedule()

        for t in range(max_time):
            self._current_time = t

            # Check for repeated state
            if stop_on_repeated_state and len(self.repeated_states) > 0:
                first_repeated_state = self.repeated_states[0]
                logger.warning(
                    f"Stopping scheduling at time {self._current_time} due to repeated state. "
                    f"First repeated state occurred between time {first_repeated_state['PreviousTime']} and {first_repeated_state['CurrentTime']}."
                )
                break

            # Check for missed deadlines
            missed_deadlines = self._schedule_current[
                (self._schedule_current["Request"] > 0) & 
                (self._current_time >= self._schedule_current["AbsoluteDeadline"])
            ]
            if stop_on_missed_deadline and not missed_deadlines.empty:
                missed_tasks = missed_deadlines["Task"].unique()
                logger.warning(
                    f"Stopping scheduling at time {self._current_time} due to missed deadline by tasks: {', '.join(missed_tasks)}."
                )
                break

            # Perform scheduling for the current time step
            self._schedule_next()