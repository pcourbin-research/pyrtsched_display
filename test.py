from taskset import TaskSet
from resourceset import ResourceSet
from scheduler import Scheduler
from schedule_display import ScheduleDisplay

datajson = {
    "tasks": [
        {
            "Name": "T1",
            "O": 0,
            "Phases": [
                {"Type": "Memory", "Duration": 2, "Premption": False},
                {"Type": "Processor", "Duration": 4, "Premption": True},
                {"Type": "Memory", "Duration": 1, "Premption": True},
            ],
            "D": 10,
            "T": 10,
        },

        {"Name": "T2", "O": 1, "R": 1, "E": 1, "W": 1, "D": 4, "T": 9},
        {"Name": "T3", "O": 0, "C": 2, "D": 5, "T": 6},
    ],
    "resources": [
        {"Name": "P1", "Type": "Processor"},
        {"Name": "M1", "Type": "Memory"},
    ],
}


tasks = TaskSet(datajson["tasks"])
resources = ResourceSet(datajson["resources"])

# print(tasks)
# print(resources)
max_time = 40
scheduler = Scheduler(tasks, resources, premption_processor=True, premption_memory=True)
scheduler.schedule(max_time)
schedule = scheduler.schedule_result
#print(schedule)

graph = ScheduleDisplay(max_time=max_time, render="browser")
graph.update(
    tasks.get_taskset_as_dataframe(), resources.get_resourceset_as_dataframe(), schedule
)
graph.fig.show()
