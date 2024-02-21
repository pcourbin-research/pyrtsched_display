from pyrtsched_display import SchedulerDM
from pyrtsched_display import ScheduleDisplay

datajson = {
    "max_time": 80,
    "premption_processor": True,
    "premption_memory": True,
    "tasks": [
        {
            "Name": "T1",
            "O": 0,
            "Phases": [
                {"Type": "Memory", "Duration": 2, "Premption": False},
                {"Type": "Processor", "Duration": 3, "Premption": True},
                {"Type": "Memory", "Duration": 1, "Premption": False},
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

max_time = datajson["max_time"]

scheduler = SchedulerDM()
scheduler.configure_json(datajson)
scheduler.schedule(max_time)

# Save the schedule to an Excel file
scheduler.schedule_result.to_excel("schedule.xlsx", index=False)

# Prepare the schedule for display
graph = ScheduleDisplay(max_time=max_time, render="browser")
graph.update_from_scheduler(scheduler)

# Display the schedule on a web browser using Plotly
graph.fig.show()

# Save the schedule to html file
#graph.fig_save("schedule.html")
