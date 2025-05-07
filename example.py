import math
from pyrtsched_display import SchedulerDM
from pyrtsched_display import ScheduleDisplay

def lcm(a, b):
    """Calculate the Least Common Multiple of two numbers."""
    return abs(a * b) // math.gcd(a, b)

def calculate_hyperperiod(task_set):
    """Calculate the hyperperiod (LCM of all task periods) from the 'tasks' structure in datajson."""
    periods = [task.get("T") for task in task_set if "T" in task]
    if not periods:
        raise ValueError("No valid periods ('T') found in the task set.")
    hyperperiod = periods[0]
    for period in periods[1:]:
        hyperperiod = lcm(hyperperiod, period)
    return hyperperiod

datajson = {
    "max_time": 80,
    "scheduler": "DM",
    "premption_processor": True,
    "premption_memory": False,
    "memory_use_processor": "True",
    "nb_processors" : 1,
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
        {"Name": "T3", "O": 0, "C": 2, "D": 20, "T": 20},
    ],
}

datajson = {
    "max_time": 100,
    "scheduler": "DM",
    "premption_processor": True,
    "premption_memory": False,
    "memory_use_processor": "True",
    "nb_processors" : 1,
    "tasks": [
        {"Name": "T1", "O": 2, "R": 2, "E": 2, "W": 1, "D": 9, "T": 10},
        {"Name": "T2", "O": 5, "R": 1, "E": 1, "W": 1, "D": 4, "T": 11},
        {"Name": "T3", "O": 0, "C": 2, "D": 20, "T": 20}
    ],
}
do_graph = True
# Run the scheduler with stop conditions
stop_on_repeated_state = True
stop_on_missed_deadline = True


max_time = datajson["max_time"]
# Calculate the hyperperiod (LCM of all task periods)
task_set = datajson["tasks"]
hyperperiod = calculate_hyperperiod(task_set)
max_time = max_time if max_time > hyperperiod * 2 else hyperperiod * 2

print(f"Hyperperiod: {hyperperiod}, Max Time: {max_time}")
scheduler = SchedulerDM()
scheduler.configure_json(datajson)

scheduler.schedule(
    max_time=max_time,
    stop_on_repeated_state=stop_on_repeated_state,
    stop_on_missed_deadline=stop_on_missed_deadline
)

# Save the schedule to an Excel file
scheduler.schedule_result.to_excel("schedule.xlsx", index=False)

repeated_states = scheduler.repeated_states
if (repeated_states is not None) and (len(repeated_states) > 0):
    # Save the repeated states to an Excel file
    scheduler.export_repeated_states_to_excel("repeated_states.xlsx")
    print(f"Repeated states found: {len(repeated_states)}")
    # Display the first pair of repeated states
    first_repeated_state = repeated_states[0]
    print(f"First repeated state occurs at time: {first_repeated_state['PreviousTime']} and {first_repeated_state['CurrentTime']}")

# Prepare the schedule for display
if (do_graph):
    graph = ScheduleDisplay(max_time=max_time, render="browser")
    graph.update_from_scheduler(scheduler)
    # Display the schedule on a web browser using Plotly
    graph.fig.show()

# Save the schedule to html file
#graph.fig_save("schedule.html")

# L'exemple ne fonctionne pas, la tâche 2 n'a pas encore été planifiée à l'instant 2. Il faut vraiment stocker Omega qui est l'horloge de chaque tâche non définie avant O_i.