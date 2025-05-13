import math

import pandas as pd
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
        {"Name": "T2", "O": 5, "R": 1, "E": 2, "W": 1, "D": 15, "T": 10},
        {"Name": "T3", "O": 0, "C": 1, "D": 30, "T": 30}
    ],
}

datajson = {
    "max_time": 30,
    "scheduler": "DM",
    "premption_processor": True,
    "premption_memory": False,
    "memory_use_processor": "True",
    "nb_processors" : 1,
    "tasks": [
        {"Name": "T1", "O": 4, "R": 2, "E": 2, "W": 1, "D": 9, "T": 10},
        {"Name": "T2", "O": 3, "R": 2, "E": 2, "W": 1, "D": 10, "T": 10}
    ],
}

do_graph = True
# Run the scheduler with stop conditions
stop_on_repeated_state = False
stop_on_missed_deadline = True


max_time = datajson["max_time"]
# Calculate the hyperperiod (LCM of all task periods)
task_set = datajson["tasks"]
hyperperiod = calculate_hyperperiod(task_set)
max_offset = max(task.get("O", 0) for task in task_set)
max_time = max_time if max_time > hyperperiod * 2 + max_offset else hyperperiod * 2 + max_offset

print(f"Hyperperiod + MaxO: {hyperperiod + max_offset}, Max Time: {max_time}")
scheduler = SchedulerDM()
scheduler.configure_json(datajson)

scheduler.schedule(
    max_time=max_time,
    stop_on_repeated_state=stop_on_repeated_state,
    stop_on_missed_deadline=stop_on_missed_deadline
)

# Save the schedule to an Excel file
scheduler.schedule_result.to_excel("schedule.xlsx", index=False)
scheduler.export_configuration_to_json("configuration.json")

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

# Iterate through the previous states and display the required information
if scheduler._previous_states:
    """for state, time in scheduler._previous_states:
        print(f"Time: {time}")
        #print(state)
        # Display tasks in alphabetical order
        tasks = state[state["Type"] == "Task"].sort_values(by="Name")
        for _, row in tasks.iterrows():
            #print(f"{row['Name']}: Clock = {row['Clock']}, Remaining = {row['Remaining']}")
            print(f"{time}_Clo_{row['Name']} = {row['Clock']}\n{time}_Rem_{row['Name']} = {row['Remaining']}")

        # Display processors in alphabetical order
        processors = state[state["Type"] == "Processor"].sort_values(by="Name")
        for _, row in processors.iterrows():
            #print(f"{row['Name']}: TaskScheduled = {row['TaskScheduled']}, RemainingMem = {row['RemainingMem']}")
            print(f"{time}_Sch_{row['Name']} = {row['TaskScheduled']}\n{time}_RemMem_{row['Name']} = {row['RemainingMem']}")
    """
    # Create a DataFrame to store the information
    columns = ["Time"] + [f"{t}" for t in range(max_time)]
    data = []

    # Add task information
    for task in task_set:
        task_name = task["Name"]
        data.append([f"{task_name}_Clo"] + [None] * max_time)
        data.append([f"{task_name}_Rem"] + [None] * max_time)

    # Add processor information
    
    for processor in scheduler.resourceset.get_resourceset_as_dataframe()["Name"].unique():
        data.append([f"{processor}_Sch"] + [None] * max_time)
        data.append([f"{processor}_RemMem"] + [None] * max_time)

    df = pd.DataFrame(data, columns=columns)

    # Populate the DataFrame with the state information
    for state, time in scheduler._previous_states:
        # Update task information
        tasks = state[state["Type"] == "Task"].sort_values(by="Name")
        for _, row in tasks.iterrows():
            df.loc[df["Time"] == f"{row['Name']}_Clo", f"{time}"] = row["Clock"]
            df.loc[df["Time"] == f"{row['Name']}_Rem", f"{time}"] = row["Remaining"]

        # Update processor information
        processors = state[state["Type"] == "Processor"].sort_values(by="Name")
        for _, row in processors.iterrows():
            df.loc[df["Time"] == f"{row['Name']}_Sch", f"{time}"] = row["TaskScheduled"]
            df.loc[df["Time"] == f"{row['Name']}_RemMem", f"{time}"] = row["RemainingMem"]

    # Save the DataFrame to an Excel file
    df.to_excel("schedule_states.xlsx", index=False)
    print(df)

#scheduler_read = SchedulerDM()
#scheduler_read.load_from_files("configuration.json", "schedule.xlsx")
#graph = ScheduleDisplay(max_time=max_time, render="browser")
#graph.update_from_scheduler(scheduler_read)
# Display the schedule on a web browser using Plotly
#graph.fig.show()

# Save the schedule to html file
#graph.fig_save("schedule.html")

# L'exemple ne fonctionne pas, la tâche 2 n'a pas encore été planifiée à l'instant 2. Il faut vraiment stocker Omega qui est l'horloge de chaque tâche non définie avant O_i.