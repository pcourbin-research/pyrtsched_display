from pyrtsched_display import SchedulerDM, SchedulerEDF, Scheduler
import json
import random
import math

def generate_task_set(num_tasks_max, max_period, max_total_utilization=0.9):
    tasks = []
    total_utilization = 0.0
    max_attempts = 100  # Maximum attempts to generate a valid task

    for i in range(num_tasks_max):
        attempts = 0
        while attempts < max_attempts:
            task = {
                "Name": f"T{i+1}",
                "O": random.randint(0, max_period // 2),
                "R": random.randint(1, 5),
                "E": random.randint(1, max_period // 2),
                "W": random.randint(1, 5),
                "D": random.randint(1, max_period),
                "T": random.randint(1, max_period)
            }
            # Ensure D <= T
            if task["D"] > task["T"]:
                task["D"] = random.randint(1, task["T"])
            
            # Calculate task utilization
            utilization = (task["R"] + task["E"] + task["W"]) / task["T"]
            
            # Ensure task utilization <= 0.8 and total utilization <= max_total_utilization
            if utilization <= 0.8 and (total_utilization + utilization) <= max_total_utilization:
                tasks.append(task)
                total_utilization += utilization
                break
            attempts += 1
        
        # If no valid task could be generated after max_attempts, stop adding tasks
        if attempts == max_attempts:
            print(f"Could not generate a valid task after {max_attempts} attempts for task {i+1}.")
            break

    # Ensure at least 2 tasks are generated
    if len(tasks) < 2:
        raise ValueError("Could not generate at least 2 valid tasks with the given constraints.")
    
    return tasks

def lcm(a, b):
    """Calculate the Least Common Multiple of two numbers."""
    return abs(a * b) // math.gcd(a, b)

def calculate_hyperperiod(task_set):
    """Calculate the hyperperiod (LCM of all task periods)."""
    periods = [task["T"] for task in task_set]
    hyperperiod = periods[0]
    for period in periods[1:]:
        hyperperiod = lcm(hyperperiod, period)
    return hyperperiod

def create_schedule(num_tasks_max, max_period, hyperperiods=2):
    task_set = generate_task_set(num_tasks_max, max_period)
    hyperperiod = calculate_hyperperiod(task_set)
    schedule_data = {
        "max_time": hyperperiod * hyperperiods,
        "scheduler": "DM",
        "memory_use_processor": "True",
        "premption_processor": "True",
        "premption_memory": "True",
        "nb_processors": 1,
        "tasks": task_set
    }
    return schedule_data

def main():
    num_tasks_max = 5
    max_period = 20
    schedule = create_schedule(num_tasks_max, max_period)
    
    with open("generated_schedule.json", "w") as f:
        json.dump(schedule, f, indent=4)
    
    print("Schedule generated and saved to generated_schedule.json")

if __name__ == "__main__":
    main()