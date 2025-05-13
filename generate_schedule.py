from pyrtsched_display import SchedulerDM, SchedulerEDF, Scheduler
import json
import random
import math
import logging

# Configuration du logger
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(name)s - %(message)s")

def generate_task_set(num_tasks_max, max_period, max_total_utilization=0.9, max_task_utilization=0.8):
    """
    Generate a set of tasks with constraints on utilization.

    Parameters:
    - num_tasks_max (int): Maximum number of tasks to generate.
    - max_period (int): Maximum period for tasks.
    - max_total_utilization (float): Maximum total utilization for all tasks.
    - max_task_utilization (float): Maximum utilization for a single task.

    Returns:
    - list: A list of generated tasks.
    """
    tasks = []
    total_utilization = 0.0
    max_attempts = 100  # Maximum attempts to generate a valid task

    for i in range(num_tasks_max):
        attempts = 0
        while attempts < max_attempts:
            task = {
                "Name": f"T{i+1}",
                #"O": random.randint(0, max_period // 2),
                "O": 0,
                "R": random.randint(1, 3),
                "E": random.randint(1, max_period // 6),
                "W": random.randint(1, 3),
                "D": random.randint(1, max_period),
                "T": random.randint(1, max_period)
            }
            # Ensure T >= R + E + W
            if task["T"] < (task["R"] + task["E"] + task["W"]):
                logger.debug(f"Adjust period for task {task['Name']}: T={task['T']}, R={task['R']}, E={task['E']}, W={task['W']}")
                task["T"] = random.randint(task["R"] + task["E"] + task["W"], max_period)
            # Ensure D <= T
            if task["D"] > task["T"] or task["D"] < (task["R"] + task["E"] + task["W"]):
                logger.debug(f"Adjust deadline for task {task['Name']}: D={task['D']}, T={task['T']}, R={task['R']}, E={task['E']}, W={task['W']}")
                task["D"] = random.randint(max(1, task["R"] + task["E"] + task["W"]), task["T"])

            
            # Calculate task utilization
            utilization = (task["R"] + task["E"] + task["W"]) / task["T"]
            
            # Ensure task utilization <= max_task_utilization and total utilization <= max_total_utilization
            if utilization <= max_task_utilization and (total_utilization + utilization) <= max_total_utilization:
                tasks.append(task)
                total_utilization += utilization
                break
            attempts += 1
        
        # If no valid task could be generated after max_attempts, stop adding tasks
        if attempts == max_attempts:
            logger.warning(f"Could not generate a valid task after {max_attempts} attempts for task {i+1}.")
            break

    # Ensure at least 2 tasks are generated
    if len(tasks) < 2:
        logger.warning("Could not generate at least 2 valid tasks with the given constraints.")
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

def create_schedule(num_tasks_max, max_period, hyperperiods=2, max_task_utilization=0.8):
    """
    Create a schedule configuration.

    Parameters:
    - num_tasks_max (int): Maximum number of tasks to generate.
    - max_period (int): Maximum period for tasks.
    - hyperperiods (int): Number of hyperperiods to consider for max_time.
    - max_task_utilization (float): Maximum utilization for a single task.

    Returns:
    - dict: A dictionary containing the schedule configuration.
    """
    task_set = generate_task_set(num_tasks_max, max_period, max_task_utilization=max_task_utilization)
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
    max_task_utilization = 0.8  # Par défaut, 0.8

    try:
        schedule = create_schedule(num_tasks_max, max_period, max_task_utilization=max_task_utilization)
        with open("generated_schedule.json", "w") as f:
            json.dump(schedule, f, indent=4)
        logger.info("Schedule generated and saved to generated_schedule.json")
    except Exception as e:
        logger.error(f"An error occurred: {e}")

if __name__ == "__main__":
    main()