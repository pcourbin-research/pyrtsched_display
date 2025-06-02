import os
import json
import logging
from pyrtsched_display import SchedulerDM
from generate_schedule import generate_task_set
from pyrtsched_display import ScheduleDisplay
from math import lcm
from tqdm import tqdm  # Import tqdm pour la barre de progression

# Configurer le logger
logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,  # Niveau de log par défaut
    format="%(asctime)s - %(levelname)s - %(name)s - %(message)s"
)

# Configurer le logger de generate_schedule
logging.getLogger("generate_schedule").setLevel(logging.ERROR)  # Changez le niveau ici (DEBUG, INFO, WARNING, ERROR)
logging.getLogger("pyrtsched_display.scheduler").setLevel(logging.ERROR)  # Changez le niveau ici (DEBUG, INFO, WARNING, ERROR)

def calculate_hyperperiod(task_set):
    """Calculate the hyperperiod (LCM of all task periods)."""
    periods = [task["T"] for task in task_set]
    hyperperiod = periods[0]
    for period in periods[1:]:
        hyperperiod = lcm(hyperperiod, period)
    return hyperperiod

def test_schedules(output_dir, num_task_sets, num_tasks, max_period, max_total_utilization=0.9, max_task_utilization=0.8):
    """Test generated task sets and save results."""
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    tested_task_sets = []
    deadlines_missed = 0
    no_repetition_found = 0
    generation_failed = 0  # Compteur pour les échecs de génération

    try:
        # Utiliser tqdm pour afficher une barre de progression
        with tqdm(total=num_task_sets, desc="Processing task sets", unit="set") as pbar:
            for i in range(num_task_sets):
                # Generate a task set
                logger.debug(f"\n\n[Task set {i + 1}] Generating task set...")
                try:
                    task_set = generate_task_set(num_tasks, max_period, max_total_utilization, max_task_utilization)
                except ValueError as e:
                    logger.debug(f"[Task set {i + 1}] Error generating task set: {e}")
                    generation_failed += 1  # Incrémenter le compteur en cas d'échec
                    pbar.update(1)  # Mettre à jour la barre de progression
                    continue
                max_offset = max(task["O"] for task in task_set)
                hyperperiod = calculate_hyperperiod(task_set)
                max_time = 2 * hyperperiod + max_offset

                # Mettre à jour la barre de progression
                pbar.set_description("Missed: %d / NoRepetition: %d / Failed: %d / Repetition: %d - %d" % (deadlines_missed, no_repetition_found, generation_failed, i - deadlines_missed - no_repetition_found - generation_failed, max_time))

                # Prepare the scheduler
                scheduler = SchedulerDM()
                datajson = {
                    "max_time": max_time,
                    "scheduler": "DM",
                    "premption_processor": True,
                    "premption_memory": False,
                    "memory_use_processor": False,
                    "nb_processors": 1,
                    "tasks": task_set,
                    "deadline_missed": None,  # Placeholder for missed deadline info
                    "repeated_state": None,  # Placeholder for repeated state info
                }
                scheduler.configure_json(datajson)

                # Run the scheduler
                logger.debug(f"[Task set {i + 1}] Running scheduler for task set {i + 1} with {len(task_set)} tasks and max_time {max_time}...")
                scheduler.schedule(max_time=max_time, stop_on_repeated_state=True, stop_on_missed_deadline=True)
                logger.debug(f"[Task set {i + 1}] Scheduler run completed.")

                # Check for missed deadlines
                missed_deadlines = scheduler.schedule_result[(scheduler.schedule_result["Missed"].notna()) &(scheduler.schedule_result["Missed"] != "")]
                if not missed_deadlines.empty:
                    deadlines_missed += 1
                    first_missed = missed_deadlines.iloc[0]
                    datajson["deadline_missed"] = {
                        "time": int(first_missed["Finish"]),
                        "task": first_missed["Task"],
                        "job": first_missed["Job"]
                    }
                    logger.debug(f"[Task set {i + 1}] Deadline missed: Task {first_missed['Task']} at time {first_missed['Finish']}.")

                # Check for repeated states
                if len(scheduler.repeated_states) > 0:
                    first_repeated = scheduler.repeated_states[0]
                    datajson["repeated_state"] = {
                        "start_time": first_repeated["PreviousTime"],
                        "end_time": first_repeated["CurrentTime"]
                    }
                    logger.debug(f"[Task set {i + 1}] Repeated state found between {first_repeated['PreviousTime']} and {first_repeated['CurrentTime']}.")
                elif missed_deadlines.empty:
                    no_repetition_found += 1

                    # Save the task set and schedule
                    task_set_filename = os.path.join(output_dir, f"{i + 1}_task_set.json")
                    schedule_filename = os.path.join(output_dir, f"{i + 1}_schedule.xlsx")
                    with open(task_set_filename, "w") as f:
                        json.dump(datajson, f, indent=4)
                    scheduler.schedule_result.to_excel(schedule_filename, index=False)
                    logger.debug(f"[Task set {i + 1}] Task set and schedule saved: {task_set_filename}, {schedule_filename}.")

                # Save the tested task set
                tested_task_sets.append(datajson)

                # Mettre à jour la barre de progression
                pbar.update(1)

    except KeyboardInterrupt:
        logger.warning("CTRL+C detected! Saving tested task sets before exiting...")

    finally:
        # Save all tested task sets
        logger.debug("Saving tested task sets...")
        tested_task_sets_filename = os.path.join(output_dir, "tested_task_sets.json")
        with open(tested_task_sets_filename, "w") as f:
            json.dump(tested_task_sets, f, indent=4)

        # Print summary
        logger.info(f"Number of task sets tested: {len(tested_task_sets)}")
        logger.info(f"Number of task sets with missed deadlines: {deadlines_missed}")
        logger.info(f"Number of usable task sets (no repetition found): {no_repetition_found}")
        logger.info(f"Number of task sets generation failed: {generation_failed}")

if __name__ == "__main__":
    # Parameters
    output_directory = "results"
    number_of_task_sets = 1000  # Number of task sets to generate and test
    number_of_tasks = 5  # Number of tasks per task set
    max_task_period = 40  # Maximum period for tasks
    max_total_utilization = 0.8  # Maximum total utilization for all tasks
    max_task_utilization = 0.4  # Maximum utilization for a single task
    
    logging.getLogger().setLevel(logging.INFO)
    test_schedules(output_directory, number_of_task_sets, number_of_tasks, max_task_period, max_total_utilization, max_task_utilization)