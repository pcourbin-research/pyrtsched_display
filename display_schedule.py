import json
import sys
import os
import pandas as pd
from pyrtsched_display import SchedulerDM
from pyrtsched_display import ScheduleDisplay

def load_task_set(json_filename, config_index=None):
    """
    Charge une configuration de tâches à partir d'un fichier JSON.
    
    Parameters:
    - json_filename (str): Chemin du fichier JSON contenant les configurations.
    - config_index (int): Index de la configuration à charger (None si une seule configuration).

    Returns:
    - dict: La configuration de tâches chargée.
    """
    with open(json_filename, "r") as f:
        task_sets = json.load(f)

    if isinstance(task_sets, list):
        if config_index is None:
            if len(task_sets) == 1:
                return task_sets[0]
            else:
                raise ValueError(f"Le fichier JSON contient plusieurs configurations. Spécifiez un index entre 0 et {len(task_sets) - 1}.")
        else:
            if 0 <= config_index < len(task_sets):
                return task_sets[config_index]
            else:
                raise IndexError(f"Index {config_index} hors des limites. Spécifiez un index entre 0 et {len(task_sets) - 1}.")
    elif isinstance(task_sets, dict):
        return task_sets
    else:
        raise ValueError("Le fichier JSON doit contenir une liste ou un dictionnaire de configurations.")

def display_schedule(json_filename, config_index=None, stop_on_deadline=False, stop_on_repetition=False, display_graph=True, export_repeated_states=True):
    """
    Charge une configuration de tâches, exécute l'ordonnancement, affiche un graphique et exporte les répétitions d'états.

    Parameters:
    - json_filename (str): Chemin du fichier JSON contenant les configurations.
    - config_index (int): Index de la configuration à charger (None si une seule configuration).
    - stop_on_deadline (bool): Arrêter si une deadline est ratée.
    - stop_on_repetition (bool): Arrêter si une répétition d'état est trouvée.
    - display_graph (bool): Afficher le graphique de l'ordonnancement.
    - export_repeated_states (bool): Exporter les répétitions d'états dans un fichier Excel.
    """
    # Charger la configuration
    try:
        datajson = load_task_set(json_filename, config_index)
    except (ValueError, IndexError) as e:
        print(f"Erreur lors du chargement de la configuration : {e}")
        return

    # Initialiser le scheduler
    scheduler = SchedulerDM()
    scheduler.configure_json(datajson)

    # Exécuter l'ordonnancement
    max_time = datajson.get("max_time", 100)
    print(f"Lancement de l'ordonnancement pour un temps maximum de {max_time} unités...")
    scheduler.schedule(
        max_time=max_time,
        stop_on_repeated_state=stop_on_repetition,
        stop_on_missed_deadline=stop_on_deadline
    )

    # Afficher le graphique si demandé
    if display_graph:
        print("Affichage du graphique...")
        display = ScheduleDisplay(max_time=max_time, render="browser")
        display.update_from_scheduler(scheduler)
        display.fig.show()

    # Exporter les répétitions d'états si demandé
    if export_repeated_states and len(scheduler.repeated_states) > 0:
        repeated_states_filename = os.path.splitext(json_filename)[0] + "_repeated_states.xlsx"
        print(f"Export des répétitions d'états dans {repeated_states_filename}...")
        scheduler.export_repeated_states_to_excel(repeated_states_filename)

    print("Ordonnancement terminé.")

if __name__ == "__main__":
    # Vérifier les arguments de la ligne de commande
    if len(sys.argv) < 2:
        print("Usage : python display_schedule.py <fichier_json> [index_configuration] [--stop-on-deadline] [--stop-on-repetition] [--no-display-graph] [--no-export-repeated-states]")
        sys.exit(1)

    json_file = sys.argv[1]
    config_index = int(sys.argv[2]) if len(sys.argv) > 2 and sys.argv[2].isdigit() else None
    stop_on_deadline = "--stop-on-deadline" in sys.argv
    stop_on_repetition = "--stop-on-repetition" in sys.argv
    display_graph = "--no-display-graph" not in sys.argv
    export_repeated_states = "--no-export-repeated-states" not in sys.argv

    if not os.path.exists(json_file):
        print(f"Le fichier {json_file} n'existe pas.")
        sys.exit(1)

    # Lancer l'affichage de l'ordonnancement
    display_schedule(json_file, config_index, stop_on_deadline, stop_on_repetition, display_graph, export_repeated_states)