from dash import Dash, dcc, html, Input, Output, State, callback, DiskcacheManager, CeleryManager
import dash_bootstrap_components as dbc

from pyrtsched_display import SchedulerDM, Scheduler
from pyrtsched_display import ScheduleDisplay
import json
import pandas as pd
import os

if 'REDIS_URL' in os.environ:
    # Use Redis & Celery if REDIS_URL set as an env variable
    from celery import Celery
    celery_app = Celery(__name__, broker=os.environ['REDIS_URL'], backend=os.environ['REDIS_URL'])
    background_callback_manager = CeleryManager(celery_app)

else:
    # Diskcache for non-production apps when developing locally
    import diskcache
    cache = diskcache.Cache("./cache")
    background_callback_manager = DiskcacheManager(cache)


app = Dash(__name__, background_callback_manager=background_callback_manager, external_stylesheets=[dbc.themes.BOOTSTRAP, dbc.icons.FONT_AWESOME])
app.scripts.config.serve_locally = True
app.css.config.serve_locally = True

app.layout = html.Div([
    dcc.Textarea(
        id='schedule_json_conf',
        value="""{
            "max_time": 40,
            "premption_processor": "True",
            "premption_memory": "True",
            "tasks": [
                {"Name": "T1", "O": 0, "R": 2, "E": 3, "W": 1, "D": 10, "T": 9},
                {"Name": "T2", "O": 1, "R": 1, "E": 1, "W": 1, "D": 4, "T": 9},
                {"Name": "T3", "O": 0, "C": 2, "D": 5, "T": 6}
            ],
            "resources": [
                {"Name": "P1", "Type": "Processor"},
                {"Name": "M1", "Type": "Memory"}
            ]
        }""",
        style={'width': '100%', 'height': 200, 'font-size': '90%',},
    ),
    dbc.Progress(id="progress", value=0, color="success", className="mb-3", animated=False, style={"height": "20px"}),
    dbc.Button("See Scheduling", id='see_schedule_button', color="primary", className="me-1"),
    dbc.Button("Cancel Scheduling !", id='cancel_see_schedule_button', color="secondary", className="me-1", disabled=True),
    dbc.Button([html.I(className="fa-solid fa-file-excel")," Download Schedule"], id='excel_schedule_button', color="primary", outline=True, className="me-1"),
    dcc.Download(id="download_excel_schedule"),
    dcc.Graph(id='graph',style={'height': '90vh'})
])

@callback(
    Output("download_excel_schedule", "data"),
    Input("excel_schedule_button", "n_clicks"),
    State('schedule_json_conf', 'value'),
    prevent_initial_call=True,
)
def download_schedule_excel(n_clicks, value):
    df = pd.DataFrame()
    if n_clicks > 0:
        print("Genrate Excel")
        print(value)
        if (value!=""):
            json_value = read_schedule_json(value)

            max_time = json_value["max_time"]
            scheduler = prepare_schedule(json_value)
            df = scheduler.schedule_result

    return dcc.send_data_frame(df.to_excel, "schedule.xlsx", sheet_name="Scheduling", index=False)

def read_schedule_json(json_value) -> dict:
    if (isinstance(json_value, str)):
        json_value = json.loads(json_value)
    return json_value

def prepare_schedule(json_value: dict) -> Scheduler:
    max_time = json_value["max_time"]

    scheduler = SchedulerDM()
    scheduler.configure_json(json_value)
    scheduler.schedule(max_time)

    return scheduler

@callback(
    Output("progress", "value"), Output("progress", "max"), Output("progress", "label"), Output("progress", "animated"),
    Input('cancel_see_schedule_button', 'n_clicks'),
    prevent_initial_call=True,
)
def cancel_display_graph(n_clicks):
    return str(0), str(3), "Waiting...", False
    
@callback(
    Output('graph', 'figure'),
    Input('see_schedule_button', 'n_clicks'),
    State('schedule_json_conf', 'value'),
    background=True,
    running=[
        (Output("see_schedule_button", "disabled"), True, False),
        (Output("cancel_see_schedule_button", "disabled"), False, True),
    ],
    cancel=Input("cancel_see_schedule_button", "n_clicks"),
    progress=[Output("progress", "value"), Output("progress", "max"), Output("progress", "label"), Output("progress", "animated")],
    prevent_initial_call=True,
)
def display_graph(set_progress, n_clicks, value):
    total_progress = 3
    current_progress = 0
    set_progress((str(current_progress), str(total_progress), "Read JSON...", True))

    graphJSON = {}
    if n_clicks > 0:
        print("Generate Graph")
        print(value)
        if (value!=""):
            json_value = read_schedule_json(value)

            current_progress += 1
            set_progress((str(current_progress), str(total_progress), "Generate Schedule...", True))

            max_time = json_value["max_time"]
            scheduler = prepare_schedule(json_value)

            current_progress += 1
            set_progress((str(current_progress), str(total_progress), "Generate Graph...", True))

            # Prepare the schedule for display
            graph = ScheduleDisplay(max_time=max_time, render="browser")
            graph.update_from_scheduler(scheduler)
            graphJSON = graph.fig

            current_progress += 1
            set_progress((str(current_progress), str(total_progress), "Finished", False))
    
    return graphJSON

if __name__ == '__main__':
    app.run(debug=True, port=8050, host='0.0.0.0')