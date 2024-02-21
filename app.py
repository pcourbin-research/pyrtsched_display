from dash import Dash, dcc, html, Input, Output, State, callback

from pyrtsched_display import SchedulerDM
from pyrtsched_display import ScheduleDisplay
import json

app = Dash(__name__)
app.scripts.config.serve_locally = True
app.css.config.serve_locally = True

app.layout = html.Div([
    dcc.Textarea(
        id='textarea-schedule_json_conf',
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
        style={'width': '100%', 'height': 200},
    ),
    html.Button('See Scheduling', id='textarea-schedule_json_conf-button', n_clicks=0),
    dcc.Graph(id='graph',style={'height': '90vh'})
])

@callback(
    Output('graph', 'figure'),
    Input('textarea-schedule_json_conf-button', 'n_clicks'),
    State('textarea-schedule_json_conf', 'value')
)
def display_graph(n_clicks, value):
    print(value)
    graphJSON = {}
    if n_clicks > 0:
        print(value)
        if (value!=""):
            if (isinstance(value, str)):
                value = json.loads(value)
            
            max_time = value["max_time"]

            scheduler = SchedulerDM()
            scheduler.configure_json(value)
            scheduler.schedule(max_time)

            # Save the schedule to an Excel file
            scheduler.schedule_result.to_excel("schedule.xlsx", index=False)

            # Prepare the schedule for display
            graph = ScheduleDisplay(max_time=max_time, render="browser")
            graph.update_from_scheduler(scheduler)
            graphJSON = graph.fig
    
    return graphJSON

if __name__ == '__main__':
    app.run(debug=True, port=8050, host='0.0.0.0')