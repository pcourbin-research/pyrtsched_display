# https://plotly.com/python/gantt/
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd
import numpy as np
from plotly.subplots import make_subplots
import plotly.io as pio
pio.renderers.default = "browser"

max_time = 40
rect_width = 0.8

def convert_date(val):
    date_start = '2020-01-01 00:00:00.'
    if (isinstance(val, str) == False):
        if (isinstance(val, pd.Series)):
            val = val.astype(str).str
        else:
            val = str(val)
        
    return date_start+val.zfill(4)

tasks = pd.DataFrame([
    dict(Name="T1", O=0, C=5, D=10, T=10),
    dict(Name="T2", O=1, C=2, D=8, T=9),
    dict(Name="T3", O=0, C=2, D=5, T=6)
])
tasks.set_index('Name', inplace=True)
tasks_list = tasks.index.tolist()

resources = pd.DataFrame([
    dict(Name="P1", Type="Processor"),
    dict(Name="P2", Type="Processor"),
    dict(Name="M1", Type="Memory")
])
resources.set_index('Name', inplace=True)
ressource_list = resources.index.tolist()
category_list = tasks_list + ressource_list

schedule = pd.DataFrame([
    dict(Task="T1", Start=1, Finish=5, Resource="P1", Missed=""),
    dict(Task="T1", Start=7, Finish=8, Resource="P1", Missed="Missed"),
    dict(Task="T2", Start=0, Finish=1, Resource="P1", Missed=""),
    dict(Task="T3", Start=0, Finish=5, Resource="P2", Missed="Missed"),
    dict(Task="T1", Start=0, Finish=1, Resource="M1", Missed=""),
    dict(Task="T2", Start=1, Finish=3, Resource="M1", Missed="")
])


schedule["Start"] = convert_date(schedule["Start"])
schedule["Finish"] = convert_date(schedule["Finish"])
schedule["Resource_Type"] = resources.loc[schedule["Resource"]]["Type"].reset_index(drop=True)
print(resources.loc[schedule["Resource"]]["Type"])
print(resources.loc[schedule["Resource"]]["Type"].reset_index(drop=True))
print(schedule)

schedule = schedule.sort_values(by=['Task', 'Resource'])

# Plots Timelines
tasks_plot = px.timeline(schedule, x_start="Start", x_end="Finish", y="Task", color="Task", 
                   pattern_shape="Resource_Type", pattern_shape_map={"Memory": "x", "Processor": ""})

resources_plot = px.timeline(schedule, x_start="Start", x_end="Finish", y="Resource", color="Task", 
                   pattern_shape="Resource_Type", pattern_shape_map={"Memory": "x", "Processor": ""})

tasks_plot.update_layout(
    showlegend=True,
    xaxis_tickformatstops = [
        dict(dtickrange=[None, 1], value="%S%4f"),
        dict(dtickrange=[1, None], value="%S%L")
    ]
)

#tasks_plot.update_yaxes(categoryorder='array', categoryarray = category_list)
#tasks_plot.update_yaxes(categoryorder="category descending")

resources_plot.update_layout(showlegend=False)
#resources_plot.update_yaxes(categoryorder='array', categoryarray = category_list)
#resources_plot.update_yaxes(categoryorder="category descending")

# https://stackoverflow.com/questions/67964550/add-custom-markers-to-gantt-chart-in-plotly
#fig = go.Figure(data=tasks_plot.data + resources_plot.data, layout=resources_plot.layout)
fig = tasks_plot #go.Figure(layout=tasks_plot.layout)
#fig = make_subplots(rows=2, cols=1, shared_xaxes=True, vertical_spacing=0.02)

#fig.add_traces(tasks_plot.data)
fig.add_traces(resources_plot.data)

fig.update_xaxes(rangeslider_visible=True)
#fig.update_yaxes(autorange="reversed")
fig.update_yaxes(categoryorder='array', categoryarray = category_list, autorange="reversed")
fig.update_traces(width=rect_width)
#fig.update_layout(barmode='group')


# Plot activations
for index in range(len(tasks_list)):
    task = tasks.loc[tasks_list[index]]
    activation = int(task["O"])
    deadline = int(task["O"])+int(task["D"])
    for t in range(int((max_time-int(task["O"]))/int(task["T"])+1)):

        activation_date = convert_date(activation)
        deadline_date = convert_date(deadline)
        fig.add_annotation(ax = activation_date, axref = 'x', 
                        x = activation_date, xref = 'x',
                        ay = index+0.50, ayref = 'y',
                        y = index-0.45, yref = 'y',
                        arrowcolor = 'black', arrowwidth = 2.5,
                        arrowside = 'end', arrowsize = 1, arrowhead = 2)
        
        fig.add_annotation(ax = deadline_date, axref = 'x', 
                        x = deadline_date, xref = 'x',
                        ay = index-0.55, ayref = 'y',
                        y = index+0.40, yref = 'y',
                        arrowcolor = 'red', arrowwidth = 2.5,
                        arrowside = 'end', arrowsize = 1, arrowhead = 2)
        
        activation += task["T"]
        deadline += task["T"]

# Plot missed deadline
for index, sched in schedule[schedule["Missed"]!=""].iterrows():
    index_task = category_list.index(sched["Task"])
    fig.add_shape(type="rect",
        x0=sched["Start"], x1=sched["Finish"], y0=index_task-rect_width/2, y1=index_task+rect_width/2,
        line=dict(color="red", width=5)
    )
    

#fig.show()
fig.write_html("fig.html")


fig.update_layout(
    autosize=False,
    width=2000,
    height=1000
)
fig.write_image("fig1.pdf")
