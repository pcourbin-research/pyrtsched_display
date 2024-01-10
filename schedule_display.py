# https://plotly.com/python/gantt/
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd
from plotly.subplots import make_subplots
import plotly.io as pio
import pathlib
import os

class ScheduleDisplay:

    _max_time = 40
    _rect_width = 0.8
    _tasks = pd.DataFrame()
    _tasks_list = []
    _resources = pd.DataFrame()
    _ressource_list = []
    _category_list = []
    _schedule = pd.DataFrame()
    _fig = None

    def __init__(self, max_time=40, render="browser"):
        self._max_time = max_time
        pio.renderers.default = render

    def update(self, tasks: pd.DataFrame, resources: pd.DataFrame, schedule: pd.DataFrame):
        self._tasks = tasks.copy()
        self._tasks.set_index('Name', inplace=True)
        self._tasks_list = self._tasks.index.tolist()
        self._category_list = self._tasks_list + self._ressource_list

        self._resources = resources.copy()
        self._resources.set_index('Name', inplace=True)
        self._ressource_list = self._resources.index.tolist()
        self._category_list = self._tasks_list + self._ressource_list

        self._schedule = schedule.copy()
        self._schedule["Start"] = ScheduleDisplay.convert_date(self._schedule["Start"])
        self._schedule["Finish"] = ScheduleDisplay.convert_date(self._schedule["Finish"])

        self._schedule["Resource_Type"] = self._resources.loc[self._schedule["Resource"]]["Type"].reset_index(drop=True)
        self._schedule = self._schedule.sort_values(by=['Task', 'Resource'])

        self._fig = None

    @property
    def max_time(self):
        return self._max_time
    
    @max_time.setter
    def max_time(self, value):
        self._max_time = value

    @property
    def fig(self):
        if (self._fig is None):
            self._fig = self.fig_generate()
        return self._fig

    def convert_date(val):
        date_start = '2020-01-01 00:00:00.'
        if (isinstance(val, str) == False):
            if (isinstance(val, pd.Series)):
                val = val.astype(str).str
            else:
                val = str(val)
        return date_start+val.zfill(4)
    
    def fig_add_activation(self, fig):
        # Plot activations
        for index in range(len(self._tasks_list)):
            task = self._tasks.loc[self._tasks_list[index]]
            activation = int(task["O"])
            deadline = int(task["O"])+int(task["D"])
            for t in range(int((self._max_time-int(task["O"]))/int(task["T"])+1)):

                activation_date = ScheduleDisplay.convert_date(activation)
                deadline_date = ScheduleDisplay.convert_date(deadline)
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
        
    def fig_add_missed(self, fig):
        # Plot missed deadline
        for index, sched in self._schedule[self._schedule["Missed"]!=""].iterrows():
            index_task = self._category_list.index(sched["Task"])
            fig.add_shape(type="rect",
                x0=sched["Start"], x1=sched["Finish"], y0=index_task-self._rect_width/2, y1=index_task+self._rect_width/2,
                line=dict(color="red", width=5)
            )

    

    def fig_generate(self):
        # Plots Timelines
        tasks_plot = px.timeline(self._schedule, x_start="Start", x_end="Finish", y="Task", color="Task", 
                        pattern_shape="Resource_Type", pattern_shape_map={"Memory": "x", "Processor": ""})

        resources_plot = px.timeline(self._schedule, x_start="Start", x_end="Finish", y="Resource", color="Task", 
                        pattern_shape="Resource_Type", pattern_shape_map={"Memory": "x", "Processor": ""})

        tasks_plot.update_layout(
            showlegend=True,
            xaxis_tickformatstops = [
                dict(dtickrange=[None, 1], value="%S%4f"),
                dict(dtickrange=[1, None], value="%S%L")
            ]
        )
        resources_plot.update_layout(showlegend=False)
        # https://stackoverflow.com/questions/67964550/add-custom-markers-to-gantt-chart-in-plotly
        #fig = go.Figure(data=tasks_plot.data + resources_plot.data, layout=resources_plot.layout)
        self._fig = tasks_plot #go.Figure(layout=tasks_plot.layout)
        #fig = make_subplots(rows=2, cols=1, shared_xaxes=True, vertical_spacing=0.02)

        #fig.add_traces(tasks_plot.data)
        self._fig.add_traces(resources_plot.data)

        self._fig.update_xaxes(rangeslider_visible=True)
        self._fig.update_yaxes(categoryorder='array', categoryarray = self._category_list, autorange="reversed")
        self._fig.update_traces(width=self._rect_width)

        self.fig_add_activation(self._fig)
        self.fig_add_missed(self._fig)

        return self._fig
    
    def fig_save(self, path="./export/fig.pdf"):
        fig = self.fig
        ext = pathlib.Path(path).suffix
        dir_path = os.path.dirname(path)

        if not os.path.exists(dir_path):
            os.makedirs(dir_path)

        if (ext.lower() in [".html", ".htm"]):
            fig.write_html(path)
        else: # png, webp, jpeg,pdf, svg, eps
            
            fig.update_layout(
                autosize = False,
                width = self._max_time*40,
                height = (len(self._tasks) + len(self._resources) + 1) * 140
            )
        
            fig.write_image(path)


            