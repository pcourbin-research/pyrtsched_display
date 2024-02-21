# https://plotly.com/python/gantt/
import plotly.express as px
import pandas as pd
import plotly.io as pio
import pathlib
import os
from . import Scheduler

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

    def update_from_scheduler(self, scheduler: Scheduler):
        self.update(scheduler.taskset.get_taskset_as_dataframe(), scheduler.resourceset.get_resourceset_as_dataframe(), scheduler.schedule_result)

    def update(self, tasks: pd.DataFrame, resources: pd.DataFrame, schedule: pd.DataFrame):
        self._tasks = tasks.copy()
        self._tasks.set_index('Name', inplace=True)
        self._tasks_list = self._tasks.index.tolist()
        self._category_list = self._tasks_list + self._ressource_list

        self._resources = resources.copy()
        # Add empty resource for processor, for row with missed deadline and no resource
        self._resources.loc[len(self._resources)] = ["", "Processor"]
        self._resources.set_index('Name', inplace=True)
        self._ressource_list = self._resources.index.tolist()
        self._category_list = self._tasks_list + self._ressource_list

        self._schedule = schedule.copy().reset_index(drop=True)
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
                                arrowcolor = 'green', arrowwidth = 2,
                                arrowside = 'end', arrowsize = 1, arrowhead = 2)
                
                fig.add_annotation(ax = deadline_date, axref = 'x', 
                                x = deadline_date, xref = 'x',
                                ay = index-0.50, ayref = 'y',
                                y = index+0.45, yref = 'y',
                                arrowcolor = 'red', arrowwidth = 2,
                                arrowside = 'end', arrowsize = 1, arrowhead = 2)
                
                activation += task["T"]
                deadline += task["T"]
        
    def fig_add_missed(self, fig):
        # Plot missed deadline
        for index, sched in self._schedule[self._schedule["Missed"]!=""].iterrows():
            index_task = self._category_list.index(sched["Task"])
            fig.add_shape(type="rect",
                x0=sched["Start"], x1=sched["Finish"], y0=index_task-self._rect_width/2-0.1, y1=index_task+self._rect_width/2+0.1,
                line=dict(color="coral", width=10)
            )

    

    def fig_generate(self):
        # Plots Timelines
        hovertemplate_task = "<b>%{base|%S%4f} - %{x|%S%4f}</b><br>Job %{customdata[3]} of %{y} on %{customdata[0]} (%{customdata[1]})<br>Phase %{customdata[4]}/%{customdata[6]}, Request remaining: %{customdata[5]}/%{customdata[7]}<extra></extra>"
        tasks_plot = px.timeline(self._schedule, x_start="Start", x_end="Finish", y="Task", color="Task", 
                        pattern_shape="Resource_Type", pattern_shape_map={"Memory": "x", "Processor": ""}, 
                        custom_data=["Resource", "Resource_Type", "Missed", "Job", "Phase", "RequestPhaseRemaining", "TotalPhase", "TotalRequestPhase"])

        hovertemplate_ressource = "<b>%{base|%S%4f} - %{x|%S%4f}</b><br>%{y} (%{customdata[1]}) execute Job %{customdata[3]} of %{customdata[0]} <br><extra></extra>"
        resources_plot = px.timeline(self._schedule, x_start="Start", x_end="Finish", y="Resource", color="Task", 
                        pattern_shape="Resource_Type", pattern_shape_map={"Memory": "x", "Processor": ""}, 
                        custom_data=["Task", "Resource_Type", "Missed", "Job"])

        tasks_plot.update_layout(
            showlegend=True,
            xaxis_tickformatstops = [
                dict(dtickrange=[None, 1], value="%S%4f"),
                dict(dtickrange=[1, None], value="%S%L")
            ]
        )
        tasks_plot.update_traces(hovertemplate=hovertemplate_task)

        resources_plot.update_layout(showlegend=False)
        resources_plot.update_traces(hovertemplate=hovertemplate_ressource)
        # https://stackoverflow.com/questions/67964550/add-custom-markers-to-gantt-chart-in-plotly
        #fig = go.Figure(data=tasks_plot.data + resources_plot.data, layout=resources_plot.layout)
        self._fig = tasks_plot 
        self._fig.add_traces(resources_plot.data)

        # set showlegend property by name of trace
        for trace in self._fig['data']: 
            if(trace['y'][0] not in self._tasks_list): trace['showlegend'] = False

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
        if (dir_path == ""):
            dir_path = "./"

        if not os.path.exists(dir_path):
            os.makedirs(dir_path)

        if (ext.lower() in [".html", ".htm"]):
            fig.write_html(path)
        else: # png, webp, jpeg, pdf, svg, eps
            """fig.update_layout(
                autosize = False,
                width = self._max_time*40,
                height = (len(self._tasks) + len(self._resources) + 1) * 140
            )"""

            fig.update_layout(
                autosize = True
            )
        
            fig.write_image(path)


            