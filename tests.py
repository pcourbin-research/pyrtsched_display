from schedule_display import ScheduleDisplay
import pandas as pd

tasks_ori = pd.DataFrame([
    dict(Name="T1", O=0, C=5, D=10, T=10),
    dict(Name="T2", O=1, C=2, D=4, T=9),
    dict(Name="T3", O=0, C=2, D=5, T=6)
])

resources_ori = pd.DataFrame([
    dict(Name="P1", Type="Processor")#,
    #dict(Name="P2", Type="Processor"),
    #dict(Name="M1", Type="Memory")
])
"""
schedule = pd.DataFrame([
    dict(Task="T1", Start=1, Finish=5, Resource="P1", Missed=""),
    dict(Task="T1", Start=7, Finish=8, Resource="P1", Missed="Missed"),
    dict(Task="T2", Start=0, Finish=1, Resource="P1", Missed=""),
    dict(Task="T3", Start=0, Finish=5, Resource="P2", Missed="Missed"),
    dict(Task="T1", Start=0, Finish=1, Resource="M1", Missed=""),
    dict(Task="T2", Start=1, Finish=3, Resource="M1", Missed="")
])
"""

max_time = 20


# DM
schedule = pd.DataFrame()
tasks = tasks_ori.sort_values(by=['D']).assign(Priority=range(len(tasks_ori)))
tasks["Request"] = 0
tasks.set_index('Name', inplace=True)
tasks_list = tasks.index.tolist()

resources = resources_ori.copy()
resources["Current"] = None
resources_list = resources.index.tolist()

for t in range(max_time):
    # Update tasks
    for index, ti in tasks.iterrows():
        if (t-ti["O"])%ti["T"] == 0:
            ti["Request"] += ti["C"]

    for ri in resources_list:
        for index, ti in tasks.iterrows():
            if ti["Request"] > 0:
                Missed = ""
                ti["Request"] -= 1
                #if (t+1-ti["O"]+ti["D"])%ti["T"] == 0 and ti["Request"] > 0 and t != ti["O"]:
                #    Missed = "Missed"
                schedule_temp = pd.DataFrame([dict(Task=index, Start=t, Finish=t+1, Resource=resources.iloc[ri]["Name"], Missed=Missed)])
                schedule = pd.concat([schedule, schedule_temp])
                break
        #if ri.Current != None and tasks.loc[ri.Current].C > 0:
            #schedule_temp = pd.DataFrame([dict(Task=, Start=1, Finish=5, Resource="P1", Missed="")])

    for index, ti in tasks.iterrows():
        if ti["Request"] > 0 and (t+1-ti["O"]+ti["D"])%ti["T"] == 0:
            Missed = "Missed"
            schedule_temp = pd.DataFrame([dict(Task=index, Start=t, Finish=t+1, Resource="P1", Missed=Missed)])
            schedule = pd.concat([schedule, schedule_temp])

print(schedule)

graph = ScheduleDisplay(max_time=max_time, render="browser")
graph.update(tasks_ori, resources_ori, schedule)

graph.fig.show()
#graph.fig_save("./export/test.html")
#graph.fig_save("./export/test.pdf")
#graph.fig_save("./export/test.svg")

#print(tasks)
