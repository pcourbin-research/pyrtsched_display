from resource import ResourceType

class TaskPhase:
    _ressource_type = ResourceType.Processor
    _duration = 0
    _premption = True

    def __init__(self, ressource_type: ResourceType, duration: int, premption: bool):
        self._ressource_type = ressource_type
        self._duration = duration
        self._premption = premption

    def __str__(self):
        return "[Type = " + str(self._ressource_type.name) + ", duration = " + str(self._duration) + ", premption = " + str(self._premption) + "]"

    @property
    def ressource_type(self) -> ResourceType:
        return self._ressource_type
    
    @ressource_type.setter
    def ressource_type(self, value: ResourceType):
        self._ressource_type = value

    @property
    def duration(self) -> int:
        return self._duration
    
    @duration.setter
    def duration(self, value: int):
        self._duration = value

    @property
    def premption(self) -> bool:
        return self._premption
    
    @premption.setter
    def premption(self, value: bool):
        self._premption = value

class Task:
    _name = ""
    _first_activation = 0
    _deadline = 0
    _period = 0
    _phases = [] # List of TaskPhase
    
    def __init__(self, name, first_activation, deadline, period):
        self._name = name
        self._first_activation = first_activation
        self._deadline = deadline
        self._period = period
        self._phases = []

    def __str__(self):
        string = "Task: " + self._name + "\n"
        string += "\tFirst activation: " + str(self._first_activation) + "\n"
        string += "\tDeadline: " + str(self._deadline) + "\n"
        string += "\tPeriod: " + str(self._period) + "\n"
        string += "\tPhases: \n" 
        for phase in self._phases:
            string += "\t\t" + phase.__str__() + "\n"
        return string

    def add_phase(self, ressource_type: ResourceType, duration: int, premption: bool):
        self._phases.append(TaskPhase(ressource_type, duration, premption))

    @property
    def name(self):
        return self._name
    
    @name.setter
    def name(self, value: str):
        self._name = value

    @property
    def first_activation(self) -> int:
        return self._first_activation
    
    @first_activation.setter
    def first_activation(self, value: int):
        self._first_activation = value

    @property
    def deadline(self) -> int:
        return self._deadline
    
    @deadline.setter
    def deadline(self, value: int):
        self._deadline = value

    @property
    def period(self) -> int:
        return self._period

    @period.setter
    def period(self, value: int):
        self._period = value
    
    @property
    def phases(self) -> list[TaskPhase]:
        return self._phases
