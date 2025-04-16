from enum import Enum

class ResourceType(Enum):
    Processor = 1
    Memory = 2

class Resource:
    _name = ""
    _type = ResourceType.Processor

    def __init__(self, name, type):
        self._name = name
        self._type = type

    def __str__(self):
        return "Resource: " + self._name + " (" + str(self._type.name) + ")"

    @property
    def name(self):
        return self._name
    
    @name.setter
    def name(self, value: str):
        self._name = value

    @property
    def type(self):
        return self._type
    
    @type.setter
    def type(self, value: ResourceType):
        self._type = value