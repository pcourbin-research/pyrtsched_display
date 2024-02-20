from enum import Enum

class ResourceType(Enum):
    Processor = 1
    Memory = 2

class Resource:
    __name = ""
    _type = ResourceType.Processor

    def __init__(self, name, type):
        self.__name = name
        self._type = type

    def __str__(self):
        return "Resource: " + self.__name + " (" + str(self._type.name) + ")"

    @property
    def name(self):
        return self.__name
    
    @name.setter
    def name(self, value: str):
        self.__name = value

    @property
    def type(self):
        return self._type
    
    @type.setter
    def type(self, value: ResourceType):
        self._type = value