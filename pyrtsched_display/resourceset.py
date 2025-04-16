import json
import pandas as pd
from . import ResourceType, Resource

class ResourceSet:
    _resources = []

    def __init__(self, nb_processor: int):
        self._resources = []

        for p in range(nb_processor):
            resource = Resource(f"P{p}", ResourceType.Processor)
            self._resources.append(resource)   

        resource = Resource("M", ResourceType.Memory)
        self._resources.append(resource) 
    
    def __str__(self):
        string = "Resources:\n"
        for resource in self._resources:
            string += "\t" + resource.__str__() + "\n"
        return string

    @property
    def resources(self) -> list[Resource]:
        return self._resources
    
    def get_resource(self, name: str) -> Resource:
        for resource in self._resources:
            if resource.name == name:
                return resource
        return None
    
    def get_resourceset_as_dataframe(self):
        schema_resourceset={'Name': 'string', 'Type': 'string'}
        df_resourceset = pd.DataFrame(columns=schema_resourceset.keys()).astype(schema_resourceset)
        for resource in self._resources:
            df_resourceset = pd.concat([df_resourceset, pd.DataFrame([dict(Name=resource.name, Type=resource.type.name)])])
        return df_resourceset