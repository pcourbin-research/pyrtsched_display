import json
from resource import ResourceType, Resource
import pandas as pd

class ResourceSet:
    _resources = []

    def __init__(self, data):
        self._resources = []
        if (isinstance(data, str)):
            data = json.loads(data)

        for resource_data in data:
            resource = Resource(resource_data["Name"], ResourceType[resource_data["Type"]])
            self._resources.append(resource)
    
    def __str__(self):
        string = "Resources:\n"
        for resource in self._resources:
            string += "\t" + resource.__str__() + "\n"
        return string

    @property
    def resources(self) -> list[Resource]:
        return self._resources
    
    def get_resourceset_as_dataframe(self):
        schema_resourceset={'Name': 'string', 'Type': 'string'}
        df_resourceset = pd.DataFrame(columns=schema_resourceset.keys()).astype(schema_resourceset)
        for resource in self._resources:
            df_resourceset = pd.concat([df_resourceset, pd.DataFrame([dict(Name=resource.name, Type=resource.type.name)])])
        return df_resourceset