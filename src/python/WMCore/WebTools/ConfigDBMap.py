"""
provide the map between configuration of web tool and 
DBFactory parameters
"""
from WMCore.Database.ConfigDBMap import ConfigDBMapInterface

class ConfigDBMap(ConfigDBMapInterface):

    def __init__(self, config):
        assert hasattr(config, 'database'), "No database configured"
        assert hasattr(config.database, 'connectUrl'), "No database url configured"
        self.dbUrl = config.database.connectUrl
        self.option = {}
        if hasattr(config.database, "socket"):
            self.option["unix_socket"] = config.database.socket
        if hasattr(config.database, "engineParameters"):
            self.option['engine_parameters'] = config.database.engineParameters
            
    def getDBUrl(self):
        return self.dbUrl
    
    def getOption(self):
        return self.option