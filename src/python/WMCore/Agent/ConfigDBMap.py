"""
provide the map between configuration of web tool and
DBFactory parameters
"""
from WMCore.Database.ConfigDBMap import ConfigDBMapInterface

class ConfigDBMap(ConfigDBMapInterface):

    def __init__(self, config):
        assert hasattr(config, 'CoreDatabase'), "No database configured"
        assert hasattr(config.CoreDatabase, 'connectUrl'), "No database url configured"
        self.dbUrl = config.CoreDatabase.connectUrl
        self.option = {}
        if hasattr(config.CoreDatabase, "socket"):
            self.option["unix_socket"] = config.CoreDatabase.socket
        if hasattr(config.CoreDatabase, "engineParameters"):
            self.option['engine_parameters'] = config.CoreDatabase.engineParameters

    def getDBUrl(self):
        return self.dbUrl

    def getOption(self):
        return self.option
