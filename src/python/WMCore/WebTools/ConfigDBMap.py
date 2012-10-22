"""
provide the map between configuration of web tool and
DBFactory parameters
"""
from WMCore.Database.ConfigDBMap import ConfigDBMapInterface

class ConfigDBMap(ConfigDBMapInterface):

    def __init__(self, config):
        assert hasattr(config, 'database'), "No database configured"
        if hasattr(config.database, 'instances'):
            # The application has multiple instances
            database = getattr(config.database.instances, config.instance)
            self.configure_my_db(database)
        else:
            self.configure_my_db(config.database)

    def configure_my_db(self, database):
        assert hasattr(database, 'connectUrl'), "No database url configured"
        self.dbUrl = database.connectUrl
        self.option = {}
        if hasattr(database, "socket"):
            self.option["unix_socket"] = database.socket
        if hasattr(database, "engineParameters"):
            self.option['engine_parameters'] = database.engineParameters

    def getDBUrl(self):
        return self.dbUrl

    def getOption(self):
        return self.option
