class ConfigDBMapInterface(object):
    
    def __init__(self, config):
        self.config = config
    
    def getDBUrl(self):
        raise NotImplementedError, "getDBUrl is not implemented"
    
    def getOption(self):
        raise NotImplementedError, "getDBUrl is not implemented"