class ConfigDBMapInterface(object):
    """
    Interface for converting the configuration to
    db url string and option dictionary for DBFactory parameters

    the implementation of this class should create db url string.
    and option for DBFactory creation.
    For details of options, reference WMCore.Database.DBFactory
    """

    def __init__(self, config):
        self.config = config

    def getDBUrl(self):
        """
        this should return db string
        i.e. mysql://username@hostname.fnal.gov:3306/TestDB
        """
        raise NotImplementedError("getDBUrl is not implemented")

    def getOption(self):
        """
        this should return options of dict format
        i.e. {'engine_parameters': {'pool_size': 10}}
        """
        raise NotImplementedError("getOption is not implemented")
