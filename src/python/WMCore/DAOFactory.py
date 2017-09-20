"""
A basic action is a thing that will run a SQL statement

A more complex one would be something that ran multiple SQL
objects to produce a single output.
"""
class DAOFactory(object):
    def __init__(self, package='WMCore', logger=None, dbinterface=None, owner=""):
        self.package = package
        self.logger = logger
        self.dbinterface = dbinterface
        self.owner = owner
        #self.logger.debug("Instantiating DAOFactory for %s package" % self.package)
        from WMCore.Database.Dialects import MySQLDialect
        from WMCore.Database.Dialects import OracleDialect
        self.dialects = {"Oracle" : OracleDialect,
                    "MySQL" : MySQLDialect,}

    def __call__(self, classname):
        """
        Somewhat fugly method to load generic SQL classes...
        """
        if not isinstance(self.dbinterface, str):

            dia = self.dbinterface.engine.dialect
            #TODO: Make good
            dialect = None
            for i in self.dialects.keys():
                if isinstance(dia, self.dialects[i]):
                    dialect = i
            if not dialect:
                raise TypeError("unknown connection type: %s" % dia)
        else:
            dialect = 'CouchDB'


        module = "%s.%s.%s" % (self.package, dialect, classname)
        #self.logger.debug("importing %s, %s" % (module, classname))
        module = __import__(module, globals(), locals(), [classname])#, -1)
        instance = getattr(module, classname.split('.')[-1])
        if self.owner:
            return instance(self.logger, self.dbinterface, self.owner)
        else:
            return instance(self.logger, self.dbinterface)
