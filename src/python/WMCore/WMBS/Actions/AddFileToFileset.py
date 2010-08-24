from WMCore.WMBS.Actions.Action import BaseAction

class AddFileToFilesetAction(BaseAction):
    name = "AddFileToFileset"
        
    def execute(self, file=None, fileset=None, dbinterface=None):
        """
        import the approriate SQL object and execute it
        """ 
        myclass = self.loadDialect(self.name, dbinterface)
        action = myclass(self.logger, dbinterface)
        try:
            return action.execute(file, fileset)
        except Exception, e:
            self.logger.exception(e)
            return False