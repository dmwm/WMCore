from WMCore.WMBS.Actions.Action import BaseAction

class FilesetExistsAction(BaseAction):
    name = "FilesetExists"
    
    def execute(self, fileset=None, dbinterface = None):
        """
        import the approriate SQL object and execute it
        """ 
        myclass = self.loadDialect(self.name, dbinterface)
        action = myclass(self.logger, dbinterface)
        try:
            return action.execute(fileset)
        except Exception, e:
            self.logger.exception(e)
            return False