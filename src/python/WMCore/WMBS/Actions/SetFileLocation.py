from WMCore.WMBS.Actions.Action import BaseAction

class SetFileLocationAction(BaseAction):
    name = "SetFileLocation"
        
    def execute(self, file=None, sename=None, dbinterface=None):
        """
        import the approriate SQL object and execute it
        """ 
        myclass = self.loadDialect(self.name, dbinterface)
        action = myclass(self.logger, dbinterface)
        try:
            return action.execute(file, sename)
        except Exception, e:
            self.logger.exception(e)
            return False