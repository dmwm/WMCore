from WMCore.WMBS.Actions.Action import BaseAction
class LoadFilesetAction(BaseAction):
    name = "LoadFileset"
    
    def execute(self, name=None, dbinterface=None):
        """
        import the approriate SQL object and execute it
        """ 
        myclass = self.loadDialect(self.name, dbinterface)
        action = myclass(self.logger, dbinterface)
        try:
            return action.execute(fileset = name)
        except Exception, e:
            self.logger.exception(e)
            return False