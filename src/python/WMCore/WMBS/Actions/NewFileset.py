from WMCore.WMBS.Actions.Action import BaseAction

class NewFilesetAction(BaseAction):
    name = "NewFileset"
        
    def execute(self, fileset = None, dbinterface = None):
        """
        import the approriate SQL object and execute it
        """ 
        myclass = self.loadDialect(self.name, dbinterface)
        self.logger.debug("Adding %s" % fileset)    
        action = myclass(self.logger, dbinterface)
        try:
            action.execute(fileset)
            return True
        except Exception, e:
            self.logger.exception(e)
            return False