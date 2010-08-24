from WMCore.WMBS.Actions.Action import BaseAction

class AddLocationAction(BaseAction):
    name = "AddLocation"
    
    def execute(self, sename=None, dbinterface=None):
        """
        import the approriate SQL object and execute it
        """ 
        myclass = self.loadDialect(self.name, dbinterface)
        action = myclass(self.logger, dbinterface)
        try:
            print "Hello!"
            value = action.execute(sename)
            print value
            return value
        except Exception, e:
            self.logger.exception(e)
            return False