from WMCore.WMBS.Actions.Action import BaseAction

class CreateWMBSAction(BaseAction):
    name = "CreateWMBS"
        
    def execute(self, dbinterface=None):
        """
        import the approriate SQL object and execute it
        """ 
        BaseAction.execute(self, dbinterface)
             
        action = self.myclass(self.logger, dbinterface)
        try:
            return action.execute()
        except:
            return False