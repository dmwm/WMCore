from WMCore.WMBS.Actions.Action import BaseAction
class ListFilesetAction(BaseAction):
    name = "ListFileset"
        
    def execute(self, dbinterface = None):
        """
        import the approriate SQL object and execute it
        """ 
        BaseAction.execute(self, dbinterface)
        
        action = self.myclass(self.logger, dbinterface)
        return action.execute()