from WMCore.WMBS.Actions.Action import BaseAction
from WMCore.WMBS.Actions.NewFileset import NewFilesetAction
from WMCore.WMBS.Actions.ListFileset import ListFilesetAction
class AddAndListFileset(BaseAction):
    name = "NewFileset"
        
    def execute(self, fileset = None, dbinterface = None):
        """
        import the approriate SQL object and execute it
        """ 
        action1 = NewFilesetAction(self.logger, dbinterface)
        action1.execute('my filesset')
        action2 = ListFilesetAction(self.logger, dbinterface)
        return action2.execute()
        