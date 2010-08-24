from WMCore.WMBS.Actions.Action import BaseAction
from WMCore.WMBS.Actions.NewFileset import NewFilesetAction
from WMCore.WMBS.Actions.ListFileset import ListFilesetAction
class AddAndListFileset(BaseAction):
    name = "NewFileset"
        
    def execute(self, fileset = None, dbinterface = None):
        action1 = NewFilesetAction(self.logger, dbinterface)
        action1.execute(fileset)
        action2 = ListFilesetAction(self.logger, dbinterface)
        return action2.execute()
        