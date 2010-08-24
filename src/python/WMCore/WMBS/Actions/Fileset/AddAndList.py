from WMCore.WMBS.Actions.Action import BaseAction
from WMCore.WMBS.Actions.Fileset.New import NewFilesetAction
from WMCore.WMBS.Actions.Fileset.List import ListFilesetAction
class AddAndListFilesetAction(BaseAction):
    name = "Fileset.AddAndList"
        
    def execute(self, fileset = None, dbinterface = None):
        action1 = NewFilesetAction(self.logger)
        action1.execute(fileset, dbinterface)
        action2 = ListFilesetAction(self.logger)
        return action2.execute(dbinterface)
        