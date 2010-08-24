from WMCore.WMBS.Actions.Action import BaseAction

class AddFileAction(BaseAction):
    name = "AddFile"
        
    def execute(self, files=None, size=0, events=0, run=0, lumi=0, dbinterface=None):
        """
        Add a (list of) new file(s) to WMBS
        """ 
        myclass = self.loadDialect(self.name, dbinterface)
        action = myclass(self.logger, dbinterface)
        try:
            return action.execute(files, size, events, run, lumi)
        except:
            return False