from WMCore.WMBS.Actions.Action import BaseAction
from WMCore.WMBS.Fileset import Fileset

class FilesetParentageAction(BaseAction):
    name = "FilesetParentage"
    
    def execute(self, dbinterface = None, child=None, parent=None):
        """
        import the approriate SQL object and execute it
        """ 
        BaseAction.execute(dbinterface)
        if  not isinstance(child, Fileset) and not isinstance(parent, Fileset):
            raise TypeError, "Parent or Child not a WMBS Fileset object"
        
        action = self.myclass(self.logger, dbinterface)
        return action.execute(child.id, parent.id)