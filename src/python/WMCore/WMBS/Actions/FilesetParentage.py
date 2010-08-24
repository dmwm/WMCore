from WMCore.WMBS.Actions.Action import BaseAction
from WMCore.WMBS.Fileset import Fileset

class FilesetParentageAction(BaseAction):
    name = "FilesetParentage"
    
    def execute(self, dbinterface = None, child=None, parent=None):
        """
        import the approriate SQL object and execute it
        """ 
        
        if  not isinstance(child, Fileset) and not isinstance(parent, Fileset):
            raise TypeError, "Parent or Child not a WMBS Fileset object"
        
        myclass = self.loadDialect(self.name, dbinterface)
        action = myclass(self.logger, dbinterface)
        try:
            return action.execute(child.id, parent.id)
        except:
            self.logger.exception(e)
            return False