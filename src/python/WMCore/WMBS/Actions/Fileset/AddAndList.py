from WMCore.WMBS.Actions.Action import BaseAction
from WMCore.DAOFactory import DAOFactory

class AddAndListFilesetAction(BaseAction):
    name = "Fileset.AddAndList"
       
    def execute(self, fileset = None, dbinterface = None):
        conn = dbinterface.connect()
        daofactory = DAOFactory(package='WMCore.WMBS', logger=self.logger, dbinterface=conn) 
        
        action1 = daofactory(classname='Fileset.New')
        action1.execute(fileset)
        action2 = daofactory(classname='Fileset.List')
        return action2.execute()
        