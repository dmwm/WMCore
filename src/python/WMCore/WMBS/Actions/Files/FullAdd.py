from WMCore.WMBS.Actions.Action import BaseAction
from WMCore.DAOFactory import DAOFactory

class FullAddAction(BaseAction):
    """
    Change the state of some files in WMBS for a subscription. This will add a record to the 
    completed or failed table and remove it from the acquired table.
    """
    name = "Files.FullAdd"
       
    def execute(self, files=None, size=0, events=0, run=0, lumi=0, daofactory = None):                
        action1 = daofactory(classname='Files.Add')
        action2 = daofactory(classname='Files.AddRunLumi')
        dbconn = daofactory.dbinterface.engine.connect()
        try:
            trans = dbconn.begin()
            action1.execute(files=files, size=size, events=events, conn = dbconn, transaction = True)
            action2.execute(files=files, run=run, lumi=lumi, conn = dbconn, transaction = True)
            trans.commit()
            dbconn.close()
        except Exception, e:
            trans.rollback()
            dbconn.close()
            self.logger.error(e)
            raise e
        return True
    