from WMCore.WMBS.Actions.Action import BaseAction
from WMCore.DAOFactory import DAOFactory

class ChangeStateAction(BaseAction):
    """
    Change the state of some files in WMBS for a subscription. This will add a record to the 
    completed or failed table and remove it from the acquired table.
    """
    name = "Subscriptions.AddAndList"
       
    def execute(self, subscription=None, file=None, state = "CompleteFiles", daofactory = None):
        if state not in ["CompleteFiles", "FailFiles"]:
            raise Exception, "Unknown state transition"
                
        action1 = daofactory(classname='Subscriptions.%s' % state)
        action2 = daofactory(classname='Subscriptions.DeleteAcquiredFiles')
        dbconn = daofactory.dbinterface.engine.connect()
        try:
            trans = dbconn.begin()
            action1.execute(subscription=subscription, file=file, conn = dbconn, transaction = True)
            action2.execute(subscription=subscription, file=file, conn = dbconn, transaction = True)
            trans.commit()
            dbconn.close()
        except Exception, e:
            trans.rollback()
            dbconn.close()
            self.logger.error(e)
            raise e
        return True
    