
class NewWorkflow(object):
    sql = """insert into wmbs_workflow (spec, owner)
                values (:spec, :owner)"""
                
    def newWorkflow(self, spec=None, owner=None, 
                           conn = None, transaction = False):
        """
        Create a workflow ready for subscriptions
        """
        binds = {'spec':spec, 'owner':owner}
        return self.processData(self.insert['newworkflow'], binds, 
                             conn = conn, transaction = transaction)