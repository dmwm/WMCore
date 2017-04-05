from __future__ import print_function, division
from WMCore.Database.DBFormatter import DBFormatter

class CheckStatusForCompletedWorkflows(DBFormatter):
    """
    _CheckStatusForCompletedWorkflow_

    Retrieve information about the workflow which checks the status of PhEDEx and DBS injection.
    There is case task is completed but no file is associated to it. (EmptyBlock flag) 
    Not sure how that happens
    """
    sql = """SELECT dw.name, df.in_phedex, df.status, count(*) as count
                FROM dbsbuffer_workflow dw  
                INNER JOIN wmbs_workflow ww on ww.name = dw.name
                LEFT JOIN dbsbuffer_file df on dw.id = df.workflow
             WHERE dw.completed = 1 group by dw.name, df.in_phedex, df.status """

    def format(self, results):
        results = self.formatDict(results)
        resultByWorkflow = {}
        for info in results:
            workflow = info['name']
            resultByWorkflow.setdefault(workflow, {})
            resultByWorkflow[workflow].setdefault("InDBS", 0)
            resultByWorkflow[workflow].setdefault("NotInDBS", 0)
            resultByWorkflow[workflow].setdefault("InPhEDEx", 0)
            resultByWorkflow[workflow].setdefault("NotInPhEDEx", 0)
            resultByWorkflow[workflow].setdefault("EmptyTasks", 0)

            count = info["count"]

            if info['status'] == "InDBS":
                resultByWorkflow[workflow]["InDBS"] += count
            elif info['status'] == "NOTUPLOADED":
                resultByWorkflow[workflow]["NotInDBS"] += count

            if info['status'] == "InDBS" or info['status'] == "NOTUPLOADED":
                if info['in_phedex'] == 1:
                    resultByWorkflow[workflow]["InPhEDEx"] += count
                elif info['in_phedex'] == 0:
                    resultByWorkflow[workflow]["NotInPhEDEx"] += count
                elif info['in_phedex'] is None:
                    resultByWorkflow[workflow]["EmptyTasks"] += count
            else:
                resultByWorkflow[workflow]["NoNeedToUpload"] += count

        return resultByWorkflow
            
        
    def execute(self, conn=None, transaction=False):
        """
        _execute_

        Retrieve information about a workflow status association in DBSBuffer.
        """
        results = self.dbi.processData(self.sql, conn = conn,
                                          transaction = transaction)

        return self.format(results)