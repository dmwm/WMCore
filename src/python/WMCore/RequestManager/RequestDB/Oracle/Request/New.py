#!/usr/bin/env python
"""
_Request.New_

API for creating a new request

"""



from WMCore.Database.DBFormatter import DBFormatter

class New(DBFormatter):
    """

    _New_

    Create a new request

    request is a dictionary of parameters, required are:

    - request_name
    - request_type
    - request_status
    - association_id
    - workflow
    - priority

    """
    def execute(self, conn = None, trans = None, **request):
        """
        _execute_

        Insert a new request into the database

        """
        reqName = request.get('request_name', None)
        if reqName == None:
            msg = "request_name not provided to Request.New.execute"
            raise RuntimeError, msg
        reqType = request.get('request_type', None)
        if reqType == None:
            msg = "request_type not provided to Request.New.execute"
            raise RuntimeError, msg
        reqStatus = request.get('request_status', None)
        if reqStatus == None:
            msg = "request_status not provided to Request.New.execute"
            raise RuntimeError, msg

        requestor = request.get("association_id", None)
        if requestor == None:
            msg = "association_id not provided to Request.New.execute"
            raise RuntimeError, msg
        workflow = request.get("workflow", None)
        if workflow == None:
            msg = "workflow not provided to Request.New.execute"
            raise RuntimeError, msg

        prep_id = request.get("prep_id", None)
        priority = request.get("requestPriority", None)
        priority = priority if priority else 0

        self.sql = """
        INSERT INTO reqmgr_request (request_name, request_type,
                                    request_status, request_priority,
                                    requestor_group_id, workflow, prep_id)

          VALUES (:req_name, :req_type, :req_status, :priority, :requestor,
                  :workflow, :prep_id)"""
        binds = {"req_name": reqName, "req_type": reqType, "req_status": reqStatus,
                 "priority": priority, "requestor": requestor, "workflow": workflow,
                 "prep_id": prep_id}
        result = self.dbi.processData(self.sql, binds, conn = conn, transaction = trans)

        reqIdOut = self.dbi.processData("select last_number from user_sequences where sequence_name='REQMGR_REQUEST_SEQ'",
                                        conn = conn,
                                        transaction = trans)
        result = self.formatOne(reqIdOut)

        if result == []:
            return None
        return result[0]
