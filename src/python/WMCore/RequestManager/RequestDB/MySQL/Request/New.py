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



        priority = request.get("request_priority", 0)

        self.sql = """
        INSERT INTO reqmgr_request (request_name, request_type,
                                    request_status, request_priority,
                                    requestor_group_id, workflow)

          VALUES (\'%s\', \'%s\', %s, %s, %s, \'%s\')
        """ % (reqName, reqType, reqStatus, priority, requestor, workflow)
        result = self.dbi.processData(self.sql,
                                      conn = conn, transaction = trans)


        reqIdOut = self.dbi.processData("SELECT LAST_INSERT_ID()",
                                        conn = conn,
                                        transaction = trans)
        result = self.formatOne(reqIdOut)
        if result == []:
            return None
        return result[0]






