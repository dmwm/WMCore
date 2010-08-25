#!/usr/bin/env python
"""
_RequestMakerInterface_


Interface Definition for a RequestMaker implementation.

A RequestMaker Implementation should do the following.

- Instantiate with no args via a factory method
- Be callable on a RequestMakerSchema implementation
- Build WorkflowSpec instances
- Embed each WorkflowSpec in a RequestSpec instance
- Return a list of RequestSpec instances generated from the schema

"""


from WMCore.RequestManager.DataStructs.Request import Request
#import WMCore.RequestManager.RequestMaker.WorkloadMaker as WorkloadMaker
from WMCore.WMSpec.WMWorkload import newWorkload
import time


class RequestMakerInterface:
    """
    _RequestMakerInterface_

    API Definition for RequestMaker implementations

    """
    def __init__(self):
        self.requestType = None
        self.schemaClass = None

    def newSchema(self):
        """
        _newSchema_

        Retrieve a new Schema Instance for this request type

        """
        newSchema = self.schemaClass()
        newSchema['RequestType'] = self.requestType
        return newSchema

    def newRequest(self):
        """
        _newRequest_

        Util method to return a new ReqMgr.DataStructs.Request instance

        """
        return Request()


    def __call__(self, schema):
        """
        _operator(schema)_

        Create a request from the schema provided

        """
        request = self.newRequest()
        request.update(schema)
        workload = self.makeWorkload(schema)
        self.loadRequestSchema(workload, schema)
        #WorkloadMaker.loadRequestSchema(workload, schema) 
        request['WorkflowSpec'] = workload
        request['SoftwareVersions'].append(schema['CMSSWVersion'])
        return request

    def makeWorkload(self, schema):
        workload = newWorkload(schema['RequestName']).data
        return workload

    def loadRequestSchema(self, workload, requestSchema):
        schema = workload.request.section_('schema')
        for key, value in requestSchema.iteritems():
            try:
                setattr(schema, key, value)
            except Exception, ex:
                continue
        schema.timeStamp = int(time.time())
        schema = workload.request.schema

        # might belong in another method to apply existing schema
        workload.owner.Group = schema.Group
        workload.owner.Requestor = schema.Requestor
        if hasattr(schema, 'RequestPriority'):
            workload.request.priority = schema.RequestPriority

