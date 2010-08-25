#!/usr/bin/env python
"""
_StoreResultsRequest_
"""

from WMCore.RequestManager.RequestMaker.RequestMakerInterface import RequestMakerInterface
from WMCore.RequestManager.DataStructs.RequestSchema import RequestSchema
from WMCore.RequestManager.RequestMaker.Registry import registerRequestType, retrieveRequestMaker


class StoreResultsRequest(RequestMakerInterface):
    """
    _StoreResultsRequest_

    RequestMaker to two file input data processing requests and workflows

    """
    def __init__(self):
        RequestMakerInterface.__init__(self)

        
class StoreResultsSchema(RequestSchema):
    """
    _StoreResults_

    Data Required for a standard cmsRun two file read processing request.

    """
    def __init__(self):
        RequestSchema.__init__(self)
        # not used yet
        self.validateFields = ['InputDatasets', 'FinalDestination']


registerRequestType("StoreResults", StoreResultsRequest, StoreResultsSchema)


if __name__ == '__main__':
   requestSchema = StoreResultsSchema()
   requestSchema['InputDatasets'] = ['somedbs:somedataset']
   requestSchema['FinalDestination'] = 'T2_Northern_South_Dakota'
   requestSchema['RequestType'] = 'StoreResults'
   requestSchema['OutputDataset'] = 'Output'
   requestSchema['Requestor'] = 'Eric'
   requestSchema['Group'] = 'PeopleSimilarToEric'
   requestSchema['CMSSWVersion'] = 'CMSSW_3_1_2'
   maker = retrieveRequestMaker(requestSchema['RequestType'])
   request = maker(requestSchema)
   requestSchema.validate()
   from WMCore.RequestManager.RequestMaker import CheckIn
   checkIn = CheckIn(request)
   checkIn()

