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
        self.validateFields = [
            'InputDatasets',
            'CMSSWVersion',
            'ScramArch',
            'Group',
            'DbsUrl'
            ]


registerRequestType("StoreResults", StoreResultsRequest, StoreResultsSchema)


if __name__ == '__main__':
   requestSchema = StoreResultsSchema()
   requestSchema['InputDatasets'] = ['/PRIM/PROC/TIER']
   requestSchema['RequestType'] = 'StoreResults'
   requestSchema['OutputDataset'] = 'Output'
   requestScheme['DbsUrl'] = 'http://cmsdbsprod.cern.ch/cms_dbs_ph_analysis_02/servlet/DBSServlet'
   requestSchema['Requestor'] = 'Eric'
   requestSchema['Group'] = 'PeopleSimilarToEric'
   requestSchema['CMSSWVersion'] = 'CMSSW_3_6_2'
   requestSchema['ScramArch'] = 'slc5_ia32_gcc434'
   maker = retrieveRequestMaker(requestSchema['RequestType'])
   request = maker(requestSchema)
   requestSchema.validate()
   from WMCore.RequestManager.RequestMaker import CheckIn
   checkIn = CheckIn(request)
   checkIn()

