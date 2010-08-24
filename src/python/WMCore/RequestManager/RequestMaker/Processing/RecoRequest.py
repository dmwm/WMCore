#!/usr/bin/env python
"""
_ReRecoRequest_


Dual input processing request.

ParentDataset/ChildDataset -> cmsRun -> outputDatasets

"""

from WMCore.RequestManager.RequestMaker.RequestMakerInterface import RequestMakerInterface
from WMCore.RequestManager.DataStructs.RequestSchema import RequestSchema
from WMCore.RequestManager.RequestMaker.Registry import registerRequestType
from WMCore.WMSpec.StdSpecs.ReReco import rerecoWorkload

class RecoRequest(RequestMakerInterface):
    """
    _ReRecoRequest_

    RequestMaker to two file input data processing requests and workflows

    """
    def __init__(self):
        RequestMakerInterface.__init__(self)

    def makeWorkload(self, schema):
       # FIXME
       return rerecoWorkload(schema['RequestName'], schema).data

        
class RecoSchema(RequestSchema):
    """
    _ReReco_

    Data Required for a standard cmsRun two file read processing request.

    """
    def __init__(self):
        RequestSchema.__init__(self)
        self.validateFields = [
            "CMSSWVersion",
            "Label",
            "GlobalTag",
            "AcquisitionEra",
            "InputDatasets",
            "DBSURL",
            "LFNCategory",
            "OutputTiers"
            ]
        self.optionalFields = [
            "SiteWhitelist",
            "SiteBlackList",
            "BlockWhitelist",
            "BlockBlacklist",
            "RunWhitelist",
            "RunBlacklist",
            "Scenario"
            ]

    def validate(self):
        RequestSchema.validate(self)
        assert(isinstance(self['OutputTiers'], list))
        for tier in self['OutputTiers']:
            assert(tier in ['RECO', 'AOD', 'ALCA'])


registerRequestType("Reco", RecoRequest, RecoSchema)


if __name__ == "__main__":
    schema = RecoSchema()
    schema["Label"] = "ReRe"
    schema["CMSSWVersion"] = "CMSSW_3_1_6"
    schema["GlobalTag"] = "31X_V1"
    schema["AcquisitionEra"] = "PromSeason09"
    schema["SiteWhitelist"] = ["T1_CERN", "T1_FNAL"]
    schema["RunBlacklist"] = [10, 11]
    schema["InputDatasets"] = "/MinimumBias/BeamCommissioning09-v1/RAW"
    schema["OutputDataset"] = "BeamCommissioning09/MinimumBias/RECO"
    schema["DBSURL"] = "http://cmsdbsprod.cern.ch/cms_dbs_prod_global/servlet/DBSServlet"
    schema["LFNCategory"] = "/store/data"
    schema['OutputTiers'] = ['RECO'] 

    request = RecoRequest()
    print str(request.makeWorkload(schema))
