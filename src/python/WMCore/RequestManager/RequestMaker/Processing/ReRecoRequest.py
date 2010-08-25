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

class ReRecoRequest(RequestMakerInterface):
    """
    _ReRecoRequest_

    RequestMaker to two file input data processing requests and workflows

    """
    def __init__(self):
        RequestMakerInterface.__init__(self)

    def makeWorkload(self, schema):
       return rerecoWorkload(schema['RequestName'], schema).data

        
class ReRecoSchema(RequestSchema):
    """
    _ReReco_

    Data Required for a standard cmsRun two file read processing request.

    """
    def __init__(self):
        RequestSchema.__init__(self)
        self.validateFields = [
            "CMSSWVersion",
            "ScramArch",
            "Label",
            "GlobalTag",
            "AcquisitionEra",
            "InputDataset",
            "SkimInput",
            "CmsPath",
            "ProcessingVersion"
            ]
        self.optionalFields = [
            "SiteWhitelist",
            "SiteBlacklist",
            "BlockWhitelist",
            "BlockBlacklist",
            "RunWhitelist",
            "RunBlacklist",
            "Scenario",
            "ProcessingConfig",
            "SkimConfig",
            "CouchUrl",
            "CouchDBName",
            "DbsUrl",
            "UnmergedLFNBase",
            "MergedLFNBase",
            "MinMergeSize",
            "MaxMergeSize",
            "MaxMergeEvents"
            ]

    def validate(self):
        RequestSchema.validate(self)
        if self['InputDataset'].count('/') != 3:
            raise RuntimeError, "Need three slashes in InputDataset "+self['InputDataset']


registerRequestType("ReReco", ReRecoRequest, ReRecoSchema)


if __name__ == "__main__":
    schema = ReRecoSchema()
    schema["Label"] = "ReRe"
    schema["CMSSWVersion"] = "CMSSW_3_5_6"
    schema["ScramArch"] = "slc5_ia32_gcc434"
    schema["GlobalTag"] = "31X_V1"
    schema["AcquisitionEra"] = "PromSeason09"
    schema["SiwteWhitelist"] = ["T1_CERN", "T1_FNAL"]
    schema["RunBlacklist"] = [10, 11]
    schema["InputDataset"] = "/MinimumBias/BeamCommissioning09-v1/RAW"
    schema["SkimInput"] = "output"
    schema["DbsUrl"] = "http://cmsdbsprod.cern.ch/cms_dbs_prod_global/servlet/DBSServlet"
    schema["CmsPath"] = "/uscmst1/prod/sw/cms"
    schema["Scenario"] = "pp"
    schema["ProcessingConfig"] = "http://cmssw.cvs.cern.ch/cgi-bin/cmssw.cgi/CMSSW/Configuration/GlobalRuns/python/rereco_FirstCollisions_MinimumBias_35X.py?revision=1.8"
    schema["ProcessingVersion"] = "v0"
    schema["SkimConfig"] = ""
    schema["SkimConfig"] = "http://cmssw.cvs.cern.ch/cgi-bin/cmssw.cgi/CMSSW/Configuration/DataOps/python/prescaleskimmer.py?revision=1.1"
    request = ReRecoRequest()
    print str(request.makeWorkload(schema))
