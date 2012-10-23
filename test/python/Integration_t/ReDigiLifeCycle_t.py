from Integration_t.RequestLifeCycleBase_t import RequestLifeCycleBase_t
import unittest

class ReDigiLifeCycle_t(unittest.TestCase, RequestLifeCycleBase_t):

    requestParams = {'CMSSWVersion' : 'CMSSW_4_4_2_patch2',
                        "Scenario": "pp", "inputMode" : "couchDB", "ProcScenario" : "pp",
                        "RequestType": "ReDigi",
                        "Requestor": 'integration',
                        "InputDataset": "/BTag/Run2011B-v1/RAW", "RunWhitelist" : [177316, 177317],
                        "MCPileup": '/MinBias_TuneZ2_7TeV-pythia6/Summer11-START311_V2-v2/GEN-SIM',
                        "GlobalTag": 'FT_R_44_V11::All', 'Group' : 'DMWM', "RequestPriority" : 10,
                        "StepOneConfigCacheID" : "step1_Summer11_R1_namedfilter",
                        "inputMode": "couchDB",
                    }


if __name__ == "__main__":
    unittest.main()
