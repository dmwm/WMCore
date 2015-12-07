from Integration_t.RequestLifeCycleBase_t import RequestLifeCycleBase_t
import unittest

class ReRecoLifeCycle_t(unittest.TestCase, RequestLifeCycleBase_t):

    requestParams = {'CMSSWVersion' : 'CMSSW_4_4_2_patch2',
                        "Scenario": "pp", "inputMode" : "Scenario", "ProcScenario" : "pp",
                        "RequestType": "ReReco",
                        "Requestor": 'integration',
                        "InputDataset": "/BTag/Run2011B-v1/RAW", "RunWhitelist" : [177316, 177317],
                        "GlobalTag": 'FT_R_44_V11::All', 'Group' : 'DMWM', "RequestPriority" : 10,
                    }


if __name__ == "__main__":
    unittest.main()
