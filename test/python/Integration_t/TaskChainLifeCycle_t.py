from Integration_t.RequestLifeCycleBase_t import RequestLifeCycleBase_t
import unittest

class TaskChainLifeCycle_t(unittest.TestCase, RequestLifeCycleBase_t):

    task1Config = {'TaskName' : 'DIGI', 'SplittingAlgorithm' : 'LumiBased',
                   'SplittingArguments' : {'lumis_per_job' : 8},
                   'InputDataset' : '/RelValSingleElectronPt10/CMSSW_6_0_0_pre3-START60_V0-v1/GEN-SIM',
                   'ConfigCacheID' : 'RelValSet_CMSSW_6_0_0_pre11_1_15.0DIGI'
    }

    task2Config = {'TaskName' : 'RECO', 'InputTask' : 'DIGI',
                   'SplittingAlgorithm' : 'LumiBased', 'SplittingArguments' : {'lumis_per_job' : 8},
                   'InputFromOutputModule' : 'FEVTDEBUGHLToutput',
                   'ConfigCacheID' : 'RelValSet_CMSSW_6_0_0_pre11_1_15.0RECO'



    }

    requestParams = {'CMSSWVersion' : 'CMSSW_4_4_2_patch2',
                        "Scenario": "pp", "inputMode" : "Scenario", "ProcScenario" : "pp",
                        "RequestType": "TaskChain", "TaskChain" : 2,
                        "Requestor": 'integration',
                        "GlobalTag": 'FT_R_44_V11::All', 'Group' : 'DMWM', "RequestPriority" : 10,
                        "Task1" : task1Config, "Task2" : task2Config,
                    }


if __name__ == "__main__":
    unittest.main()
