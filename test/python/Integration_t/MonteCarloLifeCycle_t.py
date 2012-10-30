from Integration_t.RequestLifeCycleBase_t import RequestLifeCycleBase_t

import unittest

class MonteCarloLifeCycle_t(unittest.TestCase, RequestLifeCycleBase_t):

    requestParams = {'CMSSWVersion' : 'CMSSW_4_4_2_patch2', "FilterEfficiency": 0.0361,
                        "RequestNumEvents" : 10000, "inputMode": "couchDB",
                        "RequestType": "MonteCarlo", "Requestor": 'integration',
                        "ConfigCacheID": "BdToMuMu_2MuPtFilter_7TeV-pythia6-evtgen-namedfilter",
                        "PrimaryDataset": 'BdToMuMu_2MuPtFilter_7TeV-pythia6-evtgen',
                        "GlobalTag": 'FT_R_44_V11::All', 'Group' : 'DMWM', "RequestPriority" : 10,
                        "FirstEvent" : 1, "FirstLumi" : 1
                        }

if __name__ == "__main__":
    unittest.main()
