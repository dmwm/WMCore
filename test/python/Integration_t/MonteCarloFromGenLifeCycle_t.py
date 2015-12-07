from Integration_t.RequestLifeCycleBase_t import RequestLifeCycleBase_t

import unittest

class MonteCarloFromGenLifeCycle_t(unittest.TestCase, RequestLifeCycleBase_t):

    requestParams = {'CMSSWVersion' : 'CMSSW_4_4_2_patch2', "FilterEfficiency": 0.0361,
                        "RequestNumEvents" : 10000, "inputMode": "couchDB",
                        "InputDataset": "/QCD_HT-1000ToInf_TuneZ2star_8TeV-madgraph-pythia6/Summer12-START50_V13-v1/GEN",
                        "BlockWhitelist": ['/QCD_HT-1000ToInf_TuneZ2star_8TeV-madgraph-pythia6/Summer12-START50_V13-v1/GEN#91230c68-3dbc-4894-a35e-05750105a4aa'],
                        "RequestType": "MonteCarloFromGEN", "Requestor": 'integration',
                        "ConfigCacheID": "QCD_HT-1000ToInf_TuneZ2star_8TeV-madgraph-pythia6",
                        "GlobalTag": 'FT_R_44_V11::All', 'Group' : 'DMWM', "RequestPriority" : 10,
                        "FirstEvent" : 1, "FirstLumi" : 1, "FilterEfficiency": 0.28,
                        }

if __name__ == "__main__":
    unittest.main()
