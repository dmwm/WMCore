from __future__ import print_function, division

CAMPAIGN_CONFIG = \
{
    "Summer16Geant4102": {
        "go": True,
        "EventsPerLumi": "x10"
    },
    "HIRun2015": {
        "go": True,
        "labels" : ["02May2016" ,"25Aug2016"],
        "overflow" : {"PRIM" : {}},
        "DDMcopies": {
            "all" : { "N" : 2 }
        },
        "custodial_override" : ["DQMIO"],
        "fractionpass": 1.0,
        "lumisize" : -1,
        "maxcopies" : 1,
        "custodial": "T1_FR_CCIN2P3_MSS",

        "NonCustodialSites": ["T2_US_Vanderbilt"],
        "SiteBlacklist": [
            "T1_US_FNAL",
            "T2_US_Purdue",
            "T2_US_Caltech",
            "T2_US_Florida",
            "T2_US_Nebraska",
            "T2_US_UCSD",
            "T2_US_Wisconsin"
        ]
    },
    "TTI2023Upg14GS": {
        "go": True,
        "EventsPerLumi": 25
    },
    "TTI2023Upg14D": {
        "go": True,
        "SiteWhitelist": ["T1_US_FNAL", "T1_ES_PIC", "T1_FR_CCIN2P3", "T1_IT_CNAF", "T1_RU_JINR", "T1_UK_RAL",
                          "T1_DE_KIT", "T2_US_MIT", "T2_US_Nebraska", "T2_US_Caltech", "T2_CH_CERN"],
        "secondaries": {"/PYTHIA6_MinBias_TuneZ2star_14TeV/TTI2023Upg14GS-FLATBS15_DES23_62_V1-v1/GEN-SIM": {},
                        "/PYTHIA6_MinBias_TuneZ2star_14TeV/TTI2023Upg14GS-DES23_62_V1-v1/GEN-SIM": {},
                        "/MinBias_TuneZ2star_14TeV-pythia6/TTI2023Upg14-DES23_62_V1-v1/GEN-SIM": {},
                        "/PYTHIA6_Tauola_TTbar_TuneZ2star_14TeV/TTI2023Upg14GS-CoolingDefect_DES23_62_V1-v1/GEN-SIM": {}
                        }
    },

    "Run2016B": {
        "go": True,
        "labels": ["03Feb2017", "15Feb2017", "22Feb2017", "18Apr2017_ver2", "18Apr2017"],
        "overflow": {"PRIM": {}},
        "fractionpass": 1.0,
        "lumisize": -1,
        "maxcopies": 1
    },
    "Run2015C_25ns": {
        "go": True,
        "labels": ["24Nov2016", "16Jan2017", "19Jan2017"],
        "DDMcopies": {
            "all": {"N": 4},
            "RECO": {"N": 1, "host": ["T1_US_FNAL_Disk"]}
        },
        "custodial_override": ["DQMIO"],
        "fractionpass": {"all": 1.0, "AOD": 1.0, "MINIAOD": 1.0, "DQMIO": 1.0, "USER": 1.0, "RAW-RECO": 1.0},
        "lumisize": -1,
        "maxcopies": 1,
        "primary_AAA": False,
        "overflow": {"PRIM": {}}
    },

    "PhaseIISpring17D": {
        "go": True,
        "toDDM": ["GEN-SIM-DIGI-RAW"],
        "lumisize": 1500,
        "maxcopies": 1,
        "fractionpass": 0.95,
        "tune": True,
        "SiteBlacklist": ["T1_US_FNAL", "T0_CH_CERN", "T2_CH_CERN_HLT", "T2_CH_CERN"],
        "secondaries": {
            "/MinBias_TuneCUETP8M1_14TeV-pythia8/PhaseIIFall16GS82-90X_upgrade2023_realistic_v1-v1/GEN-SIM": {},
            "/MinBias_TuneCUETP8M1_14TeV-pythia8/PhaseIISpring17GS-90X_upgrade2023_realistic_v9-v1/GEN-SIM": {
                "SiteWhitelist": ["T1_DE_KIT", "T2_IT_Rome", "T1_UK_RAL", "T2_ES_CIEMAT", "T2_US_Nebraska",
                                  "T2_US_Caltech", "T2_US_Wisconsin"]}}
    },

# RelVal config

  "CMSSW_9_1_0_pre1__UPSG_VBF_PU200-1492881257": {

    "SiteWhitelist": ["T1_US_FNAL"],
    "MergedLFNBase": "/store/relval",
    "Team": "relval",
    "NonCustodialGroup": "RelVal",
    "maxcopies": 1,
    "custodial": "T1_US_FNAL_MSS",
    "lumisize": -1,
    "phedex_group": "RelVal",
    "fractionpass": 0.0,
    "go": True
  },
  "CMSSW_9_1_0_pre3__fullSimPU_premix-1493915792": {

    "SiteWhitelist": [
      "T1_US_FNAL"
    ],
    "MergedLFNBase": "/store/relval",
    "Team": "relval",
    "NonCustodialGroup": "RelVal",
    "maxcopies": 1,
    "custodial": "T1_US_FNAL_MSS",
    "lumisize": -1,
    "phedex_group": "RelVal",
    "fractionpass": 0.0,
    "go": True
  },
}
