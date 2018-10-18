from __future__ import print_function, division

UNIFIED_CONFIG = {
    "site_for_overflow": {
        "value": ["nono_T0_CH_CERN",
                  "T2_CH_CERN_HLT"],
        "description": "The sites that we set to overflow and require a specific treatment"
    },
    "overflow_pressure": {
        "value": 0.5,
        "description": "The ratio pending/running over which to consider overflowing"
    },
    "DDM_buffer_level": {
        "value": 0.8,
        "description": "The fraction of the DDM quota we are allowed to use"
    },
    "sites_banned": {
        "value": ["T2_CH_CERN_AI",
                  "NONO_T0_CH_CERN",
                  "T2_TH_CUNSTDA",
                  "NONO_T2_US_Vanderbilt",
                  "T2_EE_Estonia",
                  "T2_UA_KIPT"
                  ],
        "description": "The sites that are banned from production"
    },
    "sites_auto_approve": {
        "value": ["T0_CH_CERN_MSS", "T1_FR_CCIN2P3_MSS"],
        "description": "The sites we can autoapprove tape request to"
    },
    "sites_space_override": {
        "value": {
        },
        "description": "Over-ride the available space at a phedex end point"
    },
    "sites_with_goodIO": {
        "value": ["T2_DE_DESY", "T2_DE_RWTH", "T2_ES_CIEMAT", "T2_FR_GRIF_LLR", "T2_FR_GRIF_IRFU", "T2_FR_IPHC",
                  "T2_FR_CCIN2P3", "T2_IT_Bari", "T2_IT_Legnaro", "T2_IT_Pisa", "T2_IT_Rome", "T2_UK_London_Brunel",
                  "T2_UK_London_IC", "T2_US_Caltech", "T2_US_MIT", "T2_US_Nebraska", "T2_US_Purdue", "T2_US_UCSD",
                  "T2_US_Wisconsin", "T2_US_Florida", "T2_BE_IIHE", "T2_EE_Estonia", "T2_PL_Swierk", "T2_CH_CERN",
                  "T2_CH_CERN_HLT"],
        "description": "The sites identified as having good storage badnwidth"
    },
    "max_cpuh_block": {
        "value": 40000000,
        "description": "Value of CPUh above which a wf is blocked from assigning"
    },
    "block_repositionning": {
        "value": True,
        "description": "Whether or not to retransfer block from WQE without location"
    },
    "allowed_bypass": {
        "description": "Who is allowed to bypass and force complete",
        "value": [["vlimant", "vlimant@cern.ch"],
                  ["prozober", "paola.katherine.rozo.bernal@cern.ch"],
                  ["mcremone", "matteoc@fnal.gov"]]
    },
    "max_tail_priority": {
        "value": 5,
        "description": "Number of workflow to increase the priority of at a time"
    },
    "injection_delay_threshold": {
        "value": 50,
        "description": "Number of days after wich to increase the priority of a workflow"
    },
    "delay_priority_increase": {
        "value": 10000,
        "description": "Priority from original increase per week over the delay threshold"
    },
    "injection_delay_priority": {
        "value": 75000,
        "description": "Priority above which we can increase the priority of a workflow after running too long"
    },

    "max_force_complete": {
        "value": 10,
        "description": "Number of workflow that can be forced complete at a time"
    },
    "max_per_round": {
        "description": "limitation on the number of wf to process per module",
        "value": {
            "transferor": None,
            "assignor": None,
            "closor": None,
            "checkor": None,
            "completor": None
        }
    },
    "default_fraction_pass": {
        "value": 1.0,
        "description": "completion fraction above which to announce dataset"
    },
    "pattern_fraction_pass": {
        "value": {},
        "description": "overide of the completion fraction of dataset with keyword"
    },
    "tiers_with_no_custodial": {
        "value": ["DQM", "DQMIO", "RECO", "RAWAODSIM"],
        "description": "The data tiers that do not go to tape. Can be overidden by custodial overide at campaign level"
    },
    "use_parent_custodial": {
        "value": False,
        "description": "Use the location of the parent dataset for custodial copy"
    },
    "tape_size_limit": {
        "value": 200,
        "description": "Size over which to prevent transfer to tape automatically"
    },
    "tiers_with_no_check": {
        "value": ["DQM", "DQMIO"],
        "description": "The data tiers that do not pass closeout checks. Can be overidden by custodial overide at campaign level"
    },
    "tiers_no_DDM": {
        "value": ["GEN-SIM", "LHE", "GEN", "DQM", "DQMIO", "GEN-SIM-DIGI-RAW", "RAW"],
        "description": "The data tiers that do not go to AnaOps"
    },
    "tiers_to_DDM": {
        "value": ["AODSIM", "MINIAODSIM", "GEN-SIM-RAW", "GEN-SIM-RECO", "GEN-SIM-RECODEBUG", "AOD", "RECO", "MINIAOD",
                  "ALCARECO", "USER", "RAW-RECO", "RAWAODSIM"],
        "description": "The data tiers that go to AnaOps"
    },
    "tiers_keep_on_disk": {
        "value": ["LHE"],
        "description": "the data tier not unlocked until used again"
    },
    "check_fullcopy_to_announce": {
        "value": False,
        "description": "Whether to check for a full copy being present prior to announcing a dataset"
    },
    "stagor_sends_back": {
        "value": True,
        "description": "Whether the stagor module can send workflow back to considered"
    },
    "max_handled_workflows": {
        "value": 4000,
        "description": "The total number of workflows that we allow to handle at a time (transfer, running, assistance)"
    },
    "max_staging_workflows": {
        "value": 700,
        "description": "The total number of workflows that we allow to stage at a time"
    },
    "max_staging_workflows_per_site": {
        "value": 700,
        "description": "The total number of workflows that we allow to stage at a time per site"
    },
    "max_transfer_in_GB": {
        "value": 800000,
        "description": "The total size of the input datasets that can be transfered at a given time"
    },
    "transfer_timeout": {
        "value": 7,
        "description": "Time in days after which to consider a transfer to be stuck"
    },
    "transfer_lowrate": {
        "value": 0.004,
        "description": "Rate in GB/s under which to consider a transfer to be stuck, after transfer_timeout days"
    },
    "less_copies_than_requested": {
        "value": 1,
        "description": "Decrease the number of requested copies by that number, floored to 1"
    },
    "chopping_threshold_in_GB": {
        "value": 4000,
        "description": "The threshold before choping an input dataset in chunk of that size for spreading to sites"
    },
    "error_codes_to_recover": {
        "value": {"50664": [{"legend": "time-out",
                             "solution": "split-2",
                             "details": None,
                             "rate": 20
                             }],
                  "50660": [{"legend": "memory excess",
                             "solution": "mem-1000",
                             "details": None,
                             "rate": 20
                             }],
                  "61104": [{"legend": "failed submit",
                             "solution": "recover",
                             "details": None,
                             "rate": 20
                             }],
                  "8028": [{"legend": "read error",
                            "solution": "recover",
                            "details": None,
                            "rate": 20
                            }],
                  "8021": [{"legend": "cmssw failure",
                            "solution": "recover",
                            "details": "FileReadError",
                            "rate": 20
                            }],
                  "71305": [{"legend": "long pending",
                             "solution": "recover",
                             "details": None,
                             "rate": 20
                             }],
                  "8001": [{"legend": "lhe failure",
                            "solution": "split-4",
                            "details": "No lhe event found in ExternalLHEProducer::produce()",
                            "rate": 20
                            }]
                  },
        "description": "The error code, threshold and rules for auto-recovery"
    },
    "error_codes_to_block": {
        "value":
            {
                "99109": [{"legend": "stage-out",
                           "solution": "recover",
                           "details": None,
                           "rate": 20
                           }]
            },
        "description": "The error code, threshold and rules to prevent auto-recovery"
    },
    "error_codes_to_notify": {
        "value": {
            "8021": {"message": "Please take a look and come back to Ops."}
        },
        "description": "The error code, threshold and rules to notify the user of an error in production"
    },
    "user_rereco": {
        "description": "The users from which we expect ReReco requests",
        "value": ["cerminar", "fabozzi"]
    },
    "user_relval": {
        "description": "The users from which we expect relval requests",
        "value": ["fabozzi", "nwickram", "bsutar", "rverma", "prebello", "piperov", "sandhya"]
    },
    "relval_routing": {
        "description": "Set of keywords and special settings for relvals",
        "value": {"cc7": {"parameters": {"SiteWhitelist": ["T2_US_Nebraska"]}},
                  "highIOtobedecidedflag": {"parameters": {"SiteWhitelist": ["T2_US_Nebraska", "T2_US_Purdue"]}}
                  }
    },
    "batch_goodness": {
        "description": "Level below which to include a note in the batch report",
        "value": 90
    }
}
