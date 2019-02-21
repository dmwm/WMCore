from __future__ import print_function, division

# TODO: need to get algorithm proper way
ALGO_DICT = {"FileBased": {"files_per_job": ''},
             "TwoFileBased": {"two_files_per_job": ''},
             "LumiBased": {"lumis_per_job": '',
                           "halt_job_on_file_boundaries": True},
             "EventAwareLumiBased": {"events_per_job": '',
                                     "job_time_limit": '',
                                     "halt_job_on_file_boundaries": True},
             "EventBased": {"events_per_job": '',
                            "events_per_lumi": ''},
             "Harvest": {"periodic_harvest_interval": ''},
             "ParentlessMergeBySize": {"min_merge_size": '',
                                       "max_merge_size": '',
                                       "max_merge_events": '',
                                       "max_wait_time": ''},
             "WMBSMergeBySize": {"min_merge_size": '',
                                 "max_merge_size": '',
                                 "max_merge_events": '',
                                 "max_wait_time": ''},
             "MergeBySize": {"min_merge_size": '',
                             "max_merge_size": '',
                             "max_merge_events": '',
                             "max_wait_time": ''}
             }

ALGO_LIST_BY_TYPES = {"Processing": ["LumiBased", "EventAwareLumiBased",
                                     "EventBased", "FileBased"],
                      "Production": ["EventBased"],
                      "Skim": ["FileBased", "TwoFileBased"],
                      "Harvesting": ["Harvest"],
                      "Merge": ["ParentlessMergeBySize",
                                "WMBSMergeBySize",
                                "MergeBySize"],
                      "Cleanup": ["FileBased"],
                      "LogCollect": ["FileBased"]
                      }

EDITABLE_SPLITTING_PARAM_CONFIG = {"algo_params": ALGO_DICT,
                                   "algo_list_by_types": ALGO_LIST_BY_TYPES
                                   }
