"""
Definition of valid status values for a request and valid status transitions.

"""

from future.utils import viewitems

# make this list to ensure insertion order here
REQUEST_START_STATE = "new"
REQUEST_STATE_TRANSITION = {
    REQUEST_START_STATE: [REQUEST_START_STATE,
                          "assignment-approved",
                          "rejected"],

    "assignment-approved": ["assigned",  # manual transition
                            "rejected"],  # manual transition

    "assigned": ["staging",
                 "aborted"],  # manual transition
    "staging": ["staged",
                "aborted"],  # manual transition
    "staged": ["acquired",
               "aborted",  # manual transition
               "failed"],
    "acquired": ["running-open",
                 "aborted",  # manual transition
                 "failed"],
    "running-open": ["running-closed",
                     "force-complete",  # manual transition
                     "aborted"],  # manual transition

    "running-closed": ["force-complete",  # manual transition
                       "completed",
                       "aborted"],  # manual transition

    "force-complete": ["completed"],

    "failed": ["rejected",  # manual transition
               "assigned"],  # manual transition

    "completed": ["closed-out",
                  "rejected"],  # manual transition

    "closed-out": ["announced", "rejected"],  # manual transition

    "announced": ["rejected", "normal-archived"],

    "aborted": ["aborted-completed"],

    "aborted-completed": ["aborted-archived"],

    "rejected": ["rejected-archived"],

    # final status
    "normal-archived": ["rejected-archived"],

    "aborted-archived": [],

    "rejected-archived": []
}

# List of requests that a human user can change a request to
REQUEST_HUMAN_STATES = ["assignment-approved",
                        "assigned",
                        "force-complete",
                        "closed-out",
                        "announced",
                        "rejected",
                        "aborted"]

ACTIVE_STATUS = ["new",
                 "assignment-approved",
                 "assigned",
                 "staging",
                 "staged",
                 "acquired",
                 "running-open",
                 "running-closed",
                 "failed",
                 "force-complete",
                 "completed",
                 "closed-out",
                 "announced",
                 "aborted",
                 "aborted-completed",
                 "rejected"]

### WMSTATS_JOB_INFO + WMSTATS_NO_JOB_INFO is meant to be equal to ACTIVE_STATUS
WMSTATS_JOB_INFO = ["running-open",
                    "running-closed",
                    "force-complete",
                    "completed",
                    "closed-out"]

WMSTATS_NO_JOB_INFO = ["new",
                       "assignment-approved",
                       "assigned",
                       "staging",
                       "staged",
                       "acquired",
                       "failed",
                       "announced",
                       "aborted",
                       "aborted-completed",
                       "rejected"]

### Used for monitoring in T0-WMStats. See: Services/WMStats/WMStatsReader
T0_ACTIVE_STATUS = ["new",
                    "Closed",
                    "Merge",
                    "Harvesting",
                    "Processing Done",
                    "AlcaSkim",
                    "completed"]

# if the state is not defined here (new) allows all the property to get
# states in the key is the source states (need to define source states instead of destination states for GUI update)
ALLOWED_ACTIONS_FOR_STATUS = {
    "new": ["RequestPriority"],
    "assignment-approved": ["RequestPriority", "Team", "SiteWhitelist", "SiteBlacklist",
                            "AcquisitionEra", "ProcessingString", "ProcessingVersion",
                            "Dashboard", "MergedLFNBase", "TrustSitelists",
                            "UnmergedLFNBase", "MinMergeSize", "MaxMergeSize",
                            "MaxMergeEvents", "BlockCloseMaxWaitTime",
                            "BlockCloseMaxFiles", "BlockCloseMaxEvents", "BlockCloseMaxSize",
                            "SoftTimeout", "GracePeriod",
                            "TrustPUSitelists", "CustodialSites",
                            "NonCustodialSites", "Override",
                            "SubscriptionPriority"],
    "assigned": ["RequestPriority"],
    "staging": ["RequestPriority", "SiteWhitelist", "SiteBlacklist"],
    "staged": ["RequestPriority"],
    "acquired": ["RequestPriority", "SiteWhitelist", "SiteBlacklist"],
    "running-open": ["RequestPriority", "SiteWhitelist", "SiteBlacklist"],
    "running-closed": ["RequestPriority"],
    "failed": [],
    "force-complete": [],
    "completed": [],
    "closed-out": [],
    "announced": [],
    "aborted": [],
    "aborted-completed": [],
    "rejected": [],
    "normal-archived": [],
    "aborted-archived": [],
    "rejected-archived": [],
}

# NOTE: We need to Explicitly add RequestStatus to reqArgsDiff, since it is
#       missing from the ALLOWED_ACTIONS_FOR_STATUS mapping. The alternative
#       would be to add it as allowed action to every state.
#       The same applies to few more, such as all the keys needed for the
#       workqueue_stat_validation() calls, but we do this only during
#       request parameters validation.
ALLOWED_ACTIONS_ALL_STATUS = ["RequestStatus"]

# NOTE: We need to explicitly add all stat keys during validation
#       They are needed for the workqueue_stat_validation() calls
ALLOWED_STAT_KEYS = ['total_jobs', 'input_lumis', 'input_events', 'input_num_files']

# Workflow state transition automatically controlled by ReqMgr2
### NOTE: order of this list matters and it's used for status transition
AUTO_TRANSITION = {"staged": ["acquired", "running-open", "running-closed", "completed"],
                   "acquired": ["running-open", "running-closed", "completed"],
                   "running-open": ["running-closed", "completed"],
                   "running-closed": ["completed"]}

# Workflow state transition automatically controlled by ReqMgr2
# Specific to workflows either aborted or force-completed
CANCEL_AUTO_TRANSITION = {"aborted": "aborted-completed",
                          "force-complete": "completed"}

# list of destination states which doesn't allow any additional argument update
STATES_ALLOW_ONLY_STATE_TRANSITION = [key for key, val in viewitems(ALLOWED_ACTIONS_FOR_STATUS) if len(val) == 0]
# each item from STATUS_TRANSITION is a dictionary with 1 item, the key
# is name of the status
REQUEST_STATE_LIST = list(REQUEST_STATE_TRANSITION)

ACTIVE_STATUS_FILTER = {"RequestStatus": ['assignment-approved', 'assigned', 'staging', 'staged',
                                          'failed', 'acquired', 'running-open', 'running-closed',
                                          'force-complete', 'completed', 'closed-out']}


def check_allowed_transition(preState, postState):
    stateList = REQUEST_STATE_TRANSITION.get(preState, [])
    if postState in stateList:
        return True
    else:
        return False


def get_modifiable_properties(status=None):
    """
    returns mondifiable property list by status.
    if status is not defined return dictionarly of all the status and property list
    TODO: Currently gets the result from hardcoded list. change to get from configuration or db
    """
    if status:
        allowedKeys = ALLOWED_ACTIONS_FOR_STATUS.get(status, 'all_attributes')
        return allowedKeys
    else:
        return ALLOWED_ACTIONS_FOR_STATUS


def get_protected_properties():
    """
    returns properties never be modified once request is created
    """
    return ["RequestName", "_id"]
