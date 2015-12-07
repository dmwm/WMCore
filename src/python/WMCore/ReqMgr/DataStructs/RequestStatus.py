"""
Definition of valid status values for a request and valid status transitions.

"""

# make this list to ensure insertion order here
REQUEST_START_STATE = "new"
REQUEST_STATE_TRANSITION = {
    REQUEST_START_STATE: [REQUEST_START_STATE,
            "assignment-approved",
            "rejected"],
    
    "assignment-approved": ["assigned", #manual transition
                            "rejected"], #manual transition
                             
    "assigned": ["negotiating",
                 "acquired",
                 "aborted", # manual transition
                 "failed"],
                             
    "negotiating": ["acquired",
                    "assigned",
                    "aborted",
                    "failed"],
                             
    "acquired": ["running-open",
                 "completed",
                 "acquired",
                 "aborted"],
                             
    "running": ["completed",
                "aborted", # manual transition
                "failed"],
                             
    "running-open": ["running-closed",
                     "aborted", # manual transition
                     "failed"],
                             
    "running-closed": ["force-complete", # manual transition
                       "completed",
                       "aborted", # manual transition
                       "failed"],
    
    "force-complete" : ["completed"],
                             
    "failed": ["rejected",  # manual transition
               "assigned"], # manual transition
                             
    "completed": ["completed",
                  "closed-out",
                  "rejected"], # manual transition
                             
    "closed-out": ["announced", "rejected"], # manual transition
    
    "announced": ["rejected", "normal-archived"],
    
    "aborted": ["aborted-completed"],
                             
    "aborted-completed": ["aborted-archived"],
    
    "rejected": ["rejected-archived"],
                             
    # final status
    "normal-archived": ["rejected-archived"],
    
    "aborted-archived": [],
    
    "rejected-archived": []
    }

ACTIVE_STATUS = ["new",
                 "assignment-approved",
                 "assigned",
                 "negotiating",
                 "acquired",
                 "running",
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

# if the state is not defined here (new, assignment-approved) allows all the property to get 
ALLOWED_ACTIONS_FOR_STATUS = {
                 "assigned": ["RequestPriority"],
                 "negotiating": ["RequestPriority"],
                 "acquired": ["RequestPriority"],
                 "running": ["RequestPriority"],
                 "running-open": ["RequestPriority"],
                 "running-closed": ["RequestPriority"],
                 "failed": ["RequestPriority"],
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

# each item from STATUS_TRANSITION is a dictionary with 1 item, the key
# is name of the status
REQUEST_STATE_LIST = REQUEST_STATE_TRANSITION.keys()

def check_allowed_transition(preState, postState):
    stateList = REQUEST_STATE_TRANSITION.get(preState, [])
    if postState in stateList:
        return True
    else:
        return False
    
def get_modifiable_properties(status = None):
    """
    returns mondifiable property list by status.
    if status is not defined return dictionarly of all the status and property list
    TODO: Currently gets the result from hardcoded list. change to get from configuration or db
    """
    if status:
        return ALLOWED_ACTIONS_FOR_STATUS.get(status, None)
    else:
        return ALLOWED_ACTIONS_FOR_STATUS
    
    
