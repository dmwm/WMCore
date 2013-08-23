"""
Definition of valid status values for a request and valid status transitions.

"""

# make this list to ensure insertion order here
REQUEST_START_STATE = "new"
REQUEST_STATE_TRANSITION = {
    REQUEST_START_STATE: [REQUEST_START_STATE,
            "testing-approved",
            "assignment-approved",
            "rejected",
            "failed",
            "aborted"],
    
    "testing-approved": ["testing-approved",
                         "testing",
                         "test-failed",
                         "aborted"],
                             
    "testing": ["testing",
                "tested",
                "test-failed",
                "aborted"],
                             
    "tested": ["tested",
               "assignment-approved",
               "failed",
               "rejected",
               "aborted"],
                             
    "test-failed": ["test-failed",
                    "testing-approved",
                    "rejected",
                    "aborted"],
    
    "assignment-approved": ["assignment-approved",
                            "assigned",
                            "rejected",
                            "aborted"],
                             
    "assigned": ["assigned",
                 "negotiating",
                 "acquired",
                 "aborted",
                 "rejected",
                 "failed"],
                             
    "negotiating": ["acquired",
                    "assigned",
                    "rejected",
                    "aborted",
                    "failed",
                    "negotiating"],
                             
    "acquired": ["running-open",
                 "failed",
                 "completed",
                 "acquired",
                 "aborted"],
                             
    "running": ["completed",
                "aborted",
                "failed"],
                             
    "running-open": ["running-closed",
                     "aborted",
                     "failed"],
                             
    "running-closed": ["completed",
                       "aborted",
                       "failed"],
                             
    "failed": ["failed",
               "testing-approved",
               "assigned"],
                             
    "completed": ["completed",
                  "closed-out",
                  "rejected"],
                             
    "closed-out": ["announced"],
    
    "announced": ["normal-archived"],
    
    "aborted": ["aborted",
                "testing-approved",
                "assigned",
                "rejected",
                "failed",
                "aborted-completed"],
                             
    "aborted-completed": ["aborted-archived"],
    
    "rejected": ["rejected",
                 "rejected-archived"],
                             
    # final status
    "normal-archived": [],
    
    "aborted-archived": [],
    
    "rejected-archived": []
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
    
    