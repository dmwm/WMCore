#!/usr/bin/env python
"""
_StatusList_

List of valid status values for a request
To be loaded into the DB on installation

"""

StatusList = [
    "new",
    "assignment-approved",
    "assigned",
    "acquired",
    "running",
    "running-open",
    "running-closed",
    "force-complete",
    "failed",
    "completed",
    "closed-out",
    "announced",
    "aborted",
    "aborted-completed",
    "rejected",
    "normal-archived",
    "aborted-archived",
    "rejected-archived"
    ]

NextStatus = {
 "new" : ["new", "testing-approved", "assignment-approved", "rejected"],
 "assignment-approved" : ["assignment-approved", "assigned", "rejected"],
 "assigned" : ["assigned","acquired", "aborted", "failed"],
 "acquired" : ["running-open", "completed", "acquired", "aborted"],
 "running" : ["completed", "aborted"],
 "running-open" : ["running-closed", "aborted"],
 "running-closed" : ["force-complete", "completed", "aborted"],
 "force-complete" : ["completed"],
 "failed" : ["failed", "assigned", "rejected"],
 "completed" : ["completed", "closed-out", "rejected"],
 "closed-out" : ["announced", "rejected"],
 "announced" : ["rejected", "normal-archived"],
 "aborted" : ["aborted-completed"],
 "rejected" : ["rejected-archived"],
 "aborted-completed": ["aborted-archived"],
 "normal-archived":["rejected-archived"],
 "aborted-archived":[],
 "rejected-archived":[]
}

ACTIVE_STATUS = ["new",
                 "assignment-approved",
                 "assigned",
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