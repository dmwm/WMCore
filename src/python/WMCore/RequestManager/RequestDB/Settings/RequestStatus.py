#!/usr/bin/env python
"""
_StatusList_

List of valid status values for a request
To be loaded into the DB on installation

"""

StatusList = [
    "new",
    "testing-approved",
    "testing",
    "tested",
    "test-failed",
    "assignment-approved",
    "assigned",
    "negotiating",
    "acquired",
    "running",
    "running-open",
    "running-closed",
    "force-complete",
    "failed",
    "epic-FAILED",
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
 "testing-approved" : ["testing-approved", "testing", "test-failed", "aborted"],
 "testing" : ["testing", "tested", "test-failed", "aborted"],
 "tested" : ["tested", "assignment-approved","failed", "rejected", "aborted"],
 "test-failed" : ["test-failed", "testing-approved", "rejected", "aborted"],
 "assignment-approved" : ["assignment-approved", "assigned", "rejected"],
 "assigned" : ["assigned", "negotiating", "acquired", "aborted", "failed"],
 "negotiating" : ["acquired", "assigned", "aborted", "failed", "negotiating"],
 "acquired" : ["running-open", "failed", "completed", "acquired", "aborted"],
 "running" : ["completed", "aborted", "failed", "epic-FAILED"],
 "running-open" : ["running-closed", "aborted", "failed", "epic-FAILED"],
 "running-closed" : ["force-complete", "completed", "aborted", "failed", "epic-FAILED"],
 "force-complete" : ["completed"],
 "failed" : ["failed", "testing-approved", "assigned", "rejected"],
 "epic-FAILED" : ["epic-FAILED"],
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