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
    "assigned-prodmgr",
    "running",
    "failed",
    "epic-FAILED",
    "completed",
    "aborted",
    "rejected"
    ]

NextStatus = {
 "new" : ["new", "testing-approved", "assignment-approved", "rejected", "failed"],
 "testing-approved" : ["testing-approved", "testing", "test-failed"],
 "testing" : ["testing", "tested", "test-failed"],
 "tested" : ["tested", "assignment-approved","failed", "rejected"],
 "test-failed" : ["test-failed", "testing-approved", "rejected"],
 "assignment-approved" : ["assignment-approved", "assigned", "rejected"],
 "assigned" : ["assigned", "assigned-prodmgr", "rejected", "failed"],
 "assigned-prodmgr" : ["assigned-prodmgr", "running", "failed", "completed"],
 "running" : ["running", "completed", "aborted", "failed", "epic-FAILED"], 
 "failed" : ["failed", "testing-approved", "assignment-approved", "assigned"],
 "epic-FAILED" : ["epic-FAILED"],
 "completed" : ["completed"],
 "aborted" : ["aborted", "testing-approved", "assignment-approved", "assigned", "rejected", "failed"],
 "rejected" : ["rejected"]
}

