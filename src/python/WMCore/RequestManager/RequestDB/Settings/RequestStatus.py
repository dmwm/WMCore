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
    "rejected",

    ]

NextStatus = {
 "new" : ["testing-approved", "assignment-approved", "rejected", "failed"],
 "testing-approved" : ["testing", "test-failed"],
 "testing" : ["tested", "test-failed"],
 "tested" : ["assignment-approved","failed", "rejected"],
 "test-failed" : ["testing-approved", "rejected"],
 "assignment-approved" : ["assigned", "rejected"],
 "assigned" : ["assigned", "assigned-prodmgr", "rejected", "failed"],
 "assigned-prodmgr" : ["running", "failed"],
 "running" : ["completed", "aborted", "failed", "epic-FAILED"], 
 "failed" : ["testing-approved", "assignment-approved", "assigned"],
 "epic-FAILED" : [],
 "completed" : [],
 "aborted" : ["testing-approved", "assignment-approved", "assigned", "rejected", "failed"],
 "rejected" : []
}

