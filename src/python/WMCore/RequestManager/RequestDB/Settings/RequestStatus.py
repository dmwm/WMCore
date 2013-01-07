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
    "failed",
    "epic-FAILED",
    "completed",
    "closed-out",
    "announced",
    "aborted",
    "rejected"
    ]

NextStatus = {
 "new" : ["new", "testing-approved", "assignment-approved", "rejected", "failed", "aborted"],
 "testing-approved" : ["testing-approved", "testing", "test-failed", "aborted"],
 "testing" : ["testing", "tested", "test-failed", "aborted"],
 "tested" : ["tested", "assignment-approved","failed", "rejected", "aborted"],
 "test-failed" : ["test-failed", "testing-approved", "rejected", "aborted"],
 "assignment-approved" : ["assignment-approved", "assigned", "rejected", "aborted"],
 "assigned" : ["assigned", "negotiating", "acquired", "aborted", "rejected", "failed"],
 "negotiating" : ["acquired", "assigned", "rejected", "aborted", "failed", "negotiating"],
 "acquired" : ["running-open", "failed", "completed", "acquired", "aborted"],
 "running" : ["completed", "aborted", "failed", "epic-FAILED"],
 "running-open" : ["running-closed", "aborted", "failed", "epic-FAILED"],
 "running-closed" : ["completed", "aborted", "failed", "epic-FAILED"],
 "failed" : ["failed", "testing-approved", "assigned"],
 "epic-FAILED" : ["epic-FAILED"],
 "completed" : ["completed", "closed-out"],
 "closed-out" : ["announced"],
 "announced" : [],
 "aborted" : ["aborted", "testing-approved", "assigned", "rejected", "failed"],
 "rejected" : ["rejected"]
}
