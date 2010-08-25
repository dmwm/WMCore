#!/usr/bin/env python
"""
_WorkflowManager_

The WorkflowManager automatically creates subscriptions associated with a 
given Workflow when Filesets matching a regular expression (applied to the
Fileset name) become available.

To perform the management, the following messages are available, which expect
the described payload information. In all cases the payload should be pickled:

import pickle
payload = pickle.dumps({"Arg1" : Val, "Arg2" : "StringVal"})

AddWorkflowToManage - Used to add a Fileset name : Subscription creation mapping
   FilesetMatch : string (regex of fileset names to match)
   WorkflowId : string (ID from WMBS database of workflow to apply to new subs)
   SplitAlgo : string (as passed to Subscription constructor)
   Type : string (as passed to Subscription constructor)

RemoveWorkflowFromManagement - Used to add a Fileset name : Subscription
                               creation mapping
   FilesetMatch : string (regex of fileset names to match)
   WorkflowId : string (ID from WMBS database of workflow to apply to new subs)
   
AddToWorkflowManagementLocationList - Adds locations to the white / black list
                                      of created subscriptions
    FilesetMatch : string (as passed to AddWorkflowToManage)
    WorkflowId : string (as passed to AddWorkflowToManage)
    Locations : string (comma separated list of locations to add to whitelist)
    Valid : bool (are these locations for whitelist (True) or blacklist (False))
    
RemoveFromWorkflowManagementLocationList - Removes locations from the white /
                                           blacklist of created subscriptions
    FilesetMatch : string (as passed to AddWorkflowToManage)
    WorkflowId : string (as passed to AddWorkflowToManage)
    Locations : string (comma separated list of locations to remove from
                        whitelist)

Note there is one potential concurrency problem. If a managment request is made,
and filesets become available before messages to handle location white / black
listing are processed, some jobs may be created before the setup is fully
complete.
"""

__all__ = []


