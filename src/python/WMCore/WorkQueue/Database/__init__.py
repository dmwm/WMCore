#!/usr/bin/env python
"""
_Database_

Implementations for the various database backends.

"""
__all__ = []



from WMCore.WorkQueue.DataStructs.WorkQueueElement import STATES

States = {}
for state, state_id in enumerate(STATES):
    States[state] = state_id
    # fill with index mapping for reverse lookup
    States[state_id] = state
