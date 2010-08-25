#!/usr/bin/env python
"""
_Database_

Implementations for the various database backends.

"""
__all__ = []
__revision__ = "$Id: __init__.py,v 1.4 2009/12/02 13:52:44 swakef Exp $"
__version__ = "$Revision: 1.4 $"

from WMCore.WorkQueue.DataStructs.WorkQueueElement import STATES

States = {}
for state, state_id in enumerate(STATES):
    States[state] = state_id
    # fill with index mapping for reverse lookup
    States[state_id] = state
