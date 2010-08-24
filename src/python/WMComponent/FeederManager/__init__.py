#!/usr/bin/env python
"""
_FeederManager_

    A FeederManager associates filesets of interest with feeders (DBS, PhEDEx 
etc.) and calls the feeders to populate the WMBS with new/updated files. 
  
    Also provide some method for callbacks to feeders such that the feeders can 
persist state. This will need to know data type and appropriate mappings - 
maybe persist to config type file instead of a database, as the state will be 
complex and varied.

    The FeederManager may need to evaluate reg exp's in fileset names... tbd
"""
__all__ = []

