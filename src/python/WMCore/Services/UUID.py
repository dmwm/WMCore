#!/usr/bin/env python

import WMCore.Algorithms.External.uuid75421 as uuid

def makeUUID():
    """
    _makeUUID_
    
    Makes a UUID from the uuid class, returns it
    """
    return str(uuid.uuid1())
