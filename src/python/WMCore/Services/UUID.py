#!/usr/bin/env python

import time
import random
import md5
import socket
import popen2

import WMCore.Algorithms.External.uuid75421 as uuid

def ourUUID( *args ):
    """
    Generates a universally unique ID.
    Any arguments only create more randomness.
    """
    t = long( time.time() * 1000 )
    r = long( random.random()*100000000000000000L )
    try:
        a = socket.gethostbyname( socket.gethostname() )
    except:
        # if we can't get a network address, just imagine one
        a = random.random()*100000000000000000L
    data = str(t)+' '+str(r)+' '+str(a)+' '+str(args)
    data = md5.md5(data).hexdigest()
    return data


def makeUUID():
    """
    _makeUUID_
    
    Makes a UUID from the uuid class, returns it
    """

    guid = uuid.uuid1()
    if not guid:
        guid = ourUUID()
    return str(guid)
    
