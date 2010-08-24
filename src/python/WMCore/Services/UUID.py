#!/usr/bin/env python

import time
import random
import md5
import socket
import popen2

def uuid( *args ):
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



def uuidgen():
    """
    Try to create a uuid with uuidgen if available, returns None if not
    """
    pop = popen2.Popen4("uuidgen")
    pop.wait()
    exitCode = pop.poll()
    if exitCode:
        return None
    hash = pop.fromchild.read().strip()
    return hash


def makeUUID():
    """
    _makeUUID_

    return a UUID

    """
    guid = uuidgen()
    if guid == None:
        guid = uuid()
    return guid