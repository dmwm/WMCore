#!/usr/bin/env python

from builtins import str
import uuid


def makeUUID():
    """
    _makeUUID_

    Makes a UUID from the uuid class, returns it
    """
    return str(uuid.uuid4())
