#!/usr/bin/env python
"""
_BasicNaming_

Default name generator using a vaguely sensible convention.
Uses GUIDs to avoid having to keep state

"""
from WMCore.Services.UUIDLib import makeUUID
from WMCore.JobSplitting.Generators.GeneratorInterface import GeneratorInterface



class BasicNaming(GeneratorInterface):
    """
    _BasicNaming_

    Basic task & guid based name generator

    """

    def __call__(self, wmbsJob):
        wmbsJob['id'] = "%s/%s" % (self.task.getPathName(), makeUUID())
        wmbsJob['name'] = "%s/%s" % (self.task.getPathName(), makeUUID())
