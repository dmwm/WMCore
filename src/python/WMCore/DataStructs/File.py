#!/usr/bin/env python
"""
_File_

Data object that contains details for a single file

"""
__all__ = []


import datetime
from WMCore.DataStructs.WMObject import WMObject
from WMCore.DataStructs.Run import Run

class File(WMObject, dict):
    """
    _File_
    Data object that contains details for a single file
    """
    def __init__(self, lfn = "", size = 0, events = 0, checksums = {}, 
                 parents = None, locations = None, merged = False):
        dict.__init__(self)
        self.setdefault("lfn", lfn)
        self.setdefault("size", size)
        self.setdefault("events", events)
        self.setdefault("checksums", checksums)
        self.setdefault('runs', set())
        self.setdefault('merged', merged)

        if locations == None:
            self.setdefault("locations", set())
        else:
            self.setdefault("locations", locations)

        if parents == None:
            self.setdefault("parents", set())
        else:
            self.setdefault("parents", parents)            

    def addRun(self, run):
        """
        _addRun_

        run should be an instance of WMCore.DataStructs.Run

        Add a run container to this file, tweak the run and lumi
        keys to be max run and max lumi for backwards compat.


        """

        if not isinstance(run, Run):
            msg = "addRun argument must be of type WMCore.DataStructs.Run"
            raise RuntimeError, msg
        
        addFlag = False  
        for runMember in self['runs']:
            if runMember.run ==  run.run:
                # this rely on Run object overwrite __add__ to update self 
                runMember + run
                addFlag = True
        
        if not addFlag:        
            self['runs'].add(run)
        return

    def load(self):
        """
        A DataStructs file has nothing to load from, other implementations will
        over-ride this method.
        """
        if self['id']:
            self['lfn'] = '/store/testing/%s' % self['id']

    def save(self):
        """
        A DataStructs file has nothing to save to, other implementations will
        over-ride this method.
        """
        pass

    def setLocation(self, se):
        self['locations'] = self['locations'] | set(self.makelist(se))

    def __cmp__(self, rhs):
        """
        Sort files in run number and lumi section order
        """
        #if self['run'] == rhs['run']:
        #    return cmp(self['lumi'], rhs['lumi'])
	
        return self.__eq__(rhs)

    def __eq__(self, rhs):
        """
        File is equal if it has the same name
        """
        eq = False
        if type(rhs) == type(self):
            eq = self['lfn'] == rhs['lfn']
        elif type(rhs) == type('string'):
            eq = self['lfn'] == rhs
        return eq

    def __ne__(self, rhs):
        return not self.__eq__(rhs)

    def __hash__(self):
        hash = self['lfn'].__hash__()
        return hash

    def json(self):
        """
        _json_
        
        JSON friendly formatted file

        """
        d = dict()
        d.update(self)
        d['runs'] = [ x.json() for x in self['runs'] ]
        d['locations'] = list(self['locations'])
        d['parents'] = list(self['parents'])
        return d
