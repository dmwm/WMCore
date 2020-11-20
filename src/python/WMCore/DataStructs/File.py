#!/usr/bin/env python
"""
_File_

Data object that contains details for a single file

"""
__all__ = []

from WMCore.DataStructs.Run import Run
from WMCore.DataStructs.WMObject import WMObject


class File(WMObject, dict):
    """
    _File_
    Data object that contains details for a single file
    """

    def __init__(self, lfn="", size=0, events=0, checksums=None,
                 parents=None, locations=None, merged=False):
        dict.__init__(self)
        checksums = checksums or {}
        self.setdefault("lfn", lfn)
        self.setdefault("size", size)
        self.setdefault("events", events)
        self.setdefault("checksums", checksums)
        self.setdefault('runs', set())
        self.setdefault('merged', merged)
        self.setdefault('last_event', 0)
        self.setdefault('first_event', 0)
        if locations is None:
            self.setdefault("locations", set())
        else:
            self.setdefault("locations", locations)

        if parents is None:
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
            raise RuntimeError(msg)

        addFlag = False
        for runMember in self['runs']:
            if runMember.run == run.run:
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

    def setLocation(self, pnn):
        # Make sure we don't add None, [], "" as file location
        if pnn:
            self['locations'] = self['locations'] | set(self.makelist(pnn))

    def __cmp__(self, rhs):
        """
        Sort files in run number and lumi section order
        """
        # if self['run'] == rhs['run']:
        #    return cmp(self['lumi'], rhs['lumi'])

        return self.__eq__(rhs)

    def __eq__(self, rhs):
        """
        File is equal if it has the same name
        """
        eq = False
        if isinstance(rhs, type(self)):
            eq = self['lfn'] == rhs['lfn']
        elif isinstance(rhs, basestring):
            eq = self['lfn'] == rhs
        return eq

    def __ne__(self, rhs):
        return not self.__eq__(rhs)

    def __hash__(self):
        thisHash = self['lfn'].__hash__()
        return thisHash

    def json(self, thunker=None):
        """
        _json_

        Serialize the file object.  This will convert all Sets() to lists and
        weed out the internal data structures that don't need to be shared.
        """
        fileDict = {"last_event": self["last_event"],
                    "first_event": self["first_event"],
                    "lfn": self["lfn"],
                    "locations": list(self["locations"]),
                    "id": self.get("id", None),
                    "checksums": self["checksums"],
                    "events": self["events"],
                    "merged": self["merged"],
                    "size": self["size"],
                    "runs": [],
                    "parents": []}

        for parent in self["parents"]:
            if isinstance(parent, basestring):
                # Then for some reason, we're passing strings
                # Done specifically for ErrorHandler
                fileDict['parents'].append(parent)
            elif thunker is None:
                continue
            else:
                fileDict["parents"].append(thunker._thunk(parent))

        for run in self["runs"]:
            runDict = {"run_number": run.run,
                       "lumis": run.lumis}
            fileDict["runs"].append(runDict)

        return fileDict

    def __to_json__(self, thunker=None):
        """
        __to_json__

        This is the standard way we jsonize other objects.
        Included here so we have a uniform method.
        """
        return self.json(thunker)
