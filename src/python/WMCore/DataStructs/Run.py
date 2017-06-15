#!/usr/bin/env python

"""
_Run_

container representing a run, and its constituent lumi sections and event counts
"""

from __future__ import print_function

from WMCore.DataStructs.WMObject import WMObject


class Run(WMObject):
    """
    _Run_

    Run container, is a list of lumi sections with associate event counts
    """

    def __init__(self, runNumber=None, *newLumis):
        WMObject.__init__(self)
        self.run = runNumber
        self.eventsPerLumi = {}
        self.extendLumis(newLumis)

    def __str__(self):
        return "Run%s:%s" % (self.run, self.eventsPerLumi)

    def __lt__(self, rhs):
        """
        Compare on run # first, then by lumis as a list is compared
        """
        if self.run != rhs.run:
            return self.run < rhs.run
        if sorted(self.eventsPerLumi.keys()) != sorted(rhs.eventsPerLumi.keys()):
            return sorted(self.eventsPerLumi.keys()) < sorted(rhs.eventsPerLumi.keys())
        return self.eventsPerLumi < rhs.eventsPerLumi

    def __gt__(self, rhs):
        """
        Compare on run # first, then by lumis as a list is compared
        """
        if self.run != rhs.run:
            return self.run > rhs.run
        if sorted(self.eventsPerLumi.keys()) != sorted(rhs.eventsPerLumi.keys()):
            return sorted(self.eventsPerLumi.keys()) > sorted(rhs.eventsPerLumi.keys())
        return self.eventsPerLumi > rhs.eventsPerLumi

    def extend(self, items):
        """
        Redirect to the function that already does this
        """
        self.extendLumis(items)
        return

    def __cmp__(self, rhs):
        return (self > rhs) - (self < rhs)  # Python3 equivalent of cmp()

    def __add__(self, rhs):
        """
        Combine two runs
        """
        if self.run != rhs.run:
            msg = "Adding together two different runs"
            msg += "Run %s does not equal Run %s" % (self.run, rhs.run)
            raise RuntimeError(msg)

        for lumi, events in rhs.eventsPerLumi.iteritems():
            if lumi not in self.eventsPerLumi or not self.eventsPerLumi[lumi]:  # Either doesn't exist, 0, or None
                self.eventsPerLumi[lumi] = events
            else:
                self.eventsPerLumi[lumi] += events
        return self

    def __iter__(self):
        return self.eventsPerLumi.__iter__()

    def __next__(self):
        """
        __next__ no longer needed
        """
        raise NotImplementedError

    def __len__(self):
        """
        Number of lumis
        """
        return self.eventsPerLumi.__len__()

    def __getitem__(self, key):
        """
        Get the nth lumi from the list (no event count)
        """
        return sorted(self.eventsPerLumi.keys()).__getitem__(key)

    def __setitem__(self, key, lumi):
        """
        Replace the nth lumi from the list (no event count)
        """
        try:
            oldLumi = sorted(self.eventsPerLumi.keys())[key]  # Extract the lumi from the sorted list
            del self.eventsPerLumi[oldLumi]  # Delete it and add the new one
        except IndexError:
            pass
        self.appendLumi(lumi)

    def __delitem__(self, key):
        try:
            oldLumi = sorted(self.eventsPerLumi.keys())[key]  # Extract the lumi from the sorted list
            del self.eventsPerLumi[oldLumi]  # Delete it
        except IndexError:
            pass

    def __eq__(self, rhs):
        """
        Check equality of run numbers and then underlying lumi/event dicts
        """
        if not isinstance(rhs, Run):
            return False
        if self.run != rhs.run:
            return False
        return self.eventsPerLumi == rhs.eventsPerLumi

    def __ne__(self, rhs):
        return not self.__eq__(rhs)

    def __hash__(self):
        """
        Calculate the value of the hash
        """
        value = self.run.__hash__()
        value += hash(frozenset(self.eventsPerLumi.items()))  # Hash that represents the dictionary
        return value

    @property
    def lumis(self):
        """
        Property that makes existing uses of myRun.lumis function by returning a list
        """
        return sorted(self.eventsPerLumi.keys())

    @lumis.setter
    def lumis(self, lumiList):
        """
        Setter to allow for replacement of the lumis with a list or list of tuples
        """
        self.eventsPerLumi = {}  # Remove existing dictionary
        for lumi in lumiList:
            if isinstance(lumi, (list, tuple)):
                self.eventsPerLumi[lumi[0]] = lumi[1]
            else:
                self.eventsPerLumi[lumi] = None

    def extendLumis(self, lumiList):
        """
        Method to replace myRun.lumis.extend() which does not work with the property
        """
        for lumi in lumiList:
            if not isinstance(lumi, (list, tuple)):  # comma separated lumi numbers
                self.eventsPerLumi[lumi] = None
            else:
                if isinstance(lumi, list) and not isinstance(lumi[0], tuple):  # then it's a plain list
                    for l in lumi:
                        self.eventsPerLumi[l] = None
                else:
                    if isinstance(lumi, tuple):  # it's an unpacked list of tuples
                        lumi = [(lumi)]
                    # it's a list/tuple of tuples
                    for tp in lumi:
                        if tp[0] in self.eventsPerLumi and self.eventsPerLumi[tp[0]]:
                            self.eventsPerLumi[tp[0]] += tp[1]  # Already exists, add events
                        else:  # Doesn't exist or is 0 or None
                            self.eventsPerLumi[tp[0]] = tp[1]

    def appendLumi(self, lumi):
        """
        Method to replace myRun.lumis.append() which does not work with the property
        """
        if isinstance(lumi, (list, tuple)) and self.eventsPerLumi[lumi[0]]:  # Already exists, add events
            self.eventsPerLumi[lumi[0]] += lumi[1]
        elif isinstance(lumi, (list, tuple)):  # Doesn't exist or is 0 or None
            self.eventsPerLumi[lumi[0]] = lumi[1]
        else:  # Just given lumis, not events
            if lumi not in self.eventsPerLumi:  # Don't overwrite existing events
                self.eventsPerLumi[lumi] = None

    def getEventsByLumi(self, lumi):
        """
        getter to select event counts by given lumi
        """
        return self.eventsPerLumi.get(lumi)

    def json(self):
        """
        _json_

        Convert to JSON friendly format.  Include some information for the
        thunker so that we can convert back.
        """
        return {"Run": self.run, "Lumis": self.eventsPerLumi,
                "thunker_encoded_json": True, "type": "WMCore.DataStructs.Run.Run"}

    def __to_json__(self, thunker=None):
        """
        __to_json__

        This is the standard way we JSONize other objects.
        Included here so we have a uniform method.
        """
        return self.json()

    def __from_json__(self, jsondata, thunker):
        """
        __from_json__

        Convert JSON data back into a Run object with integer lumi numbers
        """
        self.run = jsondata["Run"]
        self.eventsPerLumi = {}
        for lumi, events in jsondata["Lumis"].iteritems():
            self.eventsPerLumi[int(lumi)] = events  # Make the keys integers again

        return self
