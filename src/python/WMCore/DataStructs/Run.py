#!/usr/bin/env python

"""
_Run_

container representing a run, and its constituent lumi sections
"""

from WMCore.DataStructs.WMObject import WMObject


class Run(WMObject):
    """
    _Run_

    Run container, is a list of lumi sections
    """

    def __init__(self, runNumber=None, *newLumis):
        WMObject.__init__(self)
        self.run = runNumber
        self.eventsPerLumi = []
        self.eventsPerLumi.extend(newLumis)

    def __str__(self):
        return "Run%s:%s" % (self.run, list(self.eventsPerLumi))

    def __lt__(self, rhs):
        if self.run != rhs.run:
            return self.run < rhs.run
        return list(self.eventsPerLumi) < list(rhs.eventsPerLumi)

    def __gt__(self, rhs):
        if self.run != rhs.run:
            return self.run > rhs.run
        return list(self.eventsPerLumi) > list(rhs.eventsPerLumi)

    def extend(self, items):
        self.extendLumis(items)
        return

    def __cmp__(self, rhs):
        return (self > rhs) - (self < rhs)  # Python3 equivalent of cmp()

    def __add__(self, rhs):
        """
        combine two runs
        """
        if self.run != rhs.run:
            msg = "Adding together two different runs"
            msg += "Run %s does not equal Run %s" % (self.run, rhs.run)
            raise RuntimeError(msg)

        # newRun = Run(self.run, *self)
        # [ newRun.append(x) for x in rhs if x not in newRun ]
        [self.eventsPerLumi.append(x) for x in rhs.lumis if x not in self.eventsPerLumi]

        return self

    def __iter__(self):
        return self.eventsPerLumi.__iter__()

    def __next__(self):
        return self.eventsPerLumi.__next__()

    def __len__(self):
        return self.eventsPerLumi.__len__()

    def __getitem__(self, key):
        return self.eventsPerLumi.__getitem__(key)

    def __setitem__(self, key, value):
        return self.eventsPerLumi.__setitem__(key, value)

    def __delitem__(self, key):
        return self.eventsPerLumi.__delitem__(key)

    def __eq__(self, rhs):
        if not isinstance(rhs, Run):
            return False
        if self.run != rhs.run:
            return False
        return list(self.eventsPerLumi) == list(rhs.eventsPerLumi)

    def __ne__(self, rhs):
        return not self.__eq__(rhs)

    def __hash__(self):

        value = self.run.__hash__()
        self.eventsPerLumi.sort()
        for lumi in self.eventsPerLumi:
            value += lumi.__hash__()
        return value

    @property
    def lumis(self):
        return list(self.eventsPerLumi)

    @lumis.setter
    def lumis(self, lumiList):
        self.eventsPerLumi = list(lumiList)

    def extendLumis(self, lumiList):
        self.eventsPerLumi.extend(lumiList)

    def appendLumi(self, value):
        self.eventsPerLumi.append(value)

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

        This is the standard way we jsonize other objects.
        Included here so we have a uniform method.
        """
        return self.json()

    def __from_json__(self, jsondata, thunker):
        """
        __from_json__

        Conver JSON data back into a Run object.
        """
        self.run = jsondata["Run"]
        self.eventsPerLumi = jsondata["Lumis"]
        return self
