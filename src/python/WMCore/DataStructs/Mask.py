#!/usr/bin/env python
"""
_Mask_

Basic Mask object that can be used to restrict the run/lumi/event range in a
job in two ways:

- Inclusive: The stuff within the mask is processed
- Exclusive: The stuff outside of the mask is processed

"""

from builtins import range
from WMCore.DataStructs.Run import Run


class Mask(dict):
    """
    _Mask_
    """

    def __init__(self, **kwargs):
        dict.__init__(self, **kwargs)
        self.inclusive = True
        self.setdefault("inclusivemask", True)
        self.setdefault("FirstEvent", None)
        self.setdefault("LastEvent", None)
        self.setdefault("FirstLumi", None)
        self.setdefault("LastLumi", None)
        self.setdefault("FirstRun", None)
        self.setdefault("LastRun", None)
        self.setdefault("runAndLumis", {})

    def setMaxAndSkipEvents(self, maxEvents, skipEvents):
        """
        _setMaxAndSkipEvents_

        Set FirstEvent & LastEvent fields as max & skip events

        """

        self['FirstEvent'] = skipEvents
        if maxEvents is not None:
            self['LastEvent'] = skipEvents + maxEvents

        return

    def setMaxAndSkipLumis(self, maxLumis, skipLumi):
        """
        _setMaxAndSkipLumis

        Set the Maximum number of lumi sections and the starting point

        """

        self['FirstLumi'] = skipLumi
        self['LastLumi'] = skipLumi + maxLumis

        return

    def setMaxAndSkipRuns(self, maxRuns, skipRun):
        """
        _setMaxAndSkipRuns

        Set the Maximum number of runss and the starting point

        """

        self['FirstRun'] = skipRun
        self['LastRun'] = skipRun + maxRuns

        return

    def getMaxEvents(self):
        """
        _getMaxEvents_

        return maxevents setting

        """
        if self['LastEvent'] is None or self['FirstEvent'] is None:
            return None
        return self['LastEvent'] - self['FirstEvent'] + 1

    def getMax(self, keyType=None):
        """
        _getMax_

        returns the maximum number of runs/events/etc of the type of the type string
        """
        if 'First%s' % (keyType) not in self:
            return None
        if self['First%s' % (keyType)] is None or self['Last%s' % (keyType)] is None:
            return None
        return self['Last%s' % (keyType)] - self['First%s' % (keyType)] + 1

    def addRun(self, run):
        """
        _addRun_

        Add a run object
        """
        run.lumis.sort()
        firstLumi = run.lumis[0]
        lastLumi = run.lumis[0]
        for lumi in run.lumis:
            if lumi <= lastLumi + 1:
                lastLumi = lumi
            else:
                self.addRunAndLumis(run.run, lumis=[firstLumi, lastLumi])
                firstLumi = lumi
                lastLumi = lumi
        self.addRunAndLumis(run.run, lumis=[firstLumi, lastLumi])
        return

    def addRunWithLumiRanges(self, run, lumiList):
        """
        _addRunWithLumiRanges_

        Add to runAndLumis with call signature
        addRunWithLumiRanges(run=run, lumiList = [[start1,end1], [start2, end2], ...]
        """
        self['runAndLumis'][run] = lumiList
        return

    def addRunAndLumis(self, run, lumis=None):
        """
        _addRunAndLumis_

        Add runs and lumis directly
        TODO: The name of this function is a little misleading. If you pass a list of lumis
              it ignores the content of the list and adds a range based on the max/min in
              the list. Missing lumis in the list are ignored.

        NOTE: If the new run/lumi range overlaps with the pre-existing lumi ranges in the
              mask, no attempt is made to merge these together.  This can result in a mask
              with duplicate lumis.
        """
        lumis = lumis or []
        if not isinstance(lumis, list):
            lumis = list(lumis)

        if run not in self['runAndLumis']:
            self['runAndLumis'][run] = []

        self['runAndLumis'][run].append([min(lumis), max(lumis)])

        return

    def getRunAndLumis(self):
        """
        _getRunAndLumis_

        Return list of active runs and lumis
        """

        return self['runAndLumis']

    def runLumiInMask(self, run, lumi):
        """
        _runLumiInMask_

        See if a particular runLumi is in the mask
        """

        if self['runAndLumis'] == {}:
            # Empty dictionary
            # ALWAYS TRUE
            return True

        if run not in self['runAndLumis']:
            return False

        for pair in self['runAndLumis'][run]:
            # Go through each max and min pair
            if pair[0] <= lumi and pair[1] >= lumi:
                # Then the lumi is bracketed
                return True

        return False

    def filterRunLumisByMask(self, runs):
        """
        _filterRunLumisByMask_

        Pass a Mask a list of run objects, get back a list of
        run objects that correspond to the actual mask allowed values
        """
        if self['runAndLumis'] == {}:
            # Empty dictionary
            # ALWAYS TRUE
            return runs

        runDict = {}
        for r in runs:
            if r.run in runDict:
                runDict[r.run].extendLumis(r.lumis)
            else:
                runDict[r.run] = r

        maskRuns = set(self["runAndLumis"].keys())
        passedRuns = set([r.run for r in runs])
        filteredRuns = maskRuns.intersection(passedRuns)

        newRuns = set()
        for runNumber in filteredRuns:
            maskLumis = set()
            for pair in self["runAndLumis"][runNumber]:
                if pair[0] == pair[1]:
                    maskLumis.add(pair[0])
                else:
                    maskLumis = maskLumis.union(list(range(pair[0], pair[1] + 1)))

            filteredLumis = set(runDict[runNumber].lumis).intersection(maskLumis)
            if len(filteredLumis) > 0:
                filteredLumiEvents = [(lumi, runDict[runNumber].getEventsByLumi(lumi)) for lumi in filteredLumis]
                newRuns.add(Run(runNumber, *filteredLumiEvents))

        return newRuns
