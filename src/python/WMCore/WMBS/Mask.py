#!/usr/bin/env python


"""
Mask.py

This is a database-enabled version of the Mask code
This is because saving a series of lumis in the database
requires significant parsing ability.  I'm going to have
to add code to reduce the lumis to single entries and
then put them back together again.
"""

from WMCore.DataStructs.Mask import Mask as WMMask

from WMCore.WMBS.WMBSBase import WMBSBase

class Mask(WMBSBase, WMMask):
    """
    Class that will handle loading and saving of masks

    """


    def __init__(self):
        WMMask.__init__(self)
        WMBSBase.__init__(self)


        return


    def generateDictionary(self):
        """
        _generateDictionary_

        Generate a dictionary of the data in the mask
        so I don't have to recursively call myself
        """


        returnDict = {}
        for key in self:
            returnDict[key] = self[key]


        return returnDict


    def produceCommitBinds(self, jobID):
        """
        _produceCommitBinds_

        Produce a list of binds to commit to the database
        """


        maskList = []
        for run in self['runAndLumis']:
            for lumiPair in self['runAndLumis'][run]:
                tmpMask = WMMask()
                tmpMask['jobID']      = jobID
                tmpMask['inclusivemask']  = self['inclusivemask']
                tmpMask['FirstEvent'] = self['FirstEvent']
                tmpMask['LastEvent']  = self['LastEvent']
                tmpMask['FirstRun']   = int(run)
                tmpMask['LastRun']    = int(run)
                tmpMask['FirstLumi']  = lumiPair[0]
                tmpMask['LastLumi']   = lumiPair[1]
                maskList.append(tmpMask)


        return maskList



    def save(self, jobID):
        """
        _save_

        This saves the mask.
        If all you have is a simple event or lumi mask, it saves it.
        However, if you have entries in runAndLumis, it has to
        parse the whole thing.
        """

        maskSaveAction = self.daofactory(classname = "Masks.Save")


        existingTransaction = self.beginTransaction()


        if len(self['runAndLumis']) == 0:
            # Then we have nothing out of the ordinary

            maskSaveAction.execute(jobid = jobID, mask = self,
                                   conn = self.getDBConn(),
                                   transaction = self.existingTransaction())
        else:
            # Then we have to break things into separate commits
            maskList = self.produceCommitBinds(jobID = jobID)

            # Save 'em
            maskSaveAction.execute(jobid = jobID,
                                   mask = maskList,
                                   conn = self.getDBConn(),
                                   transaction = self.existingTransaction())


        self.commitTransaction(existingTransaction)


        return


    def load(self, jobID):
        """
        _load_

        Load all masks associated with the jobID.

        If multiple masks, combine them all
        """
        existingTransaction = self.beginTransaction()

        maskLoadAction = self.daofactory(classname = "Masks.Load")

        jobMask = maskLoadAction.execute(jobID, conn = self.getDBConn(),
                                         transaction = self.existingTransaction())

        self.commitTransaction(existingTransaction)

        # Now we get a bit weird.
        # We assemble things into a list
        # NOTE: Right now this will totally break down if you have multiple mask entries
        # with the same first and last event

        for mask in jobMask:
            firstLumi = None
            lastLumi  = None
            firstRun  = None
            lastRun   = None
            if mask['FirstEvent'] != None:
                self['FirstEvent'] = mask['FirstEvent']
            if mask['LastEvent'] != None:
                self['LastEvent'] = mask['LastEvent']
            if mask['FirstLumi'] != None:
                self['FirstLumi'] = mask['FirstLumi']
                firstLumi         = mask['FirstLumi']
            if mask['LastLumi'] != None:
                self['LastLumi'] = mask['LastLumi']
                lastLumi         = mask['LastLumi']
            if mask['FirstRun'] != None:
                self['FirstRun'] = mask['FirstRun']
                firstRun         = mask['FirstRun']
            if mask['LastRun'] != None:
                self['LastRun'] = mask['LastRun']
                lastRun         = mask['LastRun']

            # Only add this to the list if it makes sense
            # This requires firstRun == lastRun
            # Otherwise we don't know where the lumis end up

            blankRunLumi = False
            if firstRun == lastRun and firstRun != None:
                blankRunLumi = True
                self.addRunAndLumis(run = firstRun, lumis = [firstLumi, lastLumi])

                # Then all the run and lumi info is in the RunAndLumis dict
                mask['FirstRun']  = None
                mask['LastRun']   = None
                mask['FirstLumi'] = None
                mask['LastLumi']  = None


        return
