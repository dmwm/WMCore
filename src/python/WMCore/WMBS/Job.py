#!/usr/bin/env python
"""
_Job_

A job is owned by a jobgroup (which gives it it's workflow) and is
associated to a (set of) file(s). The job interacts with its jobgroup
to acquire/complete/fail files. A job know's it's Workflow via it's
jobgroup. A job is meaningless without a jobgroup.

A WMBS job != a job in a batch system, it's more abstract - it's the piece of
work that needs to get done.

Jobs are added to the WMBS database by their parent JobGroup, but are
responsible for updating their state (and name).
"""

import datetime

from WMCore.DataStructs.Job     import Job as WMJob
from WMCore.DataStructs.Fileset import Fileset
from WMCore.DataStructs.Mask    import Mask as WMMask
from WMCore.WMBS.File           import File
from WMCore.WMBS.Fileset        import Fileset as WMBSFileset
from WMCore.WMBS.Mask           import Mask
from WMCore.WMBS.WMBSBase       import WMBSBase
from WMCore.Services.UUID       import makeUUID

class Job(WMBSBase, WMJob):
    """
    A job in WMBS
    """
    def __init__(self, name = None, files = None, id = None):
        """
        ___init___

        jobgroup object is used to determine the workflow.
        inputFiles is a list of files that the job will process.
        """
        WMBSBase.__init__(self)
        WMJob.__init__(self, name = name, files = files)

        self["id"]           = id
        self["jobgroup"]     = None
        self["couch_record"] = None
        self["attachments"]  = {}
        self["cache_dir"]    = None
        self["sandbox"]      = None
        self['fwjr']         = None
        self["mask"]         = Mask()
        self['custom']       = {}  # For local add-ons that we want to send to JSON

        return

    def create(self, group):
        """
        _create_

        Write the job to the database.
        """
        if self["id"] != None:
            return

        existingTransaction = self.beginTransaction()

        self["jobgroup"] = group.id

        if self["name"] == None:
            self["name"] = makeUUID()

        jobAction = self.daofactory(classname = "Jobs.New")
        jobAction.execute(jobgroup = self["jobgroup"], name = self["name"],
                          couch_record = self["couch_record"],
                          location = self["location"], cache_dir = self['cache_dir'],
                          outcome = self['outcome'], fwjr = self['fwjr'],
                          conn = self.getDBConn(),
                          transaction = self.existingTransaction())

        self.exists()

        self['mask'].save(jobID = self['id'])

        self.associateFiles()
        self.commitTransaction(existingTransaction)
        return

    def delete(self):
        """
        Remove a job from WMBS
        """
        deleteAction = self.daofactory(classname = "Jobs.Delete")
        deleteAction.execute(id = self["id"], conn = self.getDBConn(),
                              transaction = self.existingTransaction())
        return

    def save(self, MaskAndFiles = True):
        """
        _save_

        Flush all changes that have been made to the job to the database.

        Note that this does not save the state of the job, the state has to
        be altered with the JobStateMachine, in order to preserve order and
        prevent any accidental trouble
        """
        existingTransaction = self.beginTransaction()

        saveAction = self.daofactory(classname = "Jobs.Save")
        saveAction.execute(jobid = self["id"], jobgroup = self["jobgroup"],
                           name = self["name"], couch_record = self["couch_record"],
                           location = self["location"], outcome = self["outcome"],
                           cache_dir = self["cache_dir"], fwjr = self['fwjr_path'],
                           retry_count = self['retry_count'],
                           conn = self.getDBConn(),
                           transaction = self.existingTransaction())

        if MaskAndFiles:
            self['mask'].save(jobID = self['id'])
            self.associateFiles()

        self.commitTransaction(existingTransaction)
        return

    def exists(self):
        """
        _exists_

        Does a job exist with this name or id.
        """
        if self["id"] != None:
            action = self.daofactory(classname = "Jobs.ExistsByID")
            result =  action.execute(id = self["id"], conn = self.getDBConn(),
                                     transaction = self.existingTransaction())
        else:
            action = self.daofactory(classname = "Jobs.Exists")
            result = action.execute(name = self["name"], conn = self.getDBConn(),
                                  transaction = self.existingTransaction())

            if result:
                self["id"] = result

        return result

    def load(self):
        """
        _load_

        Load the job's name, id, jobgroup, state, state_time, retry_count,
        couch_record, location and outcome from the database.  Either the ID
        or the name must be set before this is called.
        """
        if self["id"] != None:
            loadAction = self.daofactory(classname = "Jobs.LoadFromID")
            results = loadAction.execute(self["id"], conn = self.getDBConn(),
                                         transaction = self.existingTransaction())
        else:
            loadAction = self.daofactory(classname = "Jobs.LoadFromName")
            results = loadAction.execute(self["name"], conn = self.getDBConn(),
                                         transaction = self.existingTransaction())

        self.update(results)
        if self['mask']['FirstRun'] != None and self['mask']['FirstRun'] == self['mask']['LastRun']:
            self['mask'].load(jobID = self['id'])
        return

    def loadData(self):
        """
        _loadData_

        Load all information about the job, including the mask and all input
        files.  Either the ID or the name must be specified before this is
        called.
        """
        existingTransaction = self.beginTransaction()

        self.load()
        self.getMask()

        fileAction = self.daofactory(classname = "Jobs.LoadFiles")
        files = fileAction.execute(self["id"], conn = self.getDBConn(),
                                   transaction = self.existingTransaction())

        self["input_files"] = []
        for file in files:
            newFile = File(id = file["id"])
            newFile.loadData(parentage = 0)
            self.addFile(newFile)

        self.commitTransaction(existingTransaction)
        return

    def getMask(self):
        """
        _getMask_

        Load the job mask from the database and return it.
        """
        self['mask'].load(jobID = self['id'])
        return self["mask"]

    def getFiles(self, type = "list"):
        """
        _getFiles_

        Retrieve a list of files that are associated with the job.
        """
        if self["id"] == None:
            return WMJob.getFiles(self, type)

        existingTransaction = self.beginTransaction()
        idAction = self.daofactory(classname = "Jobs.LoadFiles")
        fileIDs = idAction.execute(self["id"], conn = self.getDBConn(),
                                   transaction = self.existingTransaction())

        currentFileIDs = WMJob.getFiles(self, type = "id")
        for fileID in fileIDs:
            if fileID["id"] not in currentFileIDs:
                self.loadData()
                break

        self.commitTransaction(existingTransaction)
        return WMJob.getFiles(self, type)


    def getFileLocations(self):
        """
        _getFileLocations_

        Assuming that all files have the same locations, this grabs the locations that the
        files are at.
        """

        locAction = self.daofactory(classname = "Jobs.LoadFileLocations")
        locations = locAction.execute(self["id"], conn = self.getDBConn(),
                                      transaction = self.existingTransaction())

        return locations


    def associateFiles(self):
        """
        _associateFiles_

        Update the wmbs_job_assoc table with the files in the inputFiles for the
        job.
        """
        files = WMJob.getFiles(self, type = "id")

        if len(files) > 0:
            addAction = self.daofactory(classname = "Jobs.AddFiles")
            addAction.execute(self["id"], files, conn = self.getDBConn(),
                              transaction = self.existingTransaction())

        return

    def getState(self):
        """
        _getState_

        Retrieve the state that the job is currently in.
        """
        action = self.daofactory(classname = "Jobs.GetState")
        state = action.execute(self["id"], conn = self.getDBConn(),
                               transaction = self.existingTransaction)

        return state

    def getOutputDBSParentLFNs(self):
        """
        _getOutputDBSParentLFNs_

        Retrieve the LFNs of the files that should be marked as the parents of
        the output of this job.  This will be the closest relative of the input
        for the job that is marked as merged.

        Note: This assumes that the input for the job will consist of files that
        are either all merged or all unmerged.  It will not work correctly if
        the input for the job consists of a mix of merged and unmerged files.
        """
        action = self.daofactory(classname = "Jobs.GetOutputParentLFNs")
        parentLFNs = action.execute(self["id"], conn = self.getDBConn(),
                                    transaction = self.existingTransaction)

        return parentLFNs

    def __to_json__(self, thunker):
        """
        __to_json__

        Serialize the job object.  This will convert all Sets() to lists and
        weed out the internal data structures that don't need to be shared.
        """
        jobDict = {"name": self["name"], "mask": self["mask"],
                   "retry_count": self["retry_count"], "state": self["state"],
                   "jobgroup": self["jobgroup"], "outcome": self["outcome"],
                   "id": self["id"], "input_files": [], "cache_dir": self["cache_dir"],
                   "sandbox": self["sandbox"]}

        for inputFile in self["input_files"]:
            jobDict["input_files"].append(thunker._thunk(inputFile))
        # These attributes are added to the job object by the ChangeState
        # code, and we want to store them in couch.
        if "_id" in self:
            jobDict["_id"] = self["_id"]
        if "state_changes" in self:
            jobDict["state_changes"] = self["state_changes"]
        if "fwkjrs" in self:
            jobDict["fwkjrs"] = []
            for fwkjr in self["fwkjrs"]:
                jobDict["fwkjrs"].append(thunker._thunk(fwkjr))

        if self['custom'] != {}:
            # If we have something in the baggage, attach it.
            # Baggage must only have simple variables
            jobDict['custom'] = self['custom']

        return jobDict

    def getCache(self, refreshFlag = False):
        """
        _getCache_

        Retrieve the location of the jobCache
        """

        if not refreshFlag and self['cache_dir']:
            return self['cache_dir']

        action = self.daofactory(classname = "Jobs.GetCache")
        state  = action.execute(self["id"], conn = self.getDBConn(),
                                transaction = self.existingTransaction)

        return state

    def setCache(self, cacheDir):
        """
        _setCache_

        Set the location of the jobCache
        """

        action = self.daofactory(classname = "Jobs.SetCache")
        state  = action.execute(id = self["id"], cacheDir = cacheDir,
                                conn = self.getDBConn(),
                                transaction = self.existingTransaction)

        return state

    def completeInputFiles(self, skipFiles = None):
        """
        _completeInputFiles_

        Set the location of the jobCache
        """

        action = self.daofactory(classname = "Jobs.CompleteInput")
        if skipFiles:
            state  = action.execute(self["id"], lfnsToSkip = {self["id"] : skipFiles},
                                    conn = self.getDBConn(),
                                    transaction = self.existingTransaction)
        else:
            state  = action.execute(self["id"],
                                    conn = self.getDBConn(),
                                    transaction = self.existingTransaction)

        return state

    def failInputFiles(self):
        """
        _failInputFiles_

        Set the location of the jobCache
        """

        action = self.daofactory(classname = "Jobs.FailInput")
        state  = action.execute(self["id"], conn = self.getDBConn(),
                                transaction = self.existingTransaction)

        return state


    def setFWJRPath(self, fwjrPath = None):
        """
        _setFWJRPath_

        Sets the path for the fwjr in the WMBS database
        """

        if not fwjrPath:
            if 'fwjr' in self.keys():
                fwjrPath = self['fwjr']
            else:
                return None


        action = self.daofactory(classname = "Jobs.SetFWJRPath")
        state  = action.execute(self['id'], fwjrPath, conn = self.getDBConn(),
                                transaction = self.existingTransaction)


        return state

    def getDataStructsJob(self):
        """
        _getDataStructsJob_

        Returns the DataStructs version of this job
        """
        job = WMJob(name = self['name'])

        # Transfer all simple keys
        for key in self.keys():
            keyType = type(self.get(key))
            if keyType in [str, long, int, float]:
                job[key] = self[key]

        for file in self['input_files']:
            job['input_files'].append(file.returnDataStructsFile())

        job['mask'] = WMMask()
        for key in self["mask"].keys():
            job["mask"][key] = self["mask"][key]

        job.baggage = self.baggage

        return job


    def loadOutputID(self):
        """
        _loadOutputID_

        Loads the output ID of the jobgroup assigned to this job
        """

        if self['id'] < 0:
            self.create()

        action = self.daofactory(classname = "Jobs.LoadOutputID")
        result = action.execute(jobID = self['id'], conn = self.getDBConn(),
                                transaction = self.existingTransaction)

        return result
