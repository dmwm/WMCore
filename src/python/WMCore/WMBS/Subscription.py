#!/usr/bin/env python
"""
_Subscription_

A simple object representing a Subscription in WMBS.

A subscription is just a way to link many sets of jobs to a
fileset and track the process of the associated jobs. It is
associated to a single fileset and a single workflow.
"""
from __future__ import print_function

from future.utils import listvalues

import logging
from collections import Counter

from WMCore.DataStructs.Fileset import Fileset as WMFileset
from WMCore.DataStructs.Subscription import Subscription as WMSubscription
from WMCore.Services.UUIDLib import makeUUID
from WMCore.WMBS.File import File
from WMCore.WMBS.Fileset import Fileset
from WMCore.WMBS.WMBSBase import WMBSBase
from WMCore.WMBS.Workflow import Workflow


class Subscription(WMBSBase, WMSubscription):
    """
    WMBS Subscription

    WMBS object for executing a task or similar chunk of work
    """

    def __init__(self, fileset=None, workflow=None, id=-1,
                 split_algo="FileBased", type="Processing"):
        WMBSBase.__init__(self)

        # If a fileset or workflow isn't passed in the base class will create
        # empty non-WMBS filesets and workflows.  We want WMBS filesets and
        # workflows so we'll create those here.
        if fileset is None:
            fileset = Fileset()
        if workflow is None:
            workflow = Workflow()

        WMSubscription.__init__(self, fileset=fileset, workflow=workflow,
                                split_algo=split_algo, type=type)

        self.setdefault("id", id)

        self.bulkDeleteLimit = 500
        return

    def create(self):
        """
        Add the subscription to the database
        """
        if self.exists() is not False:
            self.load()
            return

        existingTransaction = self.beginTransaction()
        action = self.daofactory(classname="Subscriptions.New")
        action.execute(fileset=self["fileset"].id, type=self["type"],
                       split_algo=self["split_algo"],
                       workflow=self["workflow"].id,
                       conn=self.getDBConn(),
                       transaction=self.existingTransaction())

        self.load()
        self.commitTransaction(existingTransaction)
        return

    def exists(self):
        """
        See if the subscription is in the database
        """
        action = self.daofactory(classname="Subscriptions.Exists")
        result = action.execute(fileset=self["fileset"].id,
                                workflow=self["workflow"].id,
                                conn=self.getDBConn(),
                                transaction=self.existingTransaction())
        return result

    def load(self):
        """
        _load_

        Load any meta data about the subscription.  This include the id, type,
        split algorithm, fileset id and workflow id.  Either the subscription id
        or the fileset id and workflow id must be specified for this to work.
        """
        existingTransaction = self.beginTransaction()

        if self["id"] > 0:
            action = self.daofactory(classname="Subscriptions.LoadFromID")
            result = action.execute(id=self["id"],
                                    conn=self.getDBConn(),
                                    transaction=self.existingTransaction())
        else:
            action = self.daofactory(classname="Subscriptions.LoadFromFilesetWorkflow")
            result = action.execute(fileset=self["fileset"].id,
                                    workflow=self["workflow"].id,
                                    conn=self.getDBConn(),
                                    transaction=self.existingTransaction())

        self["type"] = result["type"]
        self["id"] = result["id"]
        self["split_algo"] = result["split_algo"]

        # Only load the fileset and workflow if they haven't been loaded
        # already.
        if self["fileset"].id < 0:
            self["fileset"] = Fileset(id=result["fileset"])

        if self["workflow"].id < 0:
            self["workflow"] = Workflow(id=result["workflow"])

        self.commitTransaction(existingTransaction)
        return

    def loadData(self):
        """
        _loadData_

        Load all data having to do with the subscription including all the
        files contained in the fileset and the workflow meta data.
        """
        existingTransaction = self.beginTransaction()

        if self["id"] < 0 or self["fileset"].id < 0 or self["workflow"].id < 0:
            self.load()

        self["fileset"].loadData()
        self["workflow"].load()

        self.commitTransaction(existingTransaction)
        return

    def addWhiteBlackList(self, sites):
        """
        _addWhiteBlackList_

        Add a site white or black list for this transaction.  The sites
        paremeter must be a list of dictionaries with the following keys:
          site_name - The CMS name of the site
          valid - A bool, True for a white list, False for a black list.
        """
        existingTransaction = self.beginTransaction()

        for site in sites:
            site["sub"] = self["id"]

        action = self.daofactory(classname="Subscriptions.AddValidation")
        dummyResult = action.execute(sites=sites, conn=self.getDBConn(), transaction=self.existingTransaction())

        self.commitTransaction(existingTransaction)

    def getWhiteBlackList(self):
        """
        _getWhiteBlackList_

        Retrieve the white lists and black lists for this subscription.
        """
        existingTransaction = self.beginTransaction()

        action = self.daofactory(classname="Subscriptions.GetValidation")
        result = action.execute(self["id"],
                                conn=self.getDBConn(),
                                transaction=self.existingTransaction())

        self.commitTransaction(existingTransaction)
        return result

    def filesOfStatus(self, status, loadChecksums=True, doingJobSplitting=False):
        """
        _filesOfStatus_

        Return a Set of File objects that have the given status with respect
        to this subscription.
        """
        existingTransaction = self.beginTransaction()

        status = status.title()
        files = set()
        action = self.daofactory(classname="Subscriptions.Get%sFiles" % status)
        fileList = action.execute(self["id"], conn=self.getDBConn(),
                                  transaction=self.existingTransaction())

        if doingJobSplitting:
            fileInfoAct = self.daofactory(classname="Files.GetForJobSplittingByID")
        else:
            fileInfoAct = self.daofactory(classname="Files.GetByID")

        fileInfoDict = fileInfoAct.execute(file=[x["file"] for x in fileList],
                                           conn=self.getDBConn(),
                                           transaction=self.existingTransaction())

        # Run through all files
        for f in fileList:
            fl = File(id=f['file'])
            if loadChecksums:
                fl.loadChecksum()
            fl.update(fileInfoDict[f['file']])
            if 'locations' in f:
                fl.setLocation(f['locations'], immediateSave=False)
            files.add(fl)

        self.commitTransaction(existingTransaction)
        return files

    def acquireFiles(self, files=None):
        """
        _acquireFiles_

        Mark all files objects that are passed in as acquired for this
        subscription.  If no files are passed in then all available files
        will be acquired.
        """
        existingTransaction = self.beginTransaction()

        action = self.daofactory(classname="Subscriptions.AcquireFiles")

        if not files:
            files = self.filesOfStatus("Available")
        elif isinstance(files, (Fileset, WMFileset)):
            pass
        else:
            files = self.makelist(files)

        if len(files) == 0:
            self.commitTransaction(existingTransaction)
            return

        action.execute(self['id'], file=[x["id"] for x in files],
                       conn=self.getDBConn(),
                       transaction=self.existingTransaction())

        try:
            self.commitTransaction(existingTransaction)
        except Exception as ex:
            print("Found exception %s" % ex)
            logging.error("Exception found in committing acquireFiles transaction: %s", ex)
        return

    def completeFiles(self, files):
        """
        Mark a (set of) file(s) as completed.
        """
        existingTransaction = self.beginTransaction()

        files = self.makelist(files)

        completeAction = self.daofactory(classname="Subscriptions.CompleteFiles")
        completeAction.execute(subscription=self["id"],
                               file=[x["id"] for x in files],
                               conn=self.getDBConn(),
                               transaction=self.existingTransaction())

        self.commitTransaction(existingTransaction)
        return

    def failFiles(self, files):
        """
        Mark a (set of) file(s) as failed.
        """
        existingTransaction = self.beginTransaction()

        files = self.makelist(files)

        failAction = self.daofactory(classname="Subscriptions.FailFiles")
        failAction.execute(subscription=self["id"],
                           file=[x["id"] for x in files],
                           conn=self.getDBConn(),
                           transaction=self.existingTransaction())

        self.commitTransaction(existingTransaction)
        return

    def getJobs(self):
        """
        Return a list of all the jobs associated with a subscription
        """
        jobsAction = self.daofactory(classname="Subscriptions.Jobs")
        jobs = jobsAction.execute(subscription=self["id"],
                                  conn=self.getDBConn(),
                                  transaction=self.existingTransaction())

        return jobs

    def delete(self):
        """
        _delete_

        Delete this subscription from the database.
        """
        action = self.daofactory(classname="Subscriptions.Delete")
        action.execute(id=self["id"], conn=self.getDBConn(),
                       transaction=self.existingTransaction())

        return

    def isCompleteOnRun(self, runID):
        """
        _isCompleteOnRun_

        Check all the files in the given subscripton and the given run are completed.

        To: check query whether performance can be improved
        """
        statusAction = self.daofactory(classname="Subscriptions.IsCompleteOnRun")
        fileCount = statusAction.execute(self["id"], runID,
                                         conn=self.getDBConn(),
                                         transaction=self.existingTransaction())

        if fileCount == 0:
            return True
        else:
            return False

    def getNumberOfJobsPerSite(self, location, state):
        """
        _getNumberOfJobsPerSite_

        Access the number of jobs at a site in a given status for a given subscription
        """
        jobLocate = self.daofactory(classname="Subscriptions.GetNumberOfJobsPerSite")

        result = listvalues(jobLocate.execute(location=location,
                                              subscription=self['id'],
                                              state=state))[0]
        return result

    def getJobGroups(self):
        """
        _getJobGroups_

        Returns a list of job group IDs associated with the subscription with new jobs
        """
        action = self.daofactory(classname="Subscriptions.GetJobGroups")
        return action.execute(self['id'], conn=self.getDBConn(),
                              transaction=self.existingTransaction())

    def getAllJobGroups(self):
        """
        _getAllJobGroups_

        Returns a list of ALL jobGroups associated with the subscription
        """
        action = self.daofactory(classname="Subscriptions.GetAllJobGroups")
        return action.execute(self['id'], conn=self.getDBConn(),
                              transaction=self.existingTransaction())

    def deleteEverything(self):
        """
        _deleteEverything_

        This function should delete the subscription, and absolutely everything else having anything
        to do with the subscription that is NOT in use by any other piece.  It should check for all
        the proper ownerships through a sequence of DAO calls that will take forever.

        Nothing except the taskArchiver should be calling this.
        """
        existingTransaction = self.beginTransaction()

        jobGroups = self.getAllJobGroups()
        filesets = []

        # The order here is important
        # You need to delete files and filesets from the bottom up
        # In order to not violate parentage

        # Get output filesets from jobGroups
        for jobGroupID in jobGroups:
            loadAction = self.daofactory(classname="JobGroup.LoadFromID")
            result = loadAction.execute(jobGroupID, conn=self.getDBConn(),
                                        transaction=self.existingTransaction())
            filesets.append(result['output'])

        # Get output filesets from the workflow
        for entry in self['workflow'].outputMap:
            for outputFilesets in self['workflow'].outputMap[entry]:
                wid = outputFilesets["output_fileset"].id
                if wid not in filesets:
                    filesets.append(wid)

        # Do the input fileset LAST!
        filesets.append(self['fileset'].id)

        self.commitTransaction(existingTransaction)

        # First, jobs
        # If there are too many jobs, delete them in separate
        # transactions to reduce database load
        deleteAction = self.daofactory(classname="Jobs.Delete")
        jobDeleteList = []
        for job in self.getJobs():
            jobDeleteList.append(job['id'])
        if len(jobDeleteList) > 0:
            if len(jobDeleteList) <= self.bulkDeleteLimit:
                existingTransaction = self.beginTransaction()
                deleteAction.execute(id=jobDeleteList, conn=self.getDBConn(),
                                     transaction=self.existingTransaction())
                self.commitTransaction(existingTransaction)
            else:
                while len(jobDeleteList) > 0:
                    existingTransaction = self.beginTransaction()
                    toDelete = jobDeleteList[:self.bulkDeleteLimit]
                    jobDeleteList = jobDeleteList[self.bulkDeleteLimit:]
                    deleteAction.execute(id=toDelete, conn=self.getDBConn(),
                                         transaction=self.existingTransaction())

                    self.commitTransaction(existingTransaction)

        # Next jobGroups
        deleteAction = self.daofactory(classname="JobGroup.Delete")
        existingTransaction = self.beginTransaction()
        for jobGroupID in jobGroups:
            deleteAction.execute(id=jobGroupID, conn=self.getDBConn(),
                                 transaction=self.existingTransaction())
        self.commitTransaction(existingTransaction)

        # Now, get the filesets that needs to be deleted
        action = self.daofactory(classname="Fileset.CheckForDelete")
        existingTransaction = self.beginTransaction()
        results = action.execute(fileids=filesets,
                                 subid=self['id'],
                                 conn=self.getDBConn(),
                                 transaction=self.existingTransaction())

        self.commitTransaction(existingTransaction)

        deleteFilesets = [x['id'] for x in results]

        # Delete files in sets
        # Each set of files deleted in a separate transaction
        for filesetID in deleteFilesets:
            fileset = Fileset(id=filesetID)

            # Load the files
            filesetFiles = []
            action = self.daofactory(classname="Files.InFileset")
            results = action.execute(fileset=filesetID,
                                     conn=self.getDBConn(),
                                     transaction=self.existingTransaction())

            for result in results:
                filesetFiles.append(result['fileid'])

            # Now get rid of unused files
            if len(filesetFiles) < 1:
                # if we have unused files, of course
                continue

            parent = self.daofactory(classname="Files.DeleteParentCheck")
            action = self.daofactory(classname="Files.DeleteCheck")

            if len(filesetFiles) <= self.bulkDeleteLimit:
                existingTransaction = self.beginTransaction()
                parent.execute(file=filesetFiles, fileset=fileset.id,
                               conn=self.getDBConn(),
                               transaction=self.existingTransaction())
                action.execute(file=filesetFiles, fileset=fileset.id,
                               conn=self.getDBConn(),
                               transaction=self.existingTransaction())
                self.commitTransaction(existingTransaction)
            else:
                while len(filesetFiles) > 0:
                    existingTransaction = self.beginTransaction()
                    toDelete = filesetFiles[:self.bulkDeleteLimit]
                    filesetFiles = filesetFiles[self.bulkDeleteLimit:]
                    parent.execute(file=toDelete, fileset=fileset.id,
                                   conn=self.getDBConn(),
                                   transaction=self.existingTransaction())
                    action.execute(file=toDelete, fileset=fileset.id,
                                   conn=self.getDBConn(),
                                   transaction=self.existingTransaction())
                    self.commitTransaction(existingTransaction)

        # Start a new transaction for filesets, workflow, and the subscription
        existingTransaction = self.beginTransaction()
        for filesetID in deleteFilesets:
            # Now actually delete the filesets
            action = self.daofactory(classname="Fileset.DeleteCheck")
            dummyDeleteFilesets = action.execute(fileid=filesetID, subid=self['id'],
                                                 conn=self.getDBConn(), transaction=self.existingTransaction())

        # Next Workflow
        action = self.daofactory(classname="Workflow.DeleteCheck")
        action.execute(workid=self["workflow"].id, subid=self["id"],
                       conn=self.getDBConn(),
                       transaction=self.existingTransaction())

        self.delete()
        self.commitTransaction(existingTransaction)
        return

    def isFileCompleted(self, files):
        """
        _isFileCompleted_

        Returns True if all the given files are in complete status
        Return False if one of files are not in complete status
        """
        if not isinstance(files, list):
            files = [files]

        action = self.daofactory(classname="Subscriptions.GetCompletedByFileList")
        fileIDs = action.execute(self['id'], files, conn=self.getDBConn(),
                                 transaction=self.existingTransaction())

        for f in files:
            if f['id'] not in fileIDs:
                return False

        return True

    def markFinished(self, finished=True):
        """
        _markFinished_

        Sets the finished status of the subscription
        to the given value
        """

        action = self.daofactory(classname="Subscriptions.MarkFinishedSubscriptions")
        action.execute(self['id'], conn=self.getDBConn(),
                       transaction=self.existingTransaction())
        self.commitTransaction(self.existingTransaction)

        return

    def bulkCommit(self, jobGroups):
        """
        _bulkCommit_

        Commits all objects created during job splitting.  This is dangerous because it assumes
        that you can pass in all jobGroups.
        """

        jobList = []
        jobGroupList = []
        nameList = []

        # You have to do things in this order:
        # 1) First create Filesets, then jobGroups
        # 2) Second, create jobs pointing to jobGroups
        # 3) Deal with masks, etc.

        # First, do we exist?  We better
        # This happens in its own transaction
        if self['id'] == -1:
            self.create()

        existingTransaction = self.beginTransaction()

        # You need to create a number of Filesets equal to the
        # number of jobGroups.

        for _ in jobGroups:
            # Make a random name for each fileset
            nameList.append(makeUUID())

        # Create filesets
        action = self.daofactory(classname="Fileset.BulkNewReturn")
        fsIDs = action.execute(nameList=nameList, open=True,
                               conn=self.getDBConn(),
                               transaction=self.existingTransaction())

        for jobGroup in jobGroups:
            jobGroup.uid = makeUUID()
            jobGroupList.append({'subscription': self['id'],
                                 'uid': jobGroup.uid,
                                 'output': fsIDs.pop()})

        action = self.daofactory(classname="JobGroup.BulkNewReturn")
        jgIDs = action.execute(bulkInput=jobGroupList,
                               conn=self.getDBConn(),
                               transaction=self.existingTransaction())

        for jobGroup in jobGroups:
            for idUID in jgIDs:
                # This should assign an ID to the right job
                if jobGroup.uid == idUID['guid']:
                    jobGroup.id = idUID['id']
                    break

        for jobGroup in jobGroups:
            for job in jobGroup.newjobs:
                if job["id"] is not None:
                    continue

                job["jobgroup"] = jobGroup.id

                if job["name"] is None:
                    job["name"] = makeUUID()
                jobList.append(job)

        bulkAction = self.daofactory(classname="Jobs.New")
        result = bulkAction.execute(jobList=jobList, conn=self.getDBConn(),
                                    transaction=self.existingTransaction())

        # Move jobs to jobs from newjobs
        for jobGroup in jobGroups:
            jobGroup.jobs.extend(jobGroup.newjobs)
            jobGroup.newjobs = []

        # Use the results of the bulk commit to get the jobIDs
        fileDict = {}
        jobFileRunLumis = []
        for job in jobList:
            job['id'] = result[job['name']]
            fileDict[job['id']] = []
            for f in job['input_files']:
                fileDict[job['id']].append(f['id'])
                fileMask = job['mask'].filterRunLumisByMask(runs=f['runs'])
                for runObj in fileMask:
                    run = runObj.run
                    lumis = runObj.lumis
                    for lumi in lumis:
                        jobFileRunLumis.append((job['id'], f['id'], run, lumi))

        # Create a list of mask binds
        maskList = []
        for job in jobList:
            mask = job['mask']
            if len(list(mask['runAndLumis'].keys())) > 0:
                # Then we have multiple binds
                binds = mask.produceCommitBinds(jobID=job['id'])
                maskList.extend(binds)
            else:
                mask['jobID'] = job['id']
                maskList.append(mask)

        maskAction = self.daofactory(classname="Masks.Save")
        maskAction.execute(jobid=None, mask=maskList, conn=self.getDBConn(),
                           transaction=self.existingTransaction())

        fileAction = self.daofactory(classname="Jobs.AddFiles")
        fileAction.execute(jobDict=fileDict, conn=self.getDBConn(),
                           transaction=self.existingTransaction())

        # wfid = self['workflow'].id
        # Add work units and associate them
        # wuAction = self.daofactory(classname='WorkUnit.Add')
        # wufAction = self.daofactory(classname='Jobs.AddWorkUnits')

        # Make a count of how many times each job appears in the list of jobFileRunLumis
        # jobUnitCounts = Counter([jid for jid, _, _, _ in jobFileRunLumis])

        # for jid, fid, run, lumi in jobFileRunLumis:
        #     wuAction.execute(taskid=wfid, fileid=fid, run=run, lumi=lumi, last_unit_count=jobUnitCounts[jid],
        #                      conn=self.getDBConn(), transaction=self.existingTransaction())
        # wufAction.execute(jobFileRunLumis=jobFileRunLumis,
        #                   conn=self.getDBConn(), transaction=self.existingTransaction())

        fileList = []
        for job in jobList:
            fileList.extend(job['input_files'])

        self.acquireFiles(files=fileList)
        self.commitTransaction(existingTransaction)
        return
