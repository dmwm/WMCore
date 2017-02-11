#!/usr/bin/env python
"""
_JobGroup_

Definition of JobGroup:
    Set of jobs running on same input file for same Workflow
    Set of jobs for a single subscription
    Required for certain job splitting Algo's (.g. event split to make complete
    lumi)
    Subscription:JobGroup == 1:N
    JobGroup:Jobs = 1:N
    JobGroup:InFile = 1:1
    JobGroup:MergedOutFile = N:1
    JobGroup at least one Lumi section

A JobGroup is a set of jobs and a Fileset that contains their output.

JobGroup knows the Subscription and passes the Workflow to Jobs in the group.

Jobs know their status (active, failed, complete) and know the files they run
on but don't know the group. They do know their subscription and corresponding
workflow. This means Jobs can update their state in the database without
talking to the group, and WMBS JobGroups can calculate status from the database
instead of the in memory objects.
"""

import logging
import threading

from WMCore.DataStructs.JobGroup import JobGroup as WMJobGroup
from WMCore.WMBS.WMBSBase import WMBSBase

from WMCore.WMBS.Fileset import Fileset
from WMCore.WMBS.Job import Job
from WMCore.WMBS.Subscription import Subscription

from WMCore.Services.UUIDLib import makeUUID

class JobGroup(WMBSBase, WMJobGroup):
    """
    A group (set) of Jobs
    """
    def __init__(self, subscription = None, jobs = None, id = -1, uid = None, location = None):
        WMBSBase.__init__(self)
        WMJobGroup.__init__(self, subscription=subscription, jobs = jobs)

        self.id = id
        self.lastUpdate = None
        self.uid = uid

        if location != None:
            self.setSite(location)

        return

    def create(self):
        """
        Add the new jobgroup to WMBS, create the output Fileset object
        """
        myThread = threading.currentThread()
        existingTransaction = self.beginTransaction()

        #overwrite base class self.output for WMBS fileset
        self.output = Fileset(name = makeUUID())
        self.output.create()

        if self.uid == None:
            self.uid = makeUUID()

        action = self.daofactory(classname = "JobGroup.New")
        action.execute(self.uid, self.subscription["id"],
                       self.output.id, conn = self.getDBConn(),
                       transaction = self.existingTransaction())

        self.id = self.exists()
        self.commitTransaction(existingTransaction)

        return

    def delete(self):
        """
        Remove a jobgroup from WMBS
        """
        deleteAction = self.daofactory(classname = "JobGroup.Delete")
        deleteAction.execute(id = self.id, conn = self.getDBConn(),
                             transaction = self.existingTransaction())

        return

    def exists(self):
        """
        Does a jobgroup exist with id if id is not provided, use the uid,
        return the id
        """
        if self.id != -1:
            action = self.daofactory(classname = "JobGroup.ExistsByID")
            result =  action.execute(id = self.id, conn = self.getDBConn(),
                                     transaction = self.existingTransaction())
        else:
            action = self.daofactory(classname = "JobGroup.Exists")
            result = action.execute(uid = self.uid, conn = self.getDBConn(),
                                    transaction = self.existingTransaction())

        return result

    def load(self):
        """
        _load_

        Load all meta data associated with the JobGroup.  This includes the
        JobGroup id, uid, last_update time, subscription id and output fileset
        id.  Either the JobGroup id or uid must be specified for this to work.
        """
        existingTransaction = self.beginTransaction()

        if self.id > 0:
            loadAction = self.daofactory(classname = "JobGroup.LoadFromID")
            result = loadAction.execute(self.id, conn = self.getDBConn(),
                                        transaction = self.existingTransaction())
        else:
            loadAction = self.daofactory(classname = "JobGroup.LoadFromUID")
            result = loadAction.execute(self.uid, conn = self.getDBConn(),
                                        transaction = self.existingTransaction())

        self.id = result["id"]
        self.uid = result["uid"]
        self.lastUpdate = result["last_update"]

        self.subscription = Subscription(id = result["subscription"])
        self.subscription.load()

        self.output = Fileset(id = result["output"])
        self.output.load()

        self.jobs = []
        self.commitTransaction(existingTransaction)
        return

    def loadData(self):
        """
        _loadData_

        Load all data that is associated with the jobgroup.  This includes
        loading all the subscription information, the output fileset
        information and all the jobs that are associated with the group.
        """
        existingTransaction = self.beginTransaction()

        if self.id < 0 or self.uid == None:
            self.load()

        self.subscription.loadData()
        self.output.loadData()

        loadAction = self.daofactory(classname = "JobGroup.LoadJobs")
        result = loadAction.execute(self.id, conn = self.getDBConn(),
                                    transaction = self.existingTransaction())

        self.jobs = []
        self.newjobs = []

        for jobID in result:
            newJob = Job(id = jobID["id"])
            newJob.loadData()
            self.add(newJob)

        WMJobGroup.commit(self)
        self.commitTransaction(existingTransaction)
        return

    def commit(self):
        """
        _commit_

        Write any new jobs to the database, creating them in the database if
        necessary.
        """
        existingTransaction = self.beginTransaction()

        if self.id == -1:
            self.create()

        for j in self.newjobs:
            j.create(group = self)

        WMJobGroup.commit(self)
        self.commitTransaction(existingTransaction)
        return


    def setSite(self, site_name = None):
        """
        Updates the jobGroup with a site_name from the wmbs_location table
        """
        if not self.exists():
            return

        action = self.daofactory(classname = "JobGroup.SetSite")
        result = action.execute(site_name = site_name, jobGroupID = self.id,
                                conn = self.getDBConn(), transaction = self.existingTransaction())

        return result


    def getSite(self):
        """
        Updates the jobGroup with a site_name from the wmbs_location table
        """
        if not self.exists():
            return

        action = self.daofactory(classname = "JobGroup.GetSite")
        result = action.execute(jobGroupID = self.id, conn = self.getDBConn(),
                                transaction = self.existingTransaction())

        return result

    def listJobIDs(self):
        """
        Returns a list of job IDs
        Useful for times when threading the loading of jobGroups, where running loadData can overload UUID
        """

        existingTransaction = self.beginTransaction()

        if self.id < 0 or self.uid == None:
            self.load()

        loadAction = self.daofactory(classname = "JobGroup.LoadJobs")
        result = loadAction.execute(self.id, conn = self.getDBConn(),
                                    transaction = self.existingTransaction())

        jobIDList = []

        for jobID in result:
            jobIDList.append(jobID["id"])

        self.commitTransaction(existingTransaction)
        return jobIDList


    def commitBulk(self):
        """
        Creates jobs in a group instead of singly, as is done in jobGroup.commit()
        """

        myThread = threading.currentThread()

        if self.id == -1:
            myThread.transaction.begin()
            #existingTransaction = self.beginTransaction()
            self.create()
            #self.commitTransaction(existingTransaction)
            myThread.transaction.commit()

        existingTransaction = self.beginTransaction()

        listOfJobs = []
        for job in self.newjobs:
            #First do all the header stuff
            if job["id"] != None:
                continue

            job["jobgroup"] = self.id

            if job["name"] == None:
                job["name"] = makeUUID()

            listOfJobs.append(job)

        bulkAction = self.daofactory(classname = "Jobs.New")
        result = bulkAction.execute(jobList = listOfJobs)

        #Use the results of the bulk commit to get the jobIDs
        fileDict = {}
        for job in listOfJobs:
            job['id'] = result[job['name']]
            fileDict[job['id']] = []
            for file in job['input_files']:
                fileDict[job['id']].append(file['id'])

        maskAction = self.daofactory(classname = "Masks.New")
        maskAction.execute(jobList = listOfJobs, conn = self.getDBConn(), \
                           transaction = self.existingTransaction())

        fileAction = self.daofactory(classname = "Jobs.AddFiles")
        fileAction.execute(jobDict = fileDict, conn = self.getDBConn(), \
                           transaction = self.existingTransaction())


        WMJobGroup.commit(self)
        self.commitTransaction(existingTransaction)

        return


    def getLocationsForJobs(self):
        """
        Gets a list of the locations that jobs can run at
        """
        if not self.exists():
            return

        action = self.daofactory(classname = "JobGroup.GetLocationsForJobs")
        result = action.execute(id = self.id, conn = self.getDBConn(),
                                transaction = self.existingTransaction())

        return result


    def __str__(self):
        """
        __str__

        Print out some information about the jobGroup
        as if jobGroup inherited from dict()
        """

        d = {'id': self.id, 'uid': self.uid, 'subscription': self.subscription,
             'output': self.output, 'jobs': self.jobs,
             'newjobs': self.newjobs}

        return str(d)
