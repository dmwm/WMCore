#!/usr/bin/env python
"""
_Subscription_

A simple object representing a Subscription in WMBS.

A subscription is just a way to link many sets of jobs to a 
fileset and track the process of the associated jobs. It is 
associated to a single fileset and a single workflow.
"""

import logging

from WMCore.WMBS.Fileset  import Fileset
from WMCore.WMBS.File     import File
from WMCore.WMBS.Workflow import Workflow
from WMCore.WMBS.WMBSBase import WMBSBase

from WMCore.DataStructs.Subscription import Subscription as WMSubscription
from WMCore.DataStructs.Fileset      import Fileset      as WMFileset

from WMCore.Services.UUID import makeUUID

class Subscription(WMBSBase, WMSubscription):
    """
    WMBS Subscription

    WMBS object for executing a task or similar chunk of work
    """
    def __init__(self, fileset = None, workflow = None, id = -1,
                 split_algo = "FileBased", type = "Processing"):
        WMBSBase.__init__(self)

        # If a fileset or workflow isn't passed in the base class will create
        # empty non-WMBS filesets and workflows.  We want WMBS filesets and
        # workflows so we'll create those here.
        if fileset == None:
            fileset = Fileset()
        if workflow == None:
            workflow = Workflow()
            
        WMSubscription.__init__(self, fileset = fileset, workflow = workflow,
                                split_algo = split_algo, type = type)

        self.setdefault("id", id)
        return
        
    def create(self):
        """
        Add the subscription to the database
        """
        existingTransaction = self.beginTransaction()

        if self.exists() != False:
            self.load()
            return
        
        action = self.daofactory(classname = "Subscriptions.New")
        action.execute(fileset = self["fileset"].id, type = self["type"],
                       split_algo = self["split_algo"],
                       workflow = self["workflow"].id,
                       conn = self.getDBConn(),
                       transaction = self.existingTransaction())
        
        self.load()
        self.commitTransaction(existingTransaction)
        return
    
    def exists(self):
        """
        See if the subscription is in the database
        """
        action = self.daofactory(classname="Subscriptions.Exists")
        result = action.execute(fileset = self["fileset"].id,
                                workflow = self["workflow"].id,
                                conn = self.getDBConn(),
                                transaction = self.existingTransaction())
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
            action = self.daofactory(classname = "Subscriptions.LoadFromID")
            result = action.execute(id = self["id"],
                                    conn = self.getDBConn(),
                                    transaction = self.existingTransaction())
        else:
            action = self.daofactory(classname = "Subscriptions.LoadFromFilesetWorkflow")
            result = action.execute(fileset = self["fileset"].id,
                                    workflow = self["workflow"].id,
                                    conn = self.getDBConn(),
                                    transaction = self.existingTransaction())            

        self["type"] = result["type"]
        self["id"] = result["id"]
        self["split_algo"] = result["split_algo"]

        # Only load the fileset and workflow if they haven't been loaded
        # already.  
        if self["fileset"].id < 0:
            self["fileset"] = Fileset(id = result["fileset"])

        if self["workflow"].id < 0:
            self["workflow"] = Workflow(id = result["workflow"])
            
        self.commitTransaction(existingTransaction)
        return

    def loadData(self):
        """
        _loadData_

        Load all data having to do with the subscription including all the
        files contained in the fileset and the workflow meta data.
        """
        existingTransaction = self.beginTransaction()
        
        if self["id"] < 0 or self["fileset"].id < 0 or \
               self["workflow"].id < 0:
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

        action = self.daofactory(classname = "Subscriptions.AddValidation")
        result = action.execute(sites = sites,
                                conn = self.getDBConn(),
                                transaction = self.existingTransaction())        

        self.commitTransaction(existingTransaction)

    def getWhiteBlackList(self):
        """
        _getWhiteBlackList_

        Retrieve the white lists and black lists for this subscription.
        """
        existingTransaction = self.beginTransaction()

        action = self.daofactory(classname = "Subscriptions.GetValidation")
        result = action.execute(self["id"],
                                conn = self.getDBConn(),
                                transaction = self.existingTransaction())        

        self.commitTransaction(existingTransaction)
        return result
    
    def filesOfStatus(self, status, limit = 0, loadChecksums = True):
        """
        _filesOfStatus_
        
        Return a Set of File objects that have the given status with respect
        to this subscription.        
        """
        existingTransaction = self.beginTransaction()
        
        status = status.title()
        files  = set()
        if limit > 0:
            action = self.daofactory(classname = "Subscriptions.Get%sFilesByLimit" % status)
            fileList = action.execute(self["id"], limit, conn = self.getDBConn(),
                                      transaction = self.existingTransaction())
        else:
            action = self.daofactory(classname = "Subscriptions.Get%sFiles" % status)
            fileList = action.execute(self["id"], conn = self.getDBConn(),
                                      transaction = self.existingTransaction())
        
        fileInfoAct  = self.daofactory(classname = "Files.GetByID")
        fileInfoDict = fileInfoAct.execute(file = [x["file"] for x in fileList],
                                           conn = self.getDBConn(),
                                           transaction = self.existingTransaction())
        
        #Run through all files
        for f in fileList:
            fl = File(id = f['file'])
            if loadChecksums:
                fl.loadChecksum()
            fl.update(fileInfoDict[f['file']])
            if 'locations' in f.keys():
                fl.setLocation(f['locations'], immediateSave = False)
            files.add(fl)
            
        self.commitTransaction(existingTransaction)
        return files
    
    def acquireFiles(self, files = None):
        """
        _acuireFiles_
        
        Mark all files objects that are passed in as acquired for this
        subscription.  If now files are passed in then all available files
        will be acquired.
        """
        existingTransaction = self.beginTransaction()

        action = self.daofactory(classname = "Subscriptions.AcquireFiles")

        if not files:
            files = self.filesOfStatus("Available")
        elif type(files) == type(Fileset()) or type(files) == type(WMFileset()):
            pass
        else:
            files = self.makelist(files)

        if len(files) == 0:
            self.commitTransaction(existingTransaction)
            return

        action.execute(self['id'], file = [x["id"] for x in files],
                       conn = self.getDBConn(),
                       transaction = self.existingTransaction())
            
        try:
            self.commitTransaction(existingTransaction)
        except Exception, ex:
            print "Found exception %s" % (ex)
            logging.error("Exception found in commiting " \
                          + "acquireFiles transaction: %s" % (ex))
        return
    
    def completeFiles(self, files):
        """
        Mark a (set of) file(s) as completed.
        """
        existingTransaction = self.beginTransaction()

        files = self.makelist(files)
        
        completeAction = self.daofactory(classname = "Subscriptions.CompleteFiles")
        completeAction.execute(subscription = self["id"],
                               file = [x["id"] for x in files],
                               conn = self.getDBConn(),
                               transaction = self.existingTransaction())

        self.commitTransaction(existingTransaction)
        return
    
    def failFiles(self, files):
        """
        Mark a (set of) file(s) as failed. 
        """
        existingTransaction = self.beginTransaction()

        files = self.makelist(files)
        
        failAction = self.daofactory(classname = "Subscriptions.FailFiles")
        failAction.execute(subscription = self["id"],
                           file = [x["id"] for x in files],
                           conn = self.getDBConn(),
                           transaction = self.existingTransaction())
        
        self.commitTransaction(existingTransaction)
        return
    
    def getJobs(self):
        """
        Return a list of all the jobs associated with a subscription
        """
        jobsAction = self.daofactory(classname = "Subscriptions.Jobs")
        jobs = jobsAction.execute(subscription = self["id"],
                                  conn = self.getDBConn(),
                                  transaction = self.existingTransaction())

        return jobs
        
    def delete(self):
        """
        _delete_

        Delete this subscription from the database.
        """
        action = self.daofactory(classname = "Subscriptions.Delete")
        action.execute(id = self["id"], conn = self.getDBConn(),
                       transaction = self.existingTransaction())

        return
    
    def isCompleteOnRun(self, runID):
        """
        _isCompleteOnRun_
        
        Check all the files in the given subscripton and the given run are completed.
        
        To: check query whether performance can be improved
        """
        statusAction = self.daofactory(classname = "Subscriptions.IsCompleteOnRun")
        fileCount = statusAction.execute(self["id"], runID,
                                      conn = self.getDBConn(),
                                      transaction = self.existingTransaction())

        if fileCount == 0:
            return True
        else:
            return False
        
    def filesOfStatusByRun(self, status, runID):
        """
        _filesOfStatusByRun_
        
        Return all the files in the given subscription and the given run which
        have the given status.
        """
        existingTransaction = self.beginTransaction()

        files = []
        action = self.daofactory(classname = "Subscriptions.Get%sFilesByRun" % status)
        for f in action.execute(self["id"], runID, conn = self.getDBConn(),
                                transaction = self.existingTransaction()):
            fl = File(id = f["file"])
            fl.load()
            files.append(fl)

        self.commitTransaction(existingTransaction)
        return files 


    def getNumberOfJobsPerSite(self, location, state):
        """
        _getNumberOfJobsPerSite_
        
        Access the number of jobs at a site in a given status for a given subscription
        """
        jobLocate = self.daofactory(classname = "Subscriptions.GetNumberOfJobsPerSite")

        result = jobLocate.execute(location = location,
                                   subscription = self['id'],
                                   state = state).values()[0]
        return result
 
    def getJobGroups(self):
        """
        _getJobGroups_
        
        Returns a list of job group IDs associated with the subscription with new jobs
        """
        action = self.daofactory( classname = "Subscriptions.GetJobGroups" )
        return action.execute(self['id'], conn = self.getDBConn(),
                              transaction = self.existingTransaction())

    def getAllJobGroups(self):
        """
        _getAllJobGroups_
        
        Returns a list of ALL jobGroups associated with the subscription
        """
        action = self.daofactory( classname = "Subscriptions.GetAllJobGroups" )
        return action.execute(self['id'], conn = self.getDBConn(),
                              transaction = self.existingTransaction())

    def deleteEverything(self):
        """
        _deleteEverything_

        This function should delete the subscription, and absolutely everything else having anything
        to do with the subscription that is NOT in use by any other piece.  It should check for all
        the proper ownerships through a sequence of DAO calls that will take forever.

        Nothing except the taskArchiver should be calling this.
        """
        existingTransaction = self.beginTransaction()
        self.load()

        jobGroups = self.getAllJobGroups()


        filesets = []

        # The order here is important
        # You need to delete files and filesets from the bottom up
        # In order to not violate parentage

        # Get output filesets from jobGroups
        for jobGroupID in jobGroups:
            loadAction = self.daofactory(classname = "JobGroup.LoadFromID")
            result = loadAction.execute(jobGroupID, conn = self.getDBConn(),
                                        transaction = self.existingTransaction())
            filesets.append(result['output'])

        # Get output filesets from the workflow
        self['workflow'].load()
        for entry in self['workflow'].outputMap:
            wid = self['workflow'].outputMap[entry]["output_fileset"].id
            if not wid in filesets:
                filesets.append(wid)


        # Do the input fileset LAST!
        filesets.append(self['fileset'].id)


        #First, jobs
        deleteAction = self.daofactory(classname = "Jobs.Delete")
        jobDeleteList = []
        for job in self.getJobs():
            jobDeleteList.append(job['id'])
        if len(jobDeleteList) > 0:
            deleteAction.execute(id = jobDeleteList, conn = self.getDBConn(),
                                 transaction = self.existingTransaction())



        #Next jobGroups
        deleteAction = self.daofactory(classname = "JobGroup.Delete")
        for jobGroupID in jobGroups:
            deleteAction.execute(id = jobGroupID, conn = self.getDBConn(),
                             transaction = self.existingTransaction())



        for filesetID in filesets:
            fileset = Fileset(id = filesetID)

            # Load the files
            filesetFiles = []
            action  = self.daofactory(classname = "Files.InFileset")
            results = action.execute(fileset = filesetID,
                                     conn = self.getDBConn(),
                                     transaction = self.existingTransaction())

            for result in results:
                filesetFiles.append(result['fileid'])


            
            
            action = self.daofactory(classname = "Fileset.DeleteCheck")
            action.execute(fileid = fileset.id, subid = self["id"],
                           conn = self.getDBConn(),
                           transaction = self.existingTransaction())
            if not fileset.exists() and len(filesetFiles) > 0:
                # If we got rid of the fileset
                # If we did not delete the fileset, all files are still in use
                # Now get rid of unused files

                parent = self.daofactory(classname = "Files.DeleteParentCheck")
                action = self.daofactory(classname = "Files.DeleteCheck")
                
                parent.execute(file = filesetFiles, fileset = fileset.id,
                               conn = self.getDBConn(),
                               transaction = self.existingTransaction())
                action.execute(file = filesetFiles, fileset = fileset.id,
                               conn = self.getDBConn(),
                               transaction = self.existingTransaction())

        #Next Workflow
        action = self.daofactory(classname = "Workflow.DeleteCheck")
        action.execute(workid = self["workflow"].id, subid = self["id"],
                       conn = self.getDBConn(),
                       transaction = self.existingTransaction())

        self.delete()
        self.commitTransaction(existingTransaction)
        return
   
    def isFileCompleted(self, files):
        """
        _isFileCompleted_
        
        Returns True if all the given files are in complete status
        Return False if one of files are not in complete status
        """
        if type(files) != list:
            files = [files] 
        
        action = self.daofactory(classname = "Subscriptions.GetCompletedByFileList")
        fileIDs =  action.execute(self['id'], files, conn = self.getDBConn(),
                                  transaction = self.existingTransaction())
        
        for f in files:
            if f['id'] not in fileIDs:
                return False 
        
        return True


    def bulkCommit(self, jobGroups):
        """
        _bulkCommit_

        Commits all objects created during job splitting.  This is dangerous because it assumes
        that you can pass in all jobGroups.
        """

        jobList      = []
        jobGroupList = []
        nameList     = []

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

        for jobGroup in jobGroups:
            # Make a random name for each fileset
            nameList.append(makeUUID())

        # Create filesets
        action = self.daofactory(classname = "Fileset.BulkNewReturn")
        fsIDs  = action.execute(nameList = nameList, open = True,
                                conn = self.getDBConn(),
                                transaction = self.existingTransaction())

        for jobGroup in jobGroups:
            jobGroup.uid = makeUUID()
            jobGroupList.append({'subscription': self['id'],
                                 'uid': jobGroup.uid,
                                 'output': fsIDs.pop()})

        action = self.daofactory(classname = "JobGroup.BulkNewReturn")
        jgIDs  = action.execute(bulkInput = jobGroupList,
                                conn = self.getDBConn(),
                                transaction = self.existingTransaction())

        for jobGroup in jobGroups:
            for idUID in jgIDs:
                # This should assign an ID to the right job
                if jobGroup.uid == idUID['guid']:
                    jobGroup.id = idUID['id']
                    break

        for jobGroup in jobGroups:
            for job in jobGroup.newjobs:
                if job["id"] != None:
                    continue
            
                job["jobgroup"] = jobGroup.id

                if job["name"] == None:
                    job["name"] = makeUUID()
                jobList.append(job)


        bulkAction = self.daofactory(classname = "Jobs.New")
        result = bulkAction.execute(jobList = jobList, conn = self.getDBConn(),
                                    transaction = self.existingTransaction())

        #Move jobs to jobs from newjobs
        for jobGroup in jobGroups:
            jobGroup.jobs.extend(jobGroup.newjobs)
            jobGroup.newjobs = []

        #Use the results of the bulk commit to get the jobIDs
        fileDict = {}
        for job in jobList:
            job['id'] = result[job['name']]
            fileDict[job['id']] = []
            for f in job['input_files']:
                fileDict[job['id']].append(f['id'])


        maskAction = self.daofactory(classname = "Masks.New")
        maskAction.execute(jobList = jobList, conn = self.getDBConn(), 
                           transaction = self.existingTransaction())

        fileAction = self.daofactory(classname = "Jobs.AddFiles")
        fileAction.execute(jobDict = fileDict, conn = self.getDBConn(), 
                           transaction = self.existingTransaction())

        fileList = []
        for job in jobList:
            fileList.extend(job['input_files'])

        self.acquireFiles(files = fileList)
        self.commitTransaction(existingTransaction)
        return
