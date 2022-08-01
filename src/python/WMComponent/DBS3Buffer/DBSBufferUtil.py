#!/usr/bin/env python
"""
_DBSBufferUtil_

APIs related to using the DBSBuffer.
"""
from __future__ import print_function

from future.utils import viewitems

import threading
from collections import defaultdict

from WMCore.DAOFactory import DAOFactory
from WMComponent.DBS3Buffer.DBSBufferFile import DBSBufferFile
from WMCore.DataStructs.Run import Run
from WMCore.WMConnectionBase import WMConnectionBase


class DBSBufferUtil(WMConnectionBase):
    """
    APIs related to file addition for DBSBuffer

    """

    def __init__(self):

        myThread = threading.currentThread()
        self.daoFactory = DAOFactory(package="WMComponent.DBS3Buffer",
                                     logger=myThread.logger,
                                     dbinterface=myThread.dbi)

        return

    def loadDBSBufferFilesBulk(self, fileObjs):
        """
        _loadDBSBufferFilesBulk_

        Yes, this is a stupid place to put it.
        No, there's not better place.
        """
        dbsFiles = []

        binds = []
        for f in fileObjs:
            binds.append(f["id"])

        loadFiles = self.daoFactory(classname="DBSBufferFiles.LoadBulkFilesByID")
        results = loadFiles.execute(files=binds, transaction=False)

        for entry in results:
            # Add loaded information
            dbsfile = DBSBufferFile(id=entry['id'])
            dbsfile.update(entry)
            dbsFiles.append(dbsfile)

        for dbsfile in dbsFiles:
            if 'runInfo' in dbsfile:
                # Then we have to replace it with a real run
                for r in dbsfile['runInfo']:
                    run = Run(runNumber=r)
                    run.extend(dbsfile['runInfo'][r])
                    dbsfile.addRun(run)
                del dbsfile['runInfo']
            if 'parentLFNs' in dbsfile:
                # Then we have some parents
                for lfn in dbsfile['parentLFNs']:
                    newFile = DBSBufferFile(lfn=lfn)
                    dbsfile['parents'].add(newFile)
                del dbsfile['parentLFNs']

        return dbsFiles

    def findUploadableDAS(self):
        """
        _findUploadableDAS_

        Find all dataset_algo with uploadable files.
        """
        findDAS = self.daoFactory(classname="FindDASToUpload")
        result = findDAS.execute(transaction=False)

        return result

    def findOpenBlocks(self):
        """
        _findOpenBlocks_

        This should find all blocks.
        """
        openBlocks = self.daoFactory(classname="GetOpenBlocks")
        result = openBlocks.execute(transaction=False)

        return result

    def loadBlocksByDAS(self, das):
        """
        _loadBlocksByDAS_

        Given a DAS, find all the
        blocks associated with it in the
        Open status
        """
        findBlocks = self.daoFactory(classname="LoadBlocksByDAS")
        result = findBlocks.execute(das=das, transaction=False)

        return result

    def loadBlocks(self, blocknames):
        """
        _loadBlocks_

        Given a list of names, load the
        blocks with those names
        """

        if len(blocknames) < 1:
            # Nothing to do
            return []

        findBlocks = self.daoFactory(classname="LoadBlocks")
        result = findBlocks.execute(blocknames, transaction=False)

        return result

    def loadDataset(self, datasetName):
        """
        Given a dataset name, load its information from DBSBuffer
        :param datasetName: string with the dataset name
        :return: a dictionary with the dataset info
        """
        loadDataset = self.daoFactory(classname="ListDataset")
        result = loadDataset.execute(datasetPath=datasetName, transaction=False)
        if not result:
            # this should never happen, but just in case...
            return {}
        return result[0]

    def findUploadableFilesByDAS(self, datasetpath):
        """
        _findUploadableDAS_

        Find all the uploadable files for a given DatasetPath.
        """
        dbsFiles = []

        findFiles = self.daoFactory(classname="LoadDBSFilesByDAS")
        results = findFiles.execute(datasetpath=datasetpath, transaction=False)

        for entry in results:
            # Add loaded information
            dbsfile = DBSBufferFile(id=entry['id'])
            dbsfile.update(entry)
            dbsFiles.append(dbsfile)

        for dbsfile in dbsFiles:
            if 'runInfo' in dbsfile:
                # Then we have to replace it with a real run
                for r in dbsfile['runInfo']:
                    run = Run(runNumber=r)
                    run.extendLumis(dbsfile['runInfo'][r])
                    dbsfile.addRun(run)
                del dbsfile['runInfo']
            if 'parentLFNs' in dbsfile:
                # Then we have some parents
                for lfn in dbsfile['parentLFNs']:
                    newFile = DBSBufferFile(lfn=lfn)
                    dbsfile['parents'].add(newFile)
                del dbsfile['parentLFNs']

        return dbsFiles

    def loadFilesByBlock(self, blockname):
        """
        _loadFilesByBlock_

        Get all files associated with a block
        """
        dbsFiles = []

        findFiles = self.daoFactory(classname="LoadFilesByBlock")
        results = findFiles.execute(blockname=blockname, transaction=False)

        for entry in results:
            # Add loaded information
            dbsfile = DBSBufferFile(id=entry['id'])
            dbsfile.update(entry)
            dbsFiles.append(dbsfile)

        for dbsfile in dbsFiles:
            if 'runInfo' in dbsfile:
                # Then we have to replace it with a real run
                for r in dbsfile['runInfo']:
                    run = Run(runNumber=r)
                    run.extendLumis(dbsfile['runInfo'][r])
                    dbsfile.addRun(run)
                del dbsfile['runInfo']
            if 'parentLFNs' in dbsfile:
                # Then we have some parents
                for lfn in dbsfile['parentLFNs']:
                    newFile = DBSBufferFile(lfn=lfn)
                    dbsfile['parents'].add(newFile)
                del dbsfile['parentLFNs']

        return dbsFiles

    def getCompletedWorkflows(self):
        """
        _getCompletedWorkflows_
        get list of the workflows which are completed.
        completed here is not request manager completed status.
        It indicates all the tasks belongs to this workflow within the agent are completed
        """
        wfCompletedDAO = self.daoFactory(classname="GetCompletedWorkflows")
        result = wfCompletedDAO.execute(transaction=False)

        return result

    def getPhEDExDBSStatusForCompletedWorkflows(self, summary=False):

        """
        _getPhEDxDBSStatuForCompletedWorkflows_
        Check the PhEDEx and DBS upload status for the completed workflow
        """
        wfCompletedStatusDAO = self.daoFactory(classname="CheckStatusForCompletedWorkflows")
        result = wfCompletedStatusDAO.execute(transaction=False)
        if summary:
            result = self.summaryPhEDExDBSStatus(result)

        return result

    def summaryPhEDExDBSStatus(self, data):
        """
        data contains only workflows with completed (in agent not in reqmgr) status
        returns dictionary with kew as workflow and containing dbs/phedex upload status
        """
        summary = defaultdict(dict)
        for workflow, value in viewitems(data):
            # only getting completed workflows
            summary[workflow]["Completed"] = True

            if value["NotInPhEDEx"] == 0 and value["InPhEDEx"] >= 0:
                summary[workflow]["PhEDExInjected"] = True
            else:
                summary[workflow]["PhEDExInjected"] = False

            if value["NotInDBS"] == 0 and value["InDBS"] >= 0:
                summary[workflow]["DBSUploaded"] = True
            else:
                summary[workflow]["DBSUploaded"] = False
        return summary

    def isAllWorkflowCompleted(self):
        """
        check whether all the workflows are completed status in give agent (not globaly in reqmgr)
        This should be used as preconditon for checking draining status
        """
        completeFWFlag = self.daoFactory(classname="IsAllWorkflowsCompleted")
        return completeFWFlag.execute(transaction=False)

    def countOpenBlocks(self):
        """
        check to see if any blocks are open in DBS, reported in drain statistics
        """
        openBlockCount = self.daoFactory(classname="CountOpenBlocks")
        result = openBlockCount.execute()
        return result

    def countFilesByStatus(self, status):
        """
        get counts of files by status, reported in drain statistics
        """
        fileCount = self.daoFactory(classname="CountFilesByStatus")
        result = fileCount.execute(status)
        return result

    def countPhedexNotUploaded(self):
        """
        get counts of files not uploaded to phedex, reported in drain statistics
        """
        phedexCount = self.daoFactory(classname="CountPhedexNotUploaded")
        result = phedexCount.execute()
        return result
