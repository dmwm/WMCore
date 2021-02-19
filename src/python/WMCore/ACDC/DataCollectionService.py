#!/usr/bin/env python
# encoding: utf-8
"""
DataCollectionInterface.py

Created by Dave Evans on 2010-07-30.
Copyright (c) 2010 Fermilab. All rights reserved.
"""

from builtins import next
from future.utils import viewitems, listvalues

import logging
import threading
from operator import itemgetter

import WMCore.ACDC.CollectionTypes as CollectionTypes
import WMCore.Database.CouchUtils as CouchUtils
from WMCore.ACDC.CouchCollection import CouchCollection
from WMCore.ACDC.CouchFileset import CouchFileset
from WMCore.ACDC.CouchService import CouchService
from WMCore.DAOFactory import DAOFactory
from WMCore.DataStructs.File import File
from WMCore.DataStructs.LumiList import LumiList
from WMCore.DataStructs.Run import Run
from WMCore.WMException import WMException


def mergeFilesInfo(chunkFiles):
    """
    _mergeFilesInfo_

    Receive a list of dicts with acdc files information and merge them when
    it belongs to the same file. It also removes any possible duplicate lumi
    section (for the same file) in case ErrorHandler happened to upload the
    same job error document twice.
    """
    mergedFiles = {}

    # Merge ACDC docs without any real input data (aka MCFakeFile)
    if chunkFiles[0]['lfn'].startswith('MCFakeFile'):
        logging.info("Merging %d ACDC FakeFiles...", len(chunkFiles))
        for acdcFile in chunkFiles:
            fName = acdcFile['lfn']
            if fName not in mergedFiles:
                mergedFiles[fName] = acdcFile
            else:
                lumiSet = set(acdcFile['runs'][0]['lumis'])
                if lumiSet.issubset(set(mergedFiles[fName]['runs'][0]['lumis'])):
                    # every element in lumiSet is already in mergedFiles, it's a dup!
                    continue
                mergedFiles[fName]['events'] += acdcFile['events']
                mergedFiles[fName]['runs'][0]['lumis'].extend(acdcFile['runs'][0]['lumis'])
    else:
        logging.info("Merging %d real input files...", len(chunkFiles))
        for acdcFile in chunkFiles:
            fName = acdcFile['lfn']
            if fName not in mergedFiles:
                mergedFiles[fName] = acdcFile
            else:
                # if one run/lumi pair is there, then it's a duplicate job
                runNum = acdcFile['runs'][0]['run_number']
                lumiSet = set(acdcFile['runs'][0]['lumis'])
                if _isRunMaskDuplicate(runNum, lumiSet, mergedFiles[fName]['runs']):
                    continue
                # union of parents
                allParents = list(set(mergedFiles[fName]['parents']).union(acdcFile['parents']))
                mergedFiles[fName]['parents'] = allParents
                # just add up run/lumi pairs (don't try to merge them)
                mergedFiles[fName]['runs'].extend(acdcFile['runs'])
        _mergeRealDataRunLumis(mergedFiles)

    logging.info(" ... resulted in %d unique files.", len(mergedFiles))
    return listvalues(mergedFiles)


def _isRunMaskDuplicate(run, lumis, runLumis):
    """
    Test whether run and lumi is a subset of one of the
    runLumi pairs in runLumis
    :param run: integer run number
    :param lumi: set of lumis
    :param runLumis: list of dictionaries containing run and lumis
    """
    for runLumi in runLumis:
        if run == runLumi['run_number']:
            if lumis.issubset(runLumi['lumis']):
                return True
    return False


def _mergeRealDataRunLumis(mergedFiles):
    """
    Function to scan and merge run/lumi pairs in the same file, thus
    getting rid of duplicate lumi sections in real data.
    :param mergedFiles: list of dictionaries with ACDC file info
    :return: update data structure in place
    """
    for fname in mergedFiles:
        runLumis = {}
        for item in mergedFiles[fname]['runs']:
            runLumis.setdefault(item['run_number'], [])
            runLumis[item['run_number']].extend(item['lumis'])
        # now write those back to the original data structure
        mergedFiles[fname]['runs'] = []
        for run, lumis in viewitems(runLumis):
            mergedFiles[fname]['runs'].append({'run_number': run,
                                               'lumis': list(set(lumis))})
    return


def fixupMCFakeLumis(files, acdcVersion):
    """
    This is complicated!
    The lumi list uploaded to the ACDC Server is incorrect for, at least,
    the MCFakeFiles. Reason being that it also includes the LastLumi, which
    is actually not processed by the job, but just set as an upper boundary
    (inclusiveMask). However, if the job is the last one for a given MCFakeFile,
    then it might have a single lumi section in the list, and in this case it
    would be correct...

    This function updates data in-place.

    For more info, see issue: https://github.com/dmwm/WMCore/issues/9126
    """
    for fname, fvalues in viewitems(files):
        if fname.startswith('MCFakeFile') and acdcVersion < 2:
            for run in fvalues['runs']:
                if len(run['lumis']) > 1:
                    run['lumis'].pop()
        else:
            break


class ACDCDCSException(WMException):
    """
    Yet another dummy variable class

    """


class DataCollectionService(CouchService):
    def __init__(self, url, database, **opts):
        CouchService.__init__(self, url=url, database=database, **opts)

    @CouchUtils.connectToCouch
    def getDataCollection(self, collName):
        """
        _getDataCollection_

        Get a data collection by name
        """
        coll = CouchCollection(name=collName, database=self.database, url=self.url)

        coll.populate()
        return coll

    @CouchUtils.connectToCouch
    def failedJobs(self, failedJobs, useMask=True):
        """
        _failedJobs_

        Given a list of failed jobs, sort them into Filesets and record them

        NOTE: jobs must have a non-standard task, workflow attributes assigned to them.
        """
        # first we sort the list of dictionary by two keys: workflow then task
        failedJobs.sort(key=itemgetter('workflow'))
        failedJobs.sort(key=itemgetter('task'))

        previousWorkflow = ""
        previousTask = ""
        for job in failedJobs:
            try:
                workflow = job['workflow']
                taskName = job['task']
            except KeyError as ex:
                msg = "Missing required, non-standard key %s in job in ACDC.DataCollectionService" % (str(ex))
                logging.error(msg)
                raise ACDCDCSException(msg)

            if workflow != previousWorkflow:
                coll = CouchCollection(database=self.database, url=self.url,
                                       name=workflow,
                                       type=CollectionTypes.DataCollection)
            if taskName != previousTask:
                fileset = CouchFileset(database=self.database, url=self.url,
                                       name=taskName)
            coll.addFileset(fileset)
            inputFiles = job['input_files']
            for fInfo in inputFiles:
                if int(fInfo["merged"]) == 1:  # Looks like Oracle and MySQL return diff type
                    fInfo["parents"] = []
                elif fInfo.get("parents", []):
                    firstParent = next(iter(fInfo["parents"]))
                    if "/store/unmerged/" in firstParent:
                        # parents and input files are unmerged files - need to find merged ascendant
                        fInfo["parents"] = list(getMergedParents(fInfo["parents"]))
                    elif "MCFakeFile" in firstParent:
                        fInfo["parents"] = []
                    # other case, fInfo["parents"] all or merged parents
            if useMask:
                fileset.add(files=inputFiles, mask=job['mask'])
            else:
                fileset.add(files=inputFiles)

            previousWorkflow = workflow
            previousTask = taskName

        return

    @staticmethod
    def _sortLocationInPlace(fileInfo):
        fileInfo["locations"].sort()
        return fileInfo["locations"]

    @CouchUtils.connectToCouch
    def _getFilesetInfo(self, collectionName, filesetName, chunkOffset=None, chunkSize=None):
        """
        Fetches all the data from the ACDC Server that matches the collection
        and fileset names.
        """
        option = {"include_docs": True, "reduce": False}
        keys = [[collectionName, filesetName]]
        results = self.couchdb.loadView("ACDC", "coll_fileset_docs", option, keys)

        filesInfo = []
        for row in results["rows"]:
            files = row["doc"].get("files", [])
            fixupMCFakeLumis(files, row['doc'].get("acdc_version", 1))
            filesInfo.extend(listvalues(files))

        # second lfn sort
        filesInfo.sort(key=lambda x: x["lfn"])
        # primary location sort (python preserve sort result)
        filesInfo.sort(key=lambda x: "".join(self._sortLocationInPlace(x)))

        if chunkOffset is not None and chunkSize is not None:
            return filesInfo[chunkOffset: chunkOffset + chunkSize]
        else:
            return filesInfo

    @CouchUtils.connectToCouch
    def chunkFileset(self, collectionName, filesetName, chunkSize=100):
        """
        _chunkFileset_

        Split all of the fileset in a given collection/task into chunks.  This
        will return a list of dictionaries that contain the offset into the
        fileset and a summary of files/events/lumis that are in the fileset
        chunk.
        """
        chunks = []
        results = self._getFilesetInfo(collectionName, filesetName)

        totalFiles = 0
        currentLocation = None
        numFilesInBlock = 0
        numLumisInBlock = 0
        numEventsInBlock = 0

        for value in results:
            if currentLocation is None:
                currentLocation = value["locations"]
            if numFilesInBlock == chunkSize or currentLocation != value["locations"]:
                chunks.append({"offset": totalFiles, "files": numFilesInBlock,
                               "events": numEventsInBlock, "lumis": numLumisInBlock,
                               "locations": currentLocation})
                totalFiles += numFilesInBlock
                currentLocation = value["locations"]
                numFilesInBlock = 0
                numLumisInBlock = 0
                numEventsInBlock = 0

            numFilesInBlock += 1
            lumis = 0
            for runLumi in value["runs"]:
                lumis += len(runLumi["lumis"])
            numLumisInBlock += lumis
            numEventsInBlock += value["events"]

        if numFilesInBlock > 0:
            chunks.append({"offset": totalFiles, "files": numFilesInBlock,
                           "events": numEventsInBlock, "lumis": numLumisInBlock,
                           "locations": currentLocation})
        return chunks

    @CouchUtils.connectToCouch
    def singleChunkFileset(self, collectionName, filesetName):
        """
        _singleChunkFileset_

        Put all of the fileset in a given collection/task into a single chunk.  This
        will return a dictionary that contains the offset into the
        fileset and a summary of files/events/lumis that are in the fileset
        chunk.
        """

        files = self._getFilesetInfo(collectionName, filesetName)

        locations = set()
        numFilesInBlock = 0
        numLumisInBlock = 0
        numEventsInBlock = 0

        for fileInfo in files:
            locations |= set(fileInfo["locations"])

            numFilesInBlock += 1
            lumis = 0
            for runLumi in fileInfo["runs"]:
                lumis += len(runLumi["lumis"])
            numLumisInBlock += lumis
            numEventsInBlock += fileInfo["events"]

        return {"offset": 0, "files": numFilesInBlock,
                "events": numEventsInBlock, "lumis": numLumisInBlock,
                "locations": locations}

    @CouchUtils.connectToCouch
    def getChunkInfo(self, collectionName, filesetName, chunkOffset, chunkSize):
        """
        _getChunkInfo_

        Retrieve metadata for a particular chunk.
        """

        files = self._getFilesetInfo(collectionName, filesetName, chunkOffset, chunkSize)

        totalFiles = 0
        currentLocation = None
        numFilesInBlock = 0
        numLumisInBlock = 0
        numEventsInBlock = 0

        for fileInfo in files:
            if currentLocation is None:
                currentLocation = fileInfo["locations"]

            numFilesInBlock += 1
            lumis = 0
            for runLumi in fileInfo["runs"]:
                lumis += len(runLumi["lumis"])
            numLumisInBlock += lumis
            numEventsInBlock += fileInfo["events"]

        return {"offset": totalFiles, "files": numFilesInBlock,
                "events": numEventsInBlock, "lumis": numLumisInBlock,
                "locations": currentLocation}

    @CouchUtils.connectToCouch
    def getChunkFiles(self, collectionName, filesetName, chunkOffset, chunkSize=100):
        """
        _getChunkFiles_

        Retrieve a chunk of files from the given collection and task.
        """
        chunkFiles = []
        files = self._getFilesetInfo(collectionName, filesetName, chunkOffset, chunkSize)

        files = mergeFilesInfo(files)
        for fileInfo in files:
            newFile = File(lfn=fileInfo["lfn"], size=fileInfo["size"],
                           events=fileInfo["events"], parents=set(fileInfo["parents"]),
                           locations=set(fileInfo["locations"]), merged=fileInfo["merged"])
            for run in fileInfo["runs"]:
                newRun = Run(run["run_number"])
                newRun.extendLumis(run["lumis"])
                newFile.addRun(newRun)

            chunkFiles.append(newFile)

        return chunkFiles

    @CouchUtils.connectToCouch
    def getProductionACDCInfo(self, collectionID, taskName):
        """
        _getFileInfo_

        Query ACDC for all of the files in the given collection and task.
        Return an entry for each file with lumis and event info.
        Format is:
        [{'lfn': 'someLfn',
          'first_event': XXX,
          'lumis': [lumi0,lumi1],
          'events': XXX}]
        """
        files = self._getFilesetInfo(collectionID, taskName)
        files = mergeFilesInfo(files)

        acdcInfo = []
        for value in files:
            fileInfo = {"lfn": value["lfn"],
                        "first_event": value["first_event"],
                        "lumis": value["runs"][0]["lumis"],
                        "events": value["events"]}
            acdcInfo.append(fileInfo)
        return acdcInfo

    @CouchUtils.connectToCouch
    def getLumiWhitelist(self, collectionID, taskName):
        """
        _getLumiWhitelist_

        Query ACDC for all of the files in the given collection and task.
        Generate a run and lumi whitelist for the given files with the following
        format:
          {"run1": [[lumi1, lumi4], [lumi6, lumi10]],
           "run3": [lumi5, lumi10]}

        Note that the run numbers are strings.
        """

        files = self._getFilesetInfo(collectionID, taskName)

        allRuns = {}
        whiteList = {}

        for fileInfo in files:
            for run in fileInfo["runs"]:
                if run["run_number"] not in allRuns:
                    allRuns[run["run_number"]] = []
                allRuns[run["run_number"]].extend(run["lumis"])

        for run in allRuns:
            lumis = []
            lumis.extend(set(allRuns[run]))
            lumis.sort()

            whiteList[str(run)] = []
            lastLumi = None
            currentSet = None

            while len(lumis) > 0:
                currentLumi = lumis.pop(0)
                if currentLumi - 1 != lastLumi:
                    if currentSet is None:
                        currentSet = [currentLumi]
                    else:
                        currentSet.append(lastLumi)
                        whiteList[str(run)].append(currentSet)
                        currentSet = [currentLumi]

                lastLumi = currentLumi

            currentSet.append(lastLumi)
            whiteList[str(run)].append(currentSet)

        return whiteList

    def getLumilistWhitelist(self, collectionID, taskName):
        """
        Args:
            collectionID, taskName: Parameters for getLumiWhitelist

        Returns: a LumiList object describing the lumi list from the collection
        """

        lumiList = LumiList(compactList=self.getLumiWhitelist(collectionID, taskName))
        return lumiList


def getMergedParents(childLFNs):
    myThread = threading.currentThread()
    daoFactory = DAOFactory(package="WMCore.WMBS", logger=myThread.logger,
                            dbinterface=myThread.dbi)

    getParentInfoAction = daoFactory(classname="Files.GetParentInfo")

    parentsInfo = getParentInfoAction.execute(childLFNs)
    newParents = set()
    unmergedParents = set()
    for parentInfo in parentsInfo:
        # This will catch straight to merge files that do not have redneck
        # parents.  We will mark the straight to merge file from the job
        # as a child of the merged parent.
        if int(parentInfo["merged"]) == 1:
            newParents.add(parentInfo["lfn"])
        else:
            unmergedParents.add(parentInfo["lfn"])

    if len(unmergedParents) > 0:
        grandParentSet = getMergedParents(unmergedParents)
        newParents.union(grandParentSet)

    return newParents
