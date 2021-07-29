#!/usr/bin/env python
"""
File       : MSUnmerged.py

Description:

This MicroService is meant to remove all no longer needed files from the unmerged
area of the CMS LFN Namespace. The cleanup process is supposed to happen on a per
RSE basis.
"""

# futures
from __future__ import division, print_function

from pprint import pformat
from time import time

import random
import re
import os
import sys
import errno
import stat
import gfal2

# WMCore modules
from WMCore.MicroService.DataStructs.DefaultStructs import UNMERGED_REPORT
from WMCore.MicroService.MSCore import MSCore
from WMCore.MicroService.MSUnmerged.MSUnmergedRSE import MSUnmergedRSE
from WMCore.Services.RucioConMon.RucioConMon import RucioConMon
from WMCore.Services.WMStatsServer.WMStatsServer import WMStatsServer
# from WMCore.Services.AlertManager.AlertManagerAPI import AlertManagerAPI
from WMCore.WMException import WMException
from Utils.Pipeline import Pipeline, Functor
from Utils.TwPrint import twFormat

# from memory_profiler import profile


class MSUnmergedException(WMException):
    """
    General Exception Class for MSUnmerged Module in WMCore MicroServices
    """
    def __init__(self, message=""):
        self.message = "MSUnmergedException: %s" % message
        super(MSUnmergedException, self).__init__(self.message)


class MSUnmergedPlineExit(MSUnmergedException):
    """
    An Exception Class for MSUnmerged to signal an expected pipeline exit condition
    This one should not produce any back trace dump.
    """
    def __init__(self, message=""):
        self.message = "MSUnmergedPlineExit: %s" % message
        super(MSUnmergedPlineExit, self).__init__(self.message)


class MSUnmerged(MSCore):
    """
    MSUnmerged.py class provides the logic for cleaning the unmerged area of
    the CMS LFN Namespace.
    """

    # @profile
    def __init__(self, msConfig, logger=None):
        """
        Runs the basic setup and initialization for the MSUnmerged module
        :param msConfig: micro service configuration
        """
        super(MSUnmerged, self).__init__(msConfig, logger=logger)

        self.msConfig.setdefault("verbose", True)
        self.msConfig.setdefault("interval", 60)
        self.msConfig.setdefault("limitFilesPerRSE", 200)
        self.msConfig.setdefault("skipRSEs", [])
        self.msConfig.setdefault("rseExpr", "*")
        self.msConfig.setdefault("rucioConMon", "https://cmsweb-testbed.cern.ch/rucioconmon/")
        self.msConfig.setdefault("enableRealMode", False)
        self.msConfig.setdefault("dumpRSE", False)
        self.msConfig.setdefault("gfalLogLevel", 'normal')
        self.msConfig.setdefault("dirFilterIncl", [])
        self.msConfig.setdefault("dirFilterExcl", [])
        # TODO: Add 'alertManagerUrl' to msConfig'
        # self.alertServiceName = "ms-unmerged"
        # self.alertManagerAPI = AlertManagerAPI(self.msConfig.get("alertManagerUrl", None), logger=logger)

        # Instantiating the Rucio Consistency Monitor Client
        self.rucioConMon = RucioConMon(self.msConfig['rucioConMon'], logger=self.logger)

        self.wmstatsSvc = WMStatsServer(self.msConfig['wmstatsUrl'], logger=self.logger)

        # Building all the Pipelines:
        pName = 'plineUnmerged'
        self.plineUnmerged = Pipeline(name=pName,
                                      funcLine=[Functor(self.updateRSETimestamps, start=True, end=False),
                                                Functor(self.consRecordAge),
                                                Functor(self.getUnmergedFiles),
                                                Functor(self.filterUnmergedFiles),
                                                Functor(self.getPfn),
                                                Functor(self.cleanRSE),
                                                Functor(self.updateRSECounters, pName),
                                                Functor(self.updateRSETimestamps, start=False, end=True),
                                                Functor(self.purgeRseObj, dumpRSE=self.msConfig['dumpRSE'])])
        # Initialization of the deleted files counters:
        self.rseCounters = {}
        self.plineCounters = {}
        self.rseTimestamps = {}
        self.rseConsStats = {}
        self.protectedLFNs = []

        # The basic /store/unmerged regular expression:
        self.regStoreUnmergedLfn = re.compile("^/store/unmerged/.*$")
        self.regStoreUnmergedPfn = re.compile("^.+/store/unmerged/.*$")

    # @profile
    def execute(self):
        """
        Executes the whole MSUnmerged logic
        :return: summary
        """
        # start threads in MSManager which should call this method
        summary = dict(UNMERGED_REPORT)

        # refresh statistics on every poling cycle

        try:
            self.rseConsStats = self.rucioConMon.getRSEStats()
            # self.logger.debug("RucioConMon Stats: %s", pformat(self.rseConsStats))
            self.protectedLFNs = set(self.wmstatsSvc.getProtectedLFNs())
            # self.logger.debug("protectedLFNs: %s", pformat(self.protectedLFNs))

            if not self.protectedLFNs:
                msg = "Could not fetch list with protectedLFNs from WMStatServer. "
                msg += "Skipping the current run."
                msg += "\nTotal number of RSEs processed: %s."
                msg += "\nTotal number of files fetched from RucioConMon: %s."
                msg += "\nNumber of RSEs cleaned: %s."
                msg += "\nNumber of files deleted: %s."
                self.logger.info(msg,
                                 0,
                                 0,
                                 0,
                                 0)
                self.updateReportDict(summary, "total_num_rses", 0)
                self.updateReportDict(summary, "total_num_files", 0)
                self.updateReportDict(summary, "num_rses_cleaned", 0)
                self.updateReportDict(summary, "num_files_deleted", 0)
                return summary
        except Exception as ex:
            msg = "Unknown exception while running MSUnmerged thread Error: %s"
            self.logger.exception(msg, str(ex))
            self.updateReportDict(summary, "error", msg)

        try:
            rseList = self.getRSEList()
            msg = "Retrieved list of %s RSEs: %s "
            msg += "Service set to process up to %s RSEs per instance."
            self.logger.info(msg, len(rseList), pformat(rseList), self.msConfig["limitRSEsPerInstance"])
            random.shuffle(rseList)
        except Exception as err:  # general error
            msg = "Unknown exception while trying to estimate the final RSEs to work on. Error: %s", str(err)
            self.logger.exception(msg)
            self.updateReportDict(summary, "error", msg)

        try:
            totalNumRses, totalNumFiles, numRsesCleaned, numFilesDeleted = self._execute(rseList)
            msg = "\nTotal number of RSEs processed: %s."
            msg += "\nTotal number of files fetched from RucioConMon: %s."
            msg += "\nNumber of RSEs cleaned: %s."
            msg += "\nNumber of files deleted: %s."
            self.logger.info(msg,
                             totalNumRses,
                             totalNumFiles,
                             numRsesCleaned,
                             numFilesDeleted)
            self.updateReportDict(summary, "total_num_rses", totalNumRses)
            self.updateReportDict(summary, "total_num_files", totalNumFiles)
            self.updateReportDict(summary, "num_rses_cleaned", numRsesCleaned)
            self.updateReportDict(summary, "num_files_deleted", numFilesDeleted)
        except Exception as ex:
            msg = "Unknown exception while running MSUnmerged thread Error: %s"
            self.logger.exception(msg, str(ex))
            self.updateReportDict(summary, "error", msg)

        return summary

    # @profile
    def _execute(self, rseList):
        """
        Executes the MSUnmerged pipelines
        :param rseList: A list of RSEs to work on
        :return:        a tuple with:
                            total number of RSEs
                            total number of files found for deletion
                            number of RSEs cleaned
                            number of files deleted
        """

        pline = self.plineUnmerged
        self.resetCounters(plineName=pline.name)
        self.plineCounters[pline.name]['totalNumRses'] = len(rseList)

        for rseName in rseList:
            try:
                if rseName not in self.msConfig['skipRSEs']:
                    pline.run(MSUnmergedRSE(rseName))
                else:
                    msg = "%s: Run on RSE: %s is skipped due to a restriction set in msConfig. "
                    msg += "Will NOT retry until the RSE is removed from 'skipRSEs' list."
                    self.logger.info(msg, pline.name, rseName)
                    continue
            except MSUnmergedPlineExit as ex:
                msg = "%s: Run on RSE: %s was interrupted due to: %s "
                msg += "Will retry again in the next cycle."
                self.logger.warning(msg, pline.name, rseName, ex.message)
                continue
            except Exception as ex:
                msg = "%s: General error from pipeline. RSE: %s. Error: %s "
                msg += "Will retry again in the next cycle."
                self.logger.exception(msg, pline.name, rseName, str(ex))
                continue
        return self.plineCounters[pline.name]['totalNumRses'], \
            self.plineCounters[pline.name]['totalNumFiles'], \
            self.plineCounters[pline.name]['rsesCleaned'], \
            self.plineCounters[pline.name]['deletedSuccess']

    # @profile
    def cleanRSE(self, rse):
        """
        The method to implement the actual deletion of files for an RSE.
        :param rse: MSUnmergedRSE object to be cleaned
        :return:    The MSUnmergedRSE object
        """

        # Create the gfal2 context object:
        try:
            ctx = gfal2.creat_context()
            gfal2.set_verbose(gfal2.verbose_level.names[self.msConfig['gfalLogLevel']])
        except Exception as ex:
            msg = "RSE: %s, Failed to create gfal2 Context object. " % rse['name']
            msg += "Skipping it in the current run."
            self.logger.exception(msg)
            raise MSUnmergedPlineExit(msg)

        # Start cleaning one directory at a time:
        for dirLfn, fileLfnGen in rse['files']['toDelete'].items():
            if self.msConfig['limitFilesPerRSE'] < 0 or \
               rse['counters']['filesToDelete'] < self.msConfig['limitFilesPerRSE']:

                # First increment the dir counter:
                rse['counters']['dirsToDelete'] += 1

                # Now we consume the rse['files']['toDelete'][dirLfn] generator
                # upon that no values will be left in it. In case we need it again
                # we will have to recreate the filter as we did in self.filterUnmergedFiles()
                pfnList = []
                if not rse['pfnPrefix']:
                    # Fall back to calling Rucio on a per directory basis for
                    # resolving the lfn to pfn mapping
                    dirPfn = self.rucio.getPFN(rse['name'], dirLfn, operation='delete')[dirLfn]
                    for fileLfn in fileLfnGen:
                        fileLfnSuffix = fileLfn.split(dirLfn)[1]
                        filePfn = dirPfn + fileLfnSuffix
                        pfnList.append(filePfn)
                else:
                    # Proceed with assembling the full filePfn out of the rse['pfnPrefix'] and the fileLfn
                    dirPfn = rse['pfnPrefix'] + dirLfn
                    for fileLfn in fileLfnGen:
                        filePfn = rse['pfnPrefix'] + fileLfn
                        pfnList.append(filePfn)

                rse['counters']['filesToDelete'] += len(pfnList)
                msg = "\nRSE: %s \nDELETING: %s."
                msg += "\nPFN list with: %s entries: \n%s"
                self.logger.debug(msg, rse['name'], dirLfn, len(pfnList), twFormat(pfnList, maxLength=4))

                if self.msConfig['enableRealMode']:
                    try:
                        # execute the actual deletion in bulk - full list of files per directory
                        delResult = []
                        delResult = ctx.unlink(pfnList)

                        # Count all the successfully deleted files (if a deletion was
                        # successful a value of None is put in the delResult list):
                        deletedSuccess = [pfnStatus for pfnStatus in delResult if pfnStatus is None]
                        self.logger.debug("RSE: %s, Dir: %s, deletedSuccess: %s",
                                          rse['name'], dirLfn, deletedSuccess)
                        rse['counters']['deletedSuccess'] += len(deletedSuccess)

                        # Now clean the whole branch
                        self.logger.debug("Purging dirEntry: %s:\n", dirPfn)
                        purgeSuccess = self._purgeTree(ctx, dirPfn)
                        if not purgeSuccess:
                            msg = "RSE: %s Failed to purge nonEmpty directory: %s"
                            self.logger.error(msg, rse['name'], dirPfn)
                    except Exception as ex:
                        msg = "Error while cleaning RSE: %s. "
                        msg += "Will retry in the next cycle. Err: %s"
                        self.logger.warning(msg, rse['name'], str(ex))
        rse['isClean'] = self._checkClean(rse)

        return rse

    def _purgeTree(self, ctx, baseDirPfn):
        """
        A method to be used for purging the tree bellow a specific branch.
        It deletes every empty directory bellow that branch + the origin at the end.
        :param ctx:  The gfal2 context object
        :return:     Bool: True if it managed to purge everything, False otherwise
        """
        successList = []

        if baseDirPfn[-1] != '/':
            baseDirPfn += '/'

        for dirEntry in ctx.listdir(baseDirPfn):
            if dirEntry in ['.', '..']:
                continue
            dirEntryPfn = baseDirPfn + dirEntry
            try:
                entryStat = ctx.stat(dirEntryPfn)
            except gfal2.GError:
                e = sys.exc_info()[1]
                if e.code == errno.ENOENT:
                    self.logger.error("MISSING dirEntry: %s", dirEntryPfn)
                    successList.append(False)
                    return all(successList)
                else:
                    self.logger.error("FAILED dirEntry: %s", dirEntryPfn)
                    raise
            if stat.S_ISDIR(entryStat.st_mode):
                successList.append(self._purgeTree(ctx, dirEntryPfn))

        try:
            success = ctx.rmdir(baseDirPfn)
            # for gfal2 rmdir() exit status of 0 is success
            if success == 0:
                successList.append(True)
            else:
                successList.append(False)
            self.logger.debug("RM baseDir: %s", baseDirPfn)
        except gfal2.GError:
            e = sys.exc_info()[1]
            if e.code == errno.ENOENT:
                self.logger.error("MISSING baseDir: %s", baseDirPfn)
            else:
                self.logger.error("FAILED basedir: %s", baseDirPfn)
                raise
        return all(successList)

    def _checkClean(self, rse):
        """
        A simple function to check if every file in an RSE's unmerged area have
        been deleted
        :param rse: The RSE to be checked
        :return:    Bool: True if all files found have been deleted, False otherwise
        """
        return rse['counters']['filesToDelete'] == rse['counters']['deletedSuccess']

    def consRecordAge(self, rse):
        """
        A method to heck the duration of the consistency record for the RSE
        :param rse: The RSE to be checked
        :return:    rse or raises MSUnmergedPlineExit
        """
        rseName = rse['name']

        if rseName not in self.rseConsStats:
            msg = "RSE: %s Missing in stats records at Rucio Consistency Monitor. " % rseName
            msg += "Skipping it in the current run."
            self.logger.warning(msg)
            raise MSUnmergedPlineExit(msg)

        isConsDone = self.rseConsStats[rseName]['status'] == 'done'
        isConsNewer = self.rseConsStats[rseName]['end_time'] > self.rseTimestamps[rseName]['prevStartTime']
        isRootFailed = self.rseConsStats[rseName]['root_failed']

        if not isConsNewer:
            msg = "RSE: %s With old consistency record in Rucio Consistency Monitor. " % rseName
            msg += "Skipping it in the current run."
            self.logger.info(msg)
            raise MSUnmergedPlineExit(msg)
        if not isConsDone:
            msg = "RSE: %s In non-final state in Rucio Consistency Monitor. " % rseName
            msg += "Skipping it in the current run."
            self.logger.warning(msg)
            raise MSUnmergedPlineExit(msg)
        if isRootFailed:
            msg = "RSE: %s With failed root in Rucio Consistency Monitor. " % rseName
            msg += "Skipping it in the current run."
            self.logger.warning(msg)
            raise MSUnmergedPlineExit(msg)

        return rse

    # @profile
    def getUnmergedFiles(self, rse):
        """
        Fetches all the records of unmerged files per RSE from Rucio Consistency Monitor
        and cuts everything to a certain level in the path and puts the list in the rse obj.

        Path example:
        /store/unmerged/Run2016B/JetHT/MINIAOD/ver2_HIPM_UL2016_MiniAODv2-v2/140000/388E3DEF-9F15-D04C-B582-7DD036D9DD33.root

        Where:
        /store/unmerged/                       - root unmerged area
        /Run2016B                              - acquisition era
        /JetHT                                 - primary dataset
        /MINIAOD                               - data tier
        /ver2_HIPM_UL2016_MiniAODv2-v2         - processing string + processing version
        /140000/388E3DEF-...-7DD036D9DD33.root - to be cut off

        :param rse: The RSE to work on
        :return:    rse
        """
        rse['files']['allUnmerged'] = self.rucioConMon.getRSEUnmerged(rse['name'])
        for filePath in rse['files']['allUnmerged']:
            rse['counters']['totalNumFiles'] += 1
            # Check if what we start with is under /store/unmerged/*
            if self.regStoreUnmergedLfn.match(filePath):
                # Cut the path to the deepest level known to WMStats protected LFNs
                dirPath = self._cutPath(filePath)
                # Check if what is left is still under /store/unmerged/*
                if self.regStoreUnmergedLfn.match(dirPath):
                    # Add it to the set of allUnmerged
                    rse['dirs']['allUnmerged'].add(dirPath)
        return rse

    def _cutPath(self, filePath):
        """
        Cuts a file path to the deepest level known to WMStats protected LFNs
        :param filePath:   The full (absolute) file path together with the file name
        :return finalPath: The final path cut the to correct level
        """
        # pylint: disable=E1120
        # This is a known issue when when passing an unpacked list to a method expecting
        # at least one variable. In this case the signature of the method breaking the
        # rule is:
        # os.path.join(*newPath) != os.path.join(a, *p)

        # Split the initial filePath into chunks and fill it into a dictionary
        # containing only directory names and the root of the path e.g.
        # ['/', 'store', 'unmerged', 'RunIISummer20UL17SIM', ...]
        newPath = []
        root = filePath
        while True:
            root, tail = os.path.split(root)
            if tail:
                newPath.append(tail)
            else:
                newPath.append(root)
                break
        newPath.reverse()
        # Cut/slice the path to the level/element required.
        newPath = newPath[:7]
        # Build the path out of all that is found up to the deepest level in the LFN tree
        finalPath = os.path.join(*newPath)
        return finalPath

    # @profile
    def filterUnmergedFiles(self, rse):
        """
        This method is applying set compliment operation to the set of unmerged
        files per RSE in order to exclude the protected LFNs.
        :param rse: The RSE to work on
        :return:    rse
        """
        rse['dirs']['toDelete'] = rse['dirs']['allUnmerged'] - self.protectedLFNs
        rse['dirs']['protected'] = rse['dirs']['allUnmerged'] & self.protectedLFNs

        # The following check may seem redundant, but better stay safe than sorry
        if not (rse['dirs']['toDelete'] | rse['dirs']['protected']) == rse['dirs']['allUnmerged']:
            rse['counters']['toDelete'] = -1
            msg = "Incorrect set check while trying to estimate the final set for deletion."
            raise MSUnmergedPlineExit(msg)

        # Get rid of 'allUnmerged' directories
        rse['dirs']['allUnmerged'].clear()

        # NOTE: Here we may want to filter out all protected files from allUnmerged and leave just those
        #       eligible for deletion. This will minimize the iteration time of the filters
        #       from toDelete later on.
        # while rse['files']['allUnmerged'

        # Now create the filters for rse['files']['toDelete'] - those should be pure generators
        # A simple generator:
        def genFunc(pattern, iterable):
            for i in iterable:
                if i.startswith(pattern):
                    yield i

        # NOTE: If the 'dirFilterIncl' is non empty then the cleaning process will
        #       be enclosed only in this part of the tree and will ignore anything
        #       from /store/unmerged/ which does not belong to the included filter
        # NOTE: 'dirFilterExcl' is always applied.

        # Merge the additional filters into a final set to be applied:
        dirFilterIncl = set(self.msConfig['dirFilterIncl'])
        dirFilterExcl = set(self.msConfig['dirFilterExcl'])
        dirFilterAll = dirFilterIncl - dirFilterExcl

        # Populate the filters:
        for dirName in rse['dirs']['toDelete']:
            # apply additional filters:
            if dirFilterAll:
                for dirFilter in dirFilterAll:
                    if dirName.startswith(dirFilter):
                        rse['files']['toDelete'][dirName] = genFunc(dirName, rse['files']['allUnmerged'])
            else:
                if dirFilterExcl:
                    for dirFilter in dirFilterExcl:
                        if not dirName.startswith(dirFilter):
                            rse['files']['toDelete'][dirName] = genFunc(dirName, rse['files']['allUnmerged'])
                else:
                    rse['files']['toDelete'][dirName] = genFunc(dirName, rse['files']['allUnmerged'])

        # Update the counters:
        rse['counters']['dirsToDeleteAll'] = len(rse['files']['toDelete'])
        return rse

    def getPfn(self, rse):
        """
        A method for fetching the common Pfn (method + hostname + global path)
        for the RSE. It uses Rucio client method lfns2pfns for one of the LFNs
        already recorded in the RSE in order to resolve the lfn to pfn mapping
        and then tries to parse the resultant pfn and cut off the lfn part.
        :param rse: The RSE to be checked
        :return:    rse
        """
        # NOTE:  pfnPrefix here is considered the full part of the pfn up to the
        #        beginning of the lfn part rather than just the protocol prefix
        if rse['files']['allUnmerged']:
            lfn = next(iter(rse['files']['allUnmerged']))
            pfnDict = self.rucio.getPFN(rse['name'], lfn, operation='delete')
            pfnFull = pfnDict[lfn]
            if self.regStoreUnmergedPfn.match(pfnFull):
                pfnPrefix = pfnFull.split('/store/unmerged/')[0]
                rse['pfnPrefix'] = pfnPrefix
            else:
                msg = "Could not establish the correct pfn Prefix for RSE: %s. " % rse['name']
                msg += "Will fall back to calling Rucio on a directory basis for lfn to pfn resolution."
                self.logger.warning(msg)
        return rse

    # @profile
    def purgeRseObj(self, rse, dumpRSE=False):
        """
        Cleaning all the records in an RSE object. The final method to be used
        before an RSE exits a pipeline.
        :param rse: The RSE to be checked
        :return:    rse
        """
        msg = "\n----------------------------------------------------------"
        msg += "\nMSUnmergedRSE: \n%s"
        msg += "\n----------------------------------------------------------"
        if dumpRSE:
            self.logger.debug(msg, pformat(rse))
        else:
            self.logger.debug(msg, twFormat(rse, maxLength=6))
        rse.clear()
        return rse

    def updateRSETimestamps(self, rse, start=True, end=True):
        """
        Update/Upload all timestamps for the rse object into the MSUnmerged
        service counters
        :param rse:   The RSE to work on
        :return:      rse
        """
        rseName = rse['name']
        currTime = time()

        if rseName not in self.rseTimestamps:
            self.rseTimestamps[rseName] = {'prevStartTime': 0.0,
                                           'startTime': 0.0,
                                           'prevEndtime': 0.0,
                                           'endTime': 0.0}
        if start:
            self.rseTimestamps[rseName]['prevStartTime'] = self.rseTimestamps[rseName]['startTime']
            self.rseTimestamps[rseName]['startTime'] = currTime
        if end:
            self.rseTimestamps[rseName]['prevEndtime'] = self.rseTimestamps[rseName]['endTime']
            self.rseTimestamps[rseName]['endtime'] = currTime
        return rse

    def updateRSECounters(self, rse, pName):
        """
        Update/Upload all counters from the rse object into the MSUnmerged
        service counters
        :param rse:   The RSE to work on
        :param pName: The pipeline name whose counters to be updated
        :return:      rse
        """
        rseName = rse['name']
        self.resetCounters(rseName=rseName)
        self.rseCounters[rseName]['totalNumFiles'] = rse['counters']['totalNumFiles']
        self.rseCounters[rseName]['deletedSuccess'] = rse['counters']['deletedSuccess']
        self.rseCounters[rseName]['deletedFail'] = rse['counters']['deletedFail']

        self.plineCounters[pName]['totalNumFiles'] += rse['counters']['totalNumFiles']
        self.plineCounters[pName]['deletedSuccess'] += rse['counters']['deletedSuccess']
        self.plineCounters[pName]['deletedFail'] += rse['counters']['deletedFail']
        self.plineCounters[pName]['rsesProcessed'] += 1
        if rse['isClean']:
            self.plineCounters[pName]['rsesCleaned'] += 1

        return rse

    def resetCounters(self, rseName=None, plineName=None):
        """
        A simple function for zeroing the service counters.
        :param rseName:   RSE Name whose counters to be zeroed
        :param plineName: The Pline Name whose counters to be zeroed
        """

        # Resetting Just the RSE Counters
        if rseName is not None:
            if rseName not in self.rseCounters:
                self.rseCounters[rseName] = {}
            self.rseCounters[rseName]['totalNumFiles'] = 0
            self.rseCounters[rseName]['deletedSuccess'] = 0
            self.rseCounters[rseName]['deletedFail'] = 0
            return

        # Resetting Just the pline counters
        if plineName is not None:
            if plineName not in self.plineCounters:
                self.plineCounters[plineName] = {}
            self.plineCounters[plineName]['totalNumFiles'] = 0
            self.plineCounters[plineName]['deletedSuccess'] = 0
            self.plineCounters[plineName]['deletedFail'] = 0
            self.plineCounters[plineName]['totalNumRses'] = 0
            self.plineCounters[plineName]['rsesProcessed'] = 0
            self.plineCounters[plineName]['rsesCleaned'] = 0
            return

        # Resetting all counters
        for rseName in self.rseCounters:
            self.rseCounters[rseName]['totalNumFiles'] = 0
            self.rseCounters[rseName]['deletedSuccess'] = 0
            self.rseCounters[rseName]['deletedFail'] = 0

        for plineName in self.plineCounters:
            self.plineCounters[plineName]['totalNumFiles'] = 0
            self.plineCounters[plineName]['deletedSuccess'] = 0
            self.plineCounters[plineName]['deletedFail'] = 0
            self.plineCounters[plineName]['totalNumRses'] = 0
            self.plineCounters[plineName]['rsesProcessed'] = 0
            self.plineCounters[plineName]['rsesCleaned'] = 0
        return

    # @profile
    def getRSEList(self):
        """
        Queries Rucio for the proper RSE list to iterate through.
        :return: List of RSE names.
        """
        try:
            rseList = self.rucio.evaluateRSEExpression(self.msConfig['rseExpr'])
        except Exception as ex:
            msg = "Unknown exception while trying to fetch the initial list of RSEs to work on. Err: %s"
            self.logger.exception(msg, str(ex))
            rseList = []
        return rseList
