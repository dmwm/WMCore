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
import errno
import stat
try:
    import gfal2
except ImportError:
    # in case we do not have gfal2 installed
    print("FAILED to import gfal2. Use it only in emulateGfal2=True mode!!!")
    gfal2 = None

from pymongo import IndexModel
from pymongo.errors  import NotPrimaryError

# WMCore modules
from WMCore.MicroService.DataStructs.DefaultStructs import UNMERGED_REPORT
from WMCore.MicroService.MSCore.MSCore import MSCore
from WMCore.MicroService.MSUnmerged.MSUnmergedRSE import MSUnmergedRSE
from WMCore.Services.RucioConMon.RucioConMon import RucioConMon
from WMCore.Services.WMStatsServer.WMStatsServer import WMStatsServer
from WMCore.Database.MongoDB import MongoDB
from WMCore.WMException import WMException
from Utils.Pipeline import Pipeline, Functor
from Utils.TwPrint import twFormat
from Utils.IteratorTools import grouper

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


def createGfal2Context(logLevel="normal", emulate=False):
    """
    Create a gfal2 context object
    :param logLevel: string with the gfal2 log level
    :param emulate: boolean to be used by unit tests
    :return: the gfal2 context object
    """
    if emulate:
        return None
    ctx = gfal2.creat_context()
    gfal2.set_verbose(gfal2.verbose_level.names[logLevel])
    return ctx


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
        self.msConfig.setdefault("enableRealMode", False)
        self.msConfig.setdefault("enableT0WMStats", False)
        self.msConfig.setdefault("fullRSEToDB", False)
        self.msConfig.setdefault("gfalLogLevel", 'normal')
        self.msConfig.setdefault("dirFilterIncl", [])
        self.msConfig.setdefault("dirFilterExcl", [])
        self.msConfig.setdefault("emulateGfal2", False)
        self.msConfig.setdefault("filesToDeleteSliceSize", 100)

        self.msConfig.setdefault("mongoDBRetryCount", 3)
        self.msConfig.setdefault("mongoDBReplicaSet", None)
        self.msConfig.setdefault("mongoDBPort", None)
        self.msConfig.setdefault("mockMongoDB", False)

        msUnmergedIndex = IndexModel('name', unique=True)

        # NOTE: A full set of valid database connection parameters can be found at:
        #       https://pymongo.readthedocs.io/en/stable/api/pymongo/mongo_client.html
        msUnmergedDBConfig = {
            'database': self.msConfig['mongoDB'],
            'server': self.msConfig['mongoDBServer'],
            'port': self.msConfig['mongoDBPort'],
            'replicaSet': self.msConfig['mongoDBReplicaSet'],
            'username': self.msConfig['mongoDBUser'],
            'password': self.msConfig['mongoDBPassword'],
            'connect': True,
            'directConnection': False,
            'logger': self.logger,
            'create': True,
            'mockMongoDB': self.msConfig['mockMongoDB'],
            'collections': [('msUnmergedColl', msUnmergedIndex)]}

        mongoDB = MongoDB(**msUnmergedDBConfig)
        self.msUnmergedDB = getattr(mongoDB, self.msConfig['mongoDB'])
        self.msUnmergedColl = self.msUnmergedDB['msUnmergedColl']

        if self.msConfig['emulateGfal2'] is False and gfal2 is None:
            msg = "Failed to import gfal2 library while it's not "
            msg += "set to emulate it. Crashing the service!"
            raise ImportError(msg)

        # Instantiating the Rucio Consistency Monitor Client
        self.rucioConMon = RucioConMon(self.msConfig['rucioConMon'], logger=self.logger)

        self.wmstatsSvc = WMStatsServer(self.msConfig['wmstatsUrl'], logger=self.logger)
        self.wmstatsSvcT0 = WMStatsServer(self.msConfig['wmstatsUrlT0'], logger=self.logger)

        # Building all the Pipelines:
        pName = 'plineUnmerged'
        self.plineUnmerged = Pipeline(name=pName,
                                      funcLine=[Functor(self.getRSEFromMongoDB),
                                                Functor(self.updateRSETimestamps, start=True, end=False),
                                                Functor(self.consRecordAge),
                                                Functor(self.getUnmergedFiles),
                                                Functor(self.filterUnmergedFiles),
                                                Functor(self.getPfn),
                                                Functor(self.cleanRSE),
                                                Functor(self.updateServiceCounters),
                                                Functor(self.updateRSETimestamps, start=False, end=True),
                                                Functor(self.uploadRSEToMongoDB),
                                                Functor(self.purgeRseObj)])
        # Initialization service counters:
        self.plineCounters = {}
        self.rseTimestamps = {}

        # Initialization service common data structures:
        self.rseConsStats = {}
        self.protectedLFNs = set()

        # The basic /store/unmerged regular expression:
        self.regStoreUnmergedLfn = re.compile("^/store/unmerged/.*$")
        self.regStoreUnmergedPfn = re.compile("^.+/store/unmerged/.*$")

        # log msConfig
        self.logger.info("msConfig: %s", pformat(self.msConfig))

    # @profile
    def execute(self):
        """
        Executes the whole MSUnmerged logic
        :return: summary
        """
        # start threads in MSManager which should call this method
        summary = dict(UNMERGED_REPORT)

        # fetch the protectedLFNs list
        try:
            self.protectedLFNs = set(self.wmstatsSvc.getProtectedLFNs())
            # self.logger.debug("protectedLFNs: %s", pformat(self.protectedLFNs))
            if not self.protectedLFNs:
                msg = "Could not fetch the protectedLFNs list from Production WMStatServer. "
                msg += "Skipping the current run."
                self.logger.error(msg)
                return summary

            protectedLFNsT0 = set()
            if self.msConfig['enableT0WMStats']:
                msg = "WMStatsServer.getProtectedLFNs for T0 is not yet implemented."
                raise NotImplementedError(msg)
                # protectedLFNs = set(self.wmstatsSvcT0.getProtectedLFNs())
                # if not protectedLFNsT0:
                #     msg = "Could not fetch the protectedLFNs list from T0 WMStatServer. "
                #     msg += "Skipping the current run."
                #     self.logger.error(msg)
                #     return summary

            self.protectedLFNs = self.protectedLFNs | protectedLFNsT0

        except Exception as ex:
            msg = "Unknown exception while trying to fetch the protectedLFNs list from WMStatServer. Error: {}".format(str(ex))
            self.logger.exception(msg)
            self.updateReportDict(summary, "error", msg)
            return summary

        # refresh statistics on every poling cycle
        try:
            self.rseConsStats = self.rucioConMon.getRSEStats()
            # self.logger.debug("protectedLFNs: %s", pformat(self.protectedLFNs))

            if not self.rseConsStats:
                msg = "Could not fetch statistics from Rucio Consistency Monitor. "
                msg += "Skipping the current run."
                self.logger.error(msg)
                return summary
        except Exception as ex:
            msg = "Unknown exception while trying to fetch statistics from Rucio Consistency Monitor. Error: {}".format(str(ex))
            self.logger.exception(msg)
            self.updateReportDict(summary, "error", msg)
            return summary

        try:
            rseList = self.getRSEList()
            msg = "Retrieved list of %s RSEs: %s "
            msg += "Service set to process up to %s RSEs per instance."
            self.logger.info(msg, len(rseList), pformat(rseList), self.msConfig["limitRSEsPerInstance"])
            random.shuffle(rseList)
        except Exception as err:  # general error
            msg = "Unknown exception while trying to estimate the final list of RSEs to work on. Error: {}".format(str(err))
            self.logger.exception(msg)
            self.updateReportDict(summary, "error", msg)
            return summary

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
            msg = "Unknown exception while running MSUnmerged thread Error: {}".format(str(ex))
            self.logger.exception(msg)
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
        self.resetServiceCounters()
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
            self.plineCounters[pline.name]['filesDeletedSuccess']

    # @profile
    def cleanRSE(self, rse):
        """
        The method to implement the actual deletion of files for an RSE.
        :param rse: MSUnmergedRSE object to be cleaned
        :return:    The MSUnmergedRSE object
        """

        # Create the gfal2 context object:
        try:
            ctx = createGfal2Context(self.msConfig['gfalLogLevel'], self.msConfig['emulateGfal2'])
        except Exception as ex:
            msg = "RSE: %s, Failed to create gfal2 Context object. " % rse['name']
            msg += "Skipping it in the current run."
            self.logger.exception(msg)
            raise MSUnmergedPlineExit(msg) from ex

        filesToDeleteCurrRSE = 0

        # Start cleaning one directory at a time:
        for dirLfn, fileLfnGen in rse['files']['toDelete'].items():
            if dirLfn in rse['dirs']['deletedSuccess']:
                self.logger.info("RSE: %s, dir: %s already successfully deleted.", rse['name'], dirLfn)
                continue

            if self.msConfig['limitFilesPerRSE'] < 0 or \
               filesToDeleteCurrRSE < self.msConfig['limitFilesPerRSE']:

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

                filesToDeleteCurrRSE += len(pfnList)
                msg = "\nRSE: %s \nDELETING: %s."
                msg += "\nPFN list with: %s entries: \n%s"
                self.logger.debug(msg, rse['name'], dirLfn, len(pfnList), twFormat(pfnList, maxLength=4))

                if self.msConfig['enableRealMode']:
                    # execute the actual deletion in bulk - full list of files per directory

                    deletedSuccess = 0
                    for pfnSlice in list(grouper(pfnList, self.msConfig["filesToDeleteSliceSize"])):
                        try:
                            delResult = ctx.unlink(pfnSlice)
                            # Count all the successfully deleted files (if a deletion was
                            # successful a value of None is put in the delResult list):
                            self.logger.debug("RSE: %s, Dir: %s, delResult: %s",
                                              rse['name'], dirLfn, pformat(delResult))
                            for gfalErr in delResult:
                                if gfalErr is None:
                                    deletedSuccess += 1
                                else:
                                    errMessage = os.strerror(gfalErr.code)
                                    rse['counters']['gfalErrors'].setdefault(errMessage, 0)
                                    rse['counters']['gfalErrors'][errMessage] += 1
                        except Exception as ex:
                            msg = "Error while cleaning RSE: %s. "
                            msg += "Will retry in the next cycle. Err: %s"
                            self.logger.exception(msg, rse['name'], str(ex))

                    self.logger.info("RSE: %s, Dir: %s, filesDeletedSuccess: %s",
                                      rse['name'], dirLfn, deletedSuccess)
                    rse['counters']['filesDeletedSuccess'] += deletedSuccess

                    # Now clean the whole branch
                    self.logger.debug("Purging dirEntry: %s:\n", dirPfn)
                    purgeSuccess = self._purgeTree(ctx, dirPfn)
                    if purgeSuccess:
                        rse['dirs']['deletedSuccess'].add(dirLfn)
                        rse['counters']['dirsDeletedSuccess'] = len(rse['dirs']['deletedSuccess'])
                        # if dirLfn in rse['dirs']['toDelete']:
                        #     rse['dirs']['toDelete'].remove(dirLfn)
                        if dirLfn in rse['dirs']['deletedFail']:
                            rse['dirs']['deletedFail'].remove(dirLfn)
                        msg = "RSE: %s  Success deleting directory: %s"
                        self.logger.info(msg, rse['name'], dirPfn)
                    else:
                        rse['dirs']['deletedFail'].add(dirLfn)
                        rse['counters']['dirsDeletedFail'] = len(rse['dirs']['deletedFail'])
                        msg = "RSE: %s Failed to purge directory: %s"
                        self.logger.error(msg, rse['name'], dirPfn)
            else:
                msg = "RSE: %s reached limit of files per RSE to be deleted. Skipping directory: %s. It will be retried on the next cycle."
                self.logger.warning(msg, rse['name'], dirLfn)
        rse['isClean'] = self._checkClean(rse)

        return rse

    def _purgeTree(self, ctx, baseDirPfn):
        """
        A method to be used for purging the tree bellow a specific branch.
        It deletes every empty directory bellow that branch + the origin at the end.
        :param ctx:  The gfal2 context object
        :return:     Bool: True if it managed to purge everything, False otherwise
        """
        # NOTE: It deletes only directories and does not try to unlink any file.

        # First test if baseDirPfn is actually a directory entry:
        try:
            entryStat = ctx.stat(baseDirPfn)
            if not stat.S_ISDIR(entryStat.st_mode):
                self.logger.error("The base pfn: %s is not a directory entry.", baseDirPfn)
                return False
        except gfal2.GError as gfalExc:
            if gfalExc.code == errno.ENOENT:
                self.logger.warning("MISSING baseDir: %s", baseDirPfn)
                return True
            else:
                self.logger.error("FAILED to open baseDir: %s: gfalException: %s", baseDirPfn, str(gfalExc))
                return False

        if baseDirPfn[-1] != '/':
            baseDirPfn += '/'

        # Second recursively iterate down the tree:
        successList = []
        for dirEntry in ctx.listdir(baseDirPfn):
            if dirEntry in ['.', '..']:
                continue
            dirEntryPfn = baseDirPfn + dirEntry
            try:
                entryStat = ctx.stat(dirEntryPfn)
            except gfal2.GError as gfalExc:
                if gfalExc.code == errno.ENOENT:
                    self.logger.warning("MISSING dirEntry: %s", dirEntryPfn)
                    successList.append(True)
                else:
                    self.logger.error("FAILED to open dirEntry: %s: gfalException: %s", dirEntryPfn, str(gfalExc))
                    successList.append(False)

            if stat.S_ISDIR(entryStat.st_mode):
                successList.append(self._purgeTree(ctx, dirEntryPfn))

        # Finally remove the baseDir:
        try:
            self.logger.debug("RM baseDir: %s", baseDirPfn)
            success = ctx.rmdir(baseDirPfn)
            # for gfal2 rmdir() exit status of 0 is success
            if success == 0:
                successList.append(True)
            else:
                successList.append(False)
        except gfal2.GError as gfalExc:
            if gfalExc.code == errno.ENOENT:
                self.logger.warning("MISSING baseDir: %s", baseDirPfn)
                successList.append(True)
            else:
                self.logger.error("FAILED to remove baseDir: %s: gfalException: %s", baseDirPfn, str(gfalExc))
                successList.append(False)
        return all(successList)

    def _checkClean(self, rse):
        """
        A simple function to check if every file in an RSE's unmerged area have
        been deleted
        :param rse: The RSE to be checked
        :return:    Bool: True if all files found have been deleted, False otherwise
        """
        return rse['counters']['dirsToDelete'] == rse['counters']['dirsDeletedSuccess']

    def consRecordAge(self, rse):
        """
        A method to check the duration of the consistency record for the RSE
        :param rse: The RSE to be checked
        :return:    rse or raises MSUnmergedPlineExit
        """
        rseName = rse['name']

        if rseName not in self.rseConsStats:
            msg = "RSE: %s Missing in stats records at Rucio Consistency Monitor. " % rseName
            msg += "Skipping it in the current run."
            self.logger.warning(msg)
            rse['rucioConMonStatus'] = "Missing"
            self.updateRSETimestamps(rse, start=False, end=True)
            self.uploadRSEToMongoDB(rse)
            raise MSUnmergedPlineExit(msg)

        isConsDone = self.rseConsStats[rseName]['status'] == 'done'
        if not isConsDone:
            msg = "RSE: %s has a non-final Rucio ConMon status: %s. " % (rseName, self.rseConsStats[rseName]['status'])
            msg += "Skipping it in the current run."
            self.logger.warning(msg)
            rse['rucioConMonStatus'] = self.rseConsStats[rseName]['status']
            self.updateRSETimestamps(rse, start=False, end=True)
            self.uploadRSEToMongoDB(rse)
            raise MSUnmergedPlineExit(msg)

        isConsNewer = self.rseConsStats[rseName]['end_time'] > self.rseTimestamps[rseName]['prevStartTime']
        if not isConsNewer:
            msg = "RSE: %s With old consistency record in Rucio Consistency Monitor. " % rseName
            if 'isClean' in rse and rse['isClean']:
                msg += "And the RSE has been cleaned during the last Rucio Consistency Monitor polling cycle."
                msg += "Skipping it in the current run."
                self.logger.info(msg)
                rse['rucioConMonStatus'] = self.rseConsStats[rseName]['status']
                self.updateRSETimestamps(rse, start=False, end=True)
                self.uploadRSEToMongoDB(rse)
                raise MSUnmergedPlineExit(msg)
            else:
                msg += "But the RSE has NOT been fully cleaned during the last Rucio Consistency Monitor polling cycle."
                msg += "Retrying cleanup in the current run."
                self.logger.info(msg)
                rse['rucioConMonStatus'] = self.rseConsStats[rseName]['status']

        if isConsNewer and isConsDone:
            # NOTE: If we've got to this point then we have a brand new record for
            #       the RSE in RucioConMOn and we are then about to start a new RSE cleanup
            #       so we will need to wipe out all but the timestamps from both
            #       the current object and the MongoDB record for the object.
            msg = "RSE: %s With new consistency record in Rucio Consistency Monitor. " % rseName
            msg += "Resetting RSE and starting a fresh cleanup process in the current run."
            self.logger.info(msg)
            try:
                rse.resetRSE(self.msUnmergedColl, keepTimestamps=True, retryCount=self.msConfig['mongoDBRetryCount'])
            except NotPrimaryError:
                msg = "Could not reset RSE to MongoDB for the maximum of %s mongoDBRetryCounts configured." % self.msConfig['mongoDBRetryCount']
                msg += "Giving up now. The whole cleanup process will be retried for this RSE on the next run."
                msg += "Duplicate deletion retries may cause error messages from false positives and wrong counters during next polling cycle."
                raise MSUnmergedPlineExit(msg) from None

        # Before returning the RSE update Consistency StatTime:
        rse['timestamps']['rseConsStatTime'] = self.rseConsStats[rseName]['end_time']
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
            # Check if what we start with is under /store/unmerged/*
            if self.regStoreUnmergedLfn.match(filePath):
                # Cut the path to the deepest level known to WMStats protected LFNs
                dirPath = self._cutPath(filePath)
                # Check if what is left is still under /store/unmerged/*
                if self.regStoreUnmergedLfn.match(dirPath):
                    # Add it to the set of allUnmerged
                    rse['dirs']['allUnmerged'].add(dirPath)

        rse['counters']['totalNumFiles'] = len(rse['files']['allUnmerged'])
        rse['counters']['totalNumDirs'] = len(rse['dirs']['allUnmerged'])
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
            rse['counters']['dirsToDelete'] = -1
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

        # Update directory/files with no service filters
        if not dirFilterIncl and not dirFilterExcl:
            for dirName in rse['dirs']['toDelete']:
                rse['files']['toDelete'][dirName] = genFunc(dirName, rse['files']['allUnmerged'])
            rse['counters']['dirsToDelete'] = len(rse['files']['toDelete'])
            self.logger.info("RSE: %s: %s", rse['name'], twFormat(rse, maxLength=8))
            return rse

        # If we are here, then there are service filters...
        for dirName in rse['dirs']['toDelete']:
            # apply exclusion filter
            dirFilterExclMatch = []
            for pathExcl in dirFilterExcl:
                dirFilterExclMatch.append(dirName.startswith(pathExcl))
            if any(dirFilterExclMatch):
                # then it matched one of the exclusion paths
                continue
            if not dirFilterIncl:
                # there is no inclusion filter, simply add this directory/files
                rse['files']['toDelete'][dirName] = genFunc(dirName, rse['files']['allUnmerged'])
                continue

            # apply inclusion filter
            for pathIncl in dirFilterIncl:
                if dirName.startswith(pathIncl):
                    rse['files']['toDelete'][dirName] = genFunc(dirName, rse['files']['allUnmerged'])
                    break

        # Now apply the filters back to the set in rse['dirs']['toDelete']
        rse['dirs']['toDelete'] = set(rse['files']['toDelete'].keys())

        # Update the counters:
        rse['counters']['dirsToDelete'] = len(rse['files']['toDelete'])
        self.logger.info("RSE: %s: %s", rse['name'], twFormat(rse, maxLength=8))
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
    def purgeRseObj(self, rse, dumpRSEtoLog=False):
        """
        Cleaning all the records in an RSE object. The final method to be used
        before an RSE exits a pipeline.
        :param rse: The RSE to be checked
        :param dumpRSEToLog: Dump the whole RSEobject into the service log.
        :return:    rse
        """
        msg = "\n----------------------------------------------------------"
        msg += "\nMSUnmergedRSE: \n%s"
        msg += "\n----------------------------------------------------------"
        if dumpRSEtoLog:
            self.logger.debug(msg, pformat(rse))
        else:
            self.logger.debug(msg, twFormat(rse, maxLength=8))
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

        # Initialize the timestamps in both MSUnmerged and MSUnmergedRSE objects
        if rseName not in self.rseTimestamps:
            # NOTE: Reading the timestamps from the rse for the first time.
            #       This will reset them if no previous record for the RSE in MongoDB
            #       or will set them to the values fetched from MongoDB, provided
            #       that the RSE object has been updated from the database.
            self.rseTimestamps[rseName] = rse['timestamps']

        # # Read last RucioConMon stat time for this RSE:
        # self.rseTimestamps[rseName]['rseConsStatsTime'] = self.rseConsStats[rseName]['startTime']

        # Update the timestamps:
        if start:
            self.rseTimestamps[rseName]['prevStartTime'] = self.rseTimestamps[rseName]['startTime']
            self.rseTimestamps[rseName]['startTime'] = currTime
        if end:
            self.rseTimestamps[rseName]['prevEndTime'] = self.rseTimestamps[rseName]['endTime']
            self.rseTimestamps[rseName]['endTime'] = currTime
        rse['timestamps'] = self.rseTimestamps[rseName]
        return rse

    def updateServiceCounters(self, rse):
        """
        Update/Upload all counters from the rse object into the MSUnmerged
        service counters
        :param rse:   The RSE to work on
        :param pName: The pipeline name whose counters to be updated
        :return:      rse
        """
        pName = self.plineUnmerged.name
        self.plineCounters[pName]['totalNumFiles'] += rse['counters']['totalNumFiles']
        self.plineCounters[pName]['totalNumDirs'] += rse['counters']['totalNumDirs']
        self.plineCounters[pName]['filesDeletedSuccess'] += rse['counters']['filesDeletedSuccess']
        self.plineCounters[pName]['filesDeletedFail'] += rse['counters']['filesDeletedFail']
        self.plineCounters[pName]['dirsDeletedSuccess'] += rse['counters']['dirsDeletedSuccess']
        self.plineCounters[pName]['dirsDeletedFail'] += rse['counters']['dirsDeletedFail']
        self.plineCounters[pName]['rsesProcessed'] += 1
        if rse['isClean']:
            self.plineCounters[pName]['rsesCleaned'] += 1

        return rse

    def resetServiceCounters(self):
        """
        A simple function for zeroing the service counters.
        :param plineName: The Pline Name whose counters to be zeroed
        """
        # Resetting pline counters
        plineName = self.plineUnmerged.name
        self.plineCounters.setdefault(plineName, {})
        self.plineCounters[plineName]['totalNumFiles'] = 0
        self.plineCounters[plineName]['totalNumDirs'] = 0
        self.plineCounters[plineName]['filesDeletedSuccess'] = 0
        self.plineCounters[plineName]['filesDeletedFail'] = 0
        self.plineCounters[plineName]['dirsDeletedSuccess'] = 0
        self.plineCounters[plineName]['dirsDeletedFail'] = 0
        self.plineCounters[plineName]['totalNumRses'] = 0
        self.plineCounters[plineName]['rsesProcessed'] = 0
        self.plineCounters[plineName]['rsesCleaned'] = 0
        return

    def getRSEFromMongoDB(self, rse):
        """
        Updates the record for an RSE from MongoDB
        :param rse: The RSE object to work on
        :return:    rse
        """
        self.logger.info("RSE: %s Reading rse data from MongoDB.", rse['name'])
        rse.readRSEFromMongoDB(self.msUnmergedColl)
        return rse

    def uploadRSEToMongoDB(self, rse, fullRSEToDB=False, overwrite=True):
        """
        Updates the record for an RSE at MongoDB
        :param rse: The RSE object to work on
        :return:    rse
        """
        try:
            self.logger.info("RSE: %s Writing rse data to MongoDB.", rse['name'])
            rse.writeRSEToMongoDB(self.msUnmergedColl, fullRSEToDB=fullRSEToDB, overwrite=overwrite, retryCount=self.msConfig['mongoDBRetryCount'])
        except NotPrimaryError:
            msg = "Could not write RSE to MongoDB for the maximum of %s mongoDBRetryCounts configured." % self.msConfig['mongoDBRetryCount']
            msg += "Giving up now. The whole cleanup process will be retried for this RSE on the next run."
            msg += "Duplicate deletion retries may cause error messages from false positives and wrong counters during next polling cycle."
            raise MSUnmergedPlineExit(msg) from None
        return rse

    def getStatsFromMongoDB(self, detail=False, **kwargs):
        """
        Auxiliary method to serve the APIs of the REST calls for the service.
        Implements various queries to MongoDB based on the parameters passed.
        :param detail:           Bool marking if additional details must be queried from the database
                                 (e.g. putting the 'dirs' field into the mongo projection)
        :param rse=rseName:      String representing the RSE name to query the database for
        :return:                 Dictionary with the type of query + the data returned from the database
        """
        data = {}
        if kwargs.get('rse'):
            data["query"] = 'rse=%s&detail=%s' % (kwargs['rse'], detail)
            allDocs = (kwargs['rse'].lower() == "all_docs") if isinstance(kwargs['rse'], str) else False

            if allDocs:
                mongoProjection = {
                    "_id": False,
                    "name": True,
                    "isClean": True,
                    "rucioConMonStatus": True,
                    "counters": {
                        "gfalErrors": True,
                        "dirsToDelete": True,
                        "dirsDeletedSuccess": True,
                        "dirsDeletedFail": True}}
                mongoFilter = {}
                data["rseData"] = list(self.msUnmergedColl.find(mongoFilter, projection=mongoProjection))
            else:
                mongoProjection = {
                    "_id": False,
                    "name": True,
                    "isClean": True,
                    "pfnPrefix": True,
                    "rucioConMonStatus": True,
                    "timestamps": True,
                    "counters": True}
                if detail:
                    mongoProjection["dirs"]  = True

                rseList = kwargs['rse'] if isinstance(kwargs['rse'], list) else [kwargs['rse']]
                data["rseData"] = []
                for rseName in rseList:
                    mongoFilter = {'name': rseName}
                    data["rseData"].append(self.msUnmergedColl.find_one(mongoFilter, projection=mongoProjection))
        return data

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
