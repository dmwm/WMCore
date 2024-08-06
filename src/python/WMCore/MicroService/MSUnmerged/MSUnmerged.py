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
from datetime import datetime

import random
import re
import os
import errno
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
        Order of deletion attempts is:
        1. top directory
        2. list sub-directories and files
        3. remove each file (unlink)
        4. remove the (now) empty sub-directories
        5. try to remove the top directory again
        :param rse: MSUnmergedRSE object to be cleaned
        :return:    The MSUnmergedRSE object
        """
        # reset dirs counters
        rse['dirs']['deletedSuccess'] = set()
        rse['dirs']['deletedFail'] = set()
        self.logger.info("Start cleaning files for RSE: %s.", rse['name'])
        if not rse['dirs']['toDelete']:
            self.logger.info("There is nothing to delete for RSE: %s.", rse['name'])
            rse['isClean'] = self._checkClean(rse)
            return rse

        # Create the gfal2 context object:
        try:
            ctx = createGfal2Context(self.msConfig['gfalLogLevel'], self.msConfig['emulateGfal2'])
        except Exception as ex:
            msg = "RSE: %s, Failed to create gfal2 Context object. " % rse['name']
            msg += "Skipping it in the current run."
            self.logger.exception(msg)
            raise MSUnmergedPlineExit(msg) from ex

        # Start cleaning one directory at a time:
        for idx, dirLfn in enumerate(rse['dirs']['toDelete']):
            self.logger.info("Processing directory index %s out of %s", idx, len(rse['dirs']['toDelete']))
            # figure out the PFN prefix
            dirPfn = rse['pfnPrefix'] + dirLfn
            if not self.msConfig['enableRealMode']:
                self.logger.info("DRY-RUN: would delete directory PFN: %s for RSE: %s", dirPfn, rse['name'])
            else:
                # The following two bool flags are to track the success for directory removal
                # during all consecutive attempts/steps of cleaning the current branch.
                filesDeletedSuccess = 0
                filesDeletedFail = 0

                # Initially try to delete the whole directory even before emptying its content:
                self.logger.info("Trying to remove the whole directory: %s", dirPfn)
                rmdirSuccess = self._rmDir(ctx, dirPfn)

                if rmdirSuccess:
                    self.logger.info("Directory successfully removed: %s", dirPfn)
                    rse['counters']['dirsDeletedSuccess'] += 1
                else:
                    self.logger.info("Failed to remove the whole directory. Listing its content and deleting file by file.")
                    listFiles = self._listDir(ctx, dirPfn)
                    self.logger.info("Starting deletion of %s files:", len(listFiles))
                    for pfnSlice in list(grouper(listFiles, self.msConfig["filesToDeleteSliceSize"])):
                        self.logger.info("Executing file slice removal for %s files...", len(pfnSlice))
                        try:
                            # returns None if deletion was successful
                            for resp in ctx.unlink(pfnSlice):
                                if resp is None:
                                    filesDeletedSuccess += 1
                                else:
                                    filesDeletedFail += 1
                                    errMessage = os.strerror(resp.code)
                                    rse['counters']['gfalErrors'].setdefault(errMessage, 0)
                                    rse['counters']['gfalErrors'][errMessage] += 1
                        except Exception as ex:
                            msg = "Error while cleaning RSE: %s. "
                            msg += "Will retry in the next cycle. Err: %s"
                            self.logger.exception(msg, rse['name'], str(ex))

                    self.logger.info("RSE: %s, Dir: %s, filesDeletedSuccess: %s, filesDeletedFail: %s",
                                        rse['name'], dirLfn, filesDeletedSuccess, filesDeletedFail)

                    # now reverse engineer the deepest directory names and delete each one of them
                    setDirPfn = set()
                    for item in listFiles:
                        setDirPfn.add(os.path.dirname(item))
                    
                    for subDir in setDirPfn:
                        rmdirSuccess = self._rmDir(ctx, subDir)
                        if rmdirSuccess:
                            self.logger.info("Sub-directory successfully removed: %s", subDir)
                        else:
                            self.logger.info("Sub-directory failed to be removed: %s", subDir)

                    # lastly, try to delete the original directory 
                    if self._rmDir(ctx, dirPfn):
                        self.logger.info("Finally, directory successfully removed: %s", dirPfn)
                        rse['counters']['dirsDeletedSuccess'] += 1
                        rse['dirs']['deletedSuccess'].add(dirLfn)
                    else:
                        self.logger.info("Directory still fails to be removed: %s", dirPfn)
                        rse['counters']['dirsDeletedFail'] += 1
                        rse['dirs']['deletedFail'].add(dirLfn)

                # Updating the RSE counters with the newly successfully deleted files
                rse['counters']['filesDeletedSuccess'] += filesDeletedSuccess
                rse['counters']['filesDeletedFail'] += filesDeletedFail
        rse['isClean'] = self._checkClean(rse)

        # Explicitly release all internal resources used by the gfal2 context instance
        if ctx:
            ctx.free()
        return rse

    def _rmDir(self, ctx, dirPfn):
        """
        Auxiliary method to be used for removing a single directory entry with gfal2
        and handling eventual gfal errors raised.
        :param ctx:    Gfal Context Manager object.
        :param dirPfn: The Pfn of the directory to be removed
        :return:       Bool: True if the removal was successful, False otherwise
                       NOTE: An attempt to delete an already missing directory is considered a success
        """
        try:
            # NOTE: For gfal2 rmdir() exit status of 0 is success
            rmdirSuccess = ctx.rmdir(dirPfn) == 0
        except gfal2.GError as gfalExc:
            if gfalExc.code == errno.ENOENT:
                self.logger.warning("MISSING directory: %s", dirPfn)
                rmdirSuccess = True
            else:
                self.logger.error("FAILED to remove directory: %s: gfalException: %s, gfalErrorCode: %s", dirPfn, str(gfalExc), gfalExc.code)
                rmdirSuccess = False
        return rmdirSuccess

    def _listDir(self, ctx, dirPfn):
        """
        Recursively lists all files in the given directory and its subdirectories.

        :param ctx: Gfal context manager object
        :param dirPfn: string with a directory pfn
        """
        files = []
        try:
            for entry in ctx.listdir(dirPfn):
                # are there files inside this folder
                if entry.endswith(".root"):
                    files.append(os.path.join(dirPfn, entry))
                else:
                    # it is a directory. Go deeper another directory level
                    files.extend(self._listDir(ctx, os.path.join(dirPfn, entry)))
        except gfal2.GError as gfalExc:
            self.logger.warning("Failed to list directory: %s, gfal code: %s", dirPfn, gfalExc.code)

        self.logger.info("Entries under directory: %s is: %s", dirPfn, len(files))
        return files

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
        self.logger.info("Evaluating consistency record agent for RSE: %s.", rse['name'])

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
        rse['counters']['totalNumFiles'] = 0
        rse['counters']['totalNumDirs'] = 0
        rse['counters']['dirsToDelete'] = 0
        rse['counters']['filesToDelete'] = 0
        rse['dirs']['allUnmerged'] = []  # TODO FIXME: this is supposed to be the union of toDelete and protected
        rse['dirs']['toDelete'] = set()
        rse['dirs']['protected'] = set()

        self.logger.info("Fetching data from Rucio ConMon for RSE: %s.", rse['name'])
        for lfn in self.rucioConMon.getRSEUnmerged(rse['name'], zipped=True):
            dirPath = self._cutPath(lfn)
            # Check if what is left is still under /store/unmerged/*
            if not self.regStoreUnmergedLfn.match(dirPath):
                msg = f"Retrieved file from RucioConMon that does not belong to the unmerged area: {lfn}"
                self.logger.critical(msg)

            # general counter for possible files and unique directories
            rse['counters']['totalNumFiles'] += 1

            # now evaluate whether it is deletable or not, and persist it under the right field
            if self._isDeletable(dirPath):
                rse['dirs']['toDelete'].add(dirPath)
                rse['counters']['filesToDelete'] += 1
            else:
                rse['dirs']['protected'].add(dirPath)

        if not rse['counters']['totalNumFiles']:
            self.logger.error("RSE: %s has an empty list of unmerged files in Rucio ConMon.", rse['name'])

        rse['counters']['totalNumDirs'] = len(rse['dirs']['toDelete']) + len(rse['dirs']['protected'])
        rse['counters']['dirsToDelete'] = len(rse['dirs']['toDelete'])

        self.logger.info("RSE post-filter stats for: %s: %s", rse['name'], twFormat(rse, maxLength=8))

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

    def _isDeletable(self, dirPath):
        """
        Given a short directory path, verify if this directory can be
        deleted or not. Checks are performed against:
         * directory inclusion filter
         * directory exclusion filter
         * protected lfns

        :param dirPath: string with a shorter version of the LFN
        :return _type_: True if the directory can be deleted, False otherwise
        """
        # Check against the inclusion filter
        if self.msConfig['dirFilterIncl']:
            respFilter = [dirPath.startswith(filterItem) for filterItem in self.msConfig['dirFilterIncl']]
            if any(respFilter) is False:
                # does not match against any of the inclusion filters
                return False

        # Check against the exclusion filter
        if self.msConfig['dirFilterExcl']:
            respFilter = [dirPath.startswith(filterItem) for filterItem in self.msConfig['dirFilterExcl']]
            if any(respFilter) is True:
                # matches against at least one exclusion filter
                return False

        # Finally, check against the protected LFNs
        return dirPath not in self.protectedLFNs

    def getPfn(self, rse):
        """
        A method for fetching the common Pfn (method + hostname + global path)
        for the RSE. It uses Rucio client method lfns2pfns for one of the LFNs
        already recorded in the RSE in order to resolve the lfn to pfn mapping
        and then tries to parse the resultant pfn and cut off the lfn part.
        :param rse: The RSE to be checked
        :return:    rse
        """
        self.logger.info("Fetching PFN map for RSE: %s.", rse['name'])
        # NOTE:  pfnPrefix here is considered the full part of the pfn up to the
        #        beginning of the lfn part rather than just the protocol prefix
        if rse['dirs']['toDelete']:
            lfn = next(iter(rse['dirs']['toDelete']))
            pfnDict = self.rucio.getPFN(rse['name'], lfn, operation='delete')
            pfnFull = pfnDict[lfn]
            if self.regStoreUnmergedPfn.match(pfnFull):
                pfnPrefix = pfnFull.split('/store/unmerged/')[0]
                rse['pfnPrefix'] = pfnPrefix
            else:
                msg = "Could not establish the correct pfn Prefix for RSE: %s. " % rse['name']
                msg += "Will fall back to calling Rucio on a directory basis for lfn to pfn resolution."
                self.logger.warning(msg)
            if not rse['pfnPrefix']:
                msg = f"Failed to resolve PFN from LFN for RSE: {rse['name']}. Will retry later."
                raise MSUnmergedPlineExit(msg)

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
        self.logger.info("Purging RSE in-memory information for RSE: %s.", rse['name'])
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
        self.logger.info("Updating timestamps for RSE: %s. With start: %s, end: %s.", rse['name'], start, end)

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
        self.logger.info("Updating service counters for RSE: %s.", rse['name'])

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
            self.logger.info("Uploading RSE information to MongoDB for RSE: %s.", rse['name'])
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
                    "timestamps": True,
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

            # Rewrite all timestamps in ISO 8601 format
            for rse in data['rseData']:
                if 'timestamps' in rse:
                    for dateField, dateValue in rse['timestamps'].items():
                        dateValue = datetime.utcfromtimestamp(dateValue)
                        dateValue = dateValue.isoformat()
                        rse['timestamps'][dateField] = dateValue
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
