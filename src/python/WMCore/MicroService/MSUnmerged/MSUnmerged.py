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
import concurrent.futures

from pprint import pformat
from time import time
from datetime import datetime

import random
import re
import os
import errno
import psutil
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
        # settings for parallel file and sub-directory deletion
        self.msConfig.setdefault("parallelFileDeletionMaxWorkers", 10)
        self.msConfig.setdefault("parallelFileDeletionBatchSize", 100)
        # note that sub-directory deletion has a single directory per batch
        self.msConfig.setdefault("parallelSubDirDeletionMaxWorkers", 20)

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
                                                Functor(self.resetCounters),
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

    def resetCounters(self, rse):
        """
        Reset all counters for an RSE object and initialize structure if needed.
        :param rse: The RSE object whose counters need to be reset
        """
        rseName = rse['name']
        self.logger.info("Resetting RSE object for '%s' to start a fresh cleanup.", rseName)
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
    def cleanRSE(self, rse):
        """
        Optimized cleanup method that uses dedicated executor objects per RSE
        to minimize memory leaks from ThreadPoolExecutor.
        Creates and frees gfal2 context for each directory for better isolation.
        :param rse: MSUnmergedRSE object to be cleaned
        :return:    The MSUnmergedRSE object with updated counters and isClean flag
        """
        if not rse['dirs']['toDelete']:
            rse['isClean'] = self._checkClean(rse)
            return rse

        # Create dedicated executors for this RSE to avoid memory leaks
        dir_executor, file_executor = self._createRSEExecutors(rse['name'])

        try:
            self._logMemoryUsage(f"before processing RSE {rse['name']}")

            for dirLfn in rse['dirs']['toDelete']:
                # Create a fresh gfal2 context for each directory
                ctx = self._createGfal2ContextForDirectory(rse['name'], dirLfn)
                if ctx is None:
                    self._updateFailureCounters(rse, dirLfn)
                    continue

                try:
                    dirPfn = rse['pfnPrefix'] + dirLfn

                    if not self.msConfig['enableRealMode']:
                        self.logger.info("DRY-RUN: would delete directory: %s", dirPfn)
                        continue

                    # First attempt: Try to delete the whole directory
                    if self._rmDir(ctx, dirPfn):
                        self._updateSuccessCounters(rse, dirLfn)
                        continue

                    # Second attempt: Try to delete sub-directories first using dedicated executor
                    if self._rmSubDirsInParallel(ctx, dirPfn, dir_executor):
                        # If sub-directories were deleted, try the parent again
                        if self._rmDir(ctx, dirPfn):
                            self._updateSuccessCounters(rse, dirLfn)
                            continue

                    # Third attempt: If we get here, we need to delete files using dedicated executor
                    # List and delete files in batches to control memory usage
                    successCount = 0
                    failCount = 0
                    for fileBatch in self._listFilesForDeletion(ctx, dirPfn):
                        batchSuccess, batchFail = self.deleteFilesInParallel(ctx, fileBatch, file_executor)
                        successCount += batchSuccess
                        failCount += batchFail

                        # Update counters after each batch
                        self._updateFileCounters(rse, batchSuccess, batchFail)

                    # Final attempt: Try to delete the directory again
                    if self._rmDir(ctx, dirPfn):
                        self._updateSuccessCounters(rse, dirLfn)
                    else:
                        self._updateFailureCounters(rse, dirLfn)

                except Exception as ex:
                    self.logger.exception("Error processing directory %s: %s", dirLfn, str(ex))
                    self._updateFailureCounters(rse, dirLfn)
                finally:
                    # Always free the gfal2 context for this directory
                    self._freeGfal2Context(ctx, rse['name'], dirLfn)

                # Log memory usage after processing each directory
                self._logMemoryUsage(f"after processing directory {dirLfn}")

        except Exception as ex:
            self.logger.exception("Error during RSE cleanup: %s", str(ex))
        finally:
            # Clean up executors explicitly to prevent memory leaks
            self._cleanupRSEExecutors(dir_executor, file_executor, rse['name'])

            self._logMemoryUsage(f"after completing RSE {rse['name']}")

        rse['isClean'] = self._checkClean(rse)
        return rse

    def _createGfal2ContextForDirectory(self, rseName, dirLfn):
        """
        Create a gfal2 context for a specific directory with proper error handling.
        :param rseName: Name of the RSE
        :param dirLfn: Directory LFN being processed
        :return: gfal2 context object or None if creation failed
        """
        try:
            ctx = createGfal2Context(self.msConfig['gfalLogLevel'], self.msConfig['emulateGfal2'])
            self.logger.debug("Created gfal2 context for RSE %s, directory %s", rseName, dirLfn)
            return ctx
        except Exception:
            msg = "RSE: %s, Failed to create gfal2 context for directory: %s. " % (rseName, dirLfn)
            msg += "Skipping this directory."
            self.logger.exception(msg)
            return None

    def _freeGfal2Context(self, ctx, rseName, dirLfn):
        """
        Free a gfal2 context with proper error handling.
        :param ctx: gfal2 context to free
        :param rseName: Name of the RSE (for logging)
        :param dirLfn: Directory LFN (for logging)
        """
        if ctx:
            try:
                ctx.free()
                self.logger.debug("Freed gfal2 context for RSE %s, directory %s", rseName, dirLfn)
            except Exception as ex:
                self.logger.warning("Failed to free gfal2 context for RSE %s, directory %s: %s",
                                  rseName, dirLfn, str(ex))

    def _createRSEExecutors(self, rseName):
        """
        Create dedicated executor objects for an RSE to avoid memory leaks.
        :param rseName: Name of the RSE for logging purposes
        :return: tuple of (dir_executor, file_executor)
        """
        # Create executor for directory operations
        dir_executor = concurrent.futures.ThreadPoolExecutor(
            max_workers=self.msConfig['parallelSubDirDeletionMaxWorkers']
        )

        # Create executor for file operations
        file_executor = concurrent.futures.ThreadPoolExecutor(
            max_workers=self.msConfig['parallelFileDeletionMaxWorkers']
        )

        self.logger.info("Created dedicated executors for RSE %s: dir_executor=%d workers, file_executor=%d workers",
                       rseName, self.msConfig['parallelSubDirDeletionMaxWorkers'],
                       self.msConfig['parallelFileDeletionMaxWorkers'])

        return dir_executor, file_executor

    def _cleanupRSEExecutors(self, dir_executor, file_executor, rseName):
        """
        Clean up executor objects to prevent memory leaks.
        :param dir_executor: Directory executor to clean up
        :param file_executor: File executor to clean up
        :param rseName: Name of the RSE for logging purposes
        """
        # Clean up directory executor
        if dir_executor:
            self.logger.info("Shutting down directory executor for RSE %s", rseName)
            dir_executor.shutdown(wait=True)
            del dir_executor

        # Clean up file executor
        if file_executor:
            self.logger.info("Shutting down file executor for RSE %s", rseName)
            file_executor.shutdown(wait=True)
            del file_executor

    def _updateSuccessCounters(self, rse, dirLfn):
        """Update counters for successful directory deletion"""
        rse['counters']['dirsDeletedSuccess'] += 1
        rse['dirs']['deletedSuccess'].add(dirLfn)

    def _updateFailureCounters(self, rse, dirLfn):
        """Update counters for failed directory deletion"""
        rse['counters']['dirsDeletedFail'] += 1
        rse['dirs']['deletedFail'].add(dirLfn)

    def _updateFileCounters(self, rse, successCount, failCount):
        """Update counters for file deletion operations"""
        rse['counters']['filesDeletedSuccess'] += successCount
        rse['counters']['filesDeletedFail'] += failCount

    def _trackGfalError(self, error):
        """
        Track and count different types of gfal2 errors.
        :param error: gfal2 error object or error code
        """
        if hasattr(error, 'code'):
            errorCode = error.code
            errorMessage = str(error)
        else:
            errorCode = error
            errorMessage = os.strerror(error)

        # Initialize the error counter if it doesn't exist
        if 'gfalErrors' not in self.plineCounters[self.plineUnmerged.name]:
            self.plineCounters[self.plineUnmerged.name]['gfalErrors'] = {}

        # Update the error counter
        if errorMessage not in self.plineCounters[self.plineUnmerged.name]['gfalErrors']:
            self.plineCounters[self.plineUnmerged.name]['gfalErrors'][errorMessage] = 0
        self.plineCounters[self.plineUnmerged.name]['gfalErrors'][errorMessage] += 1

        self.logger.error("GFAL error occurred - Code: %s, Message: %s", errorCode, errorMessage)

    def _rmDir(self, ctx, dirPfn):
        """
        Auxiliary method to be used for removing a single directory entry with gfal2
        and handling eventual gfal errors raised.
        :param ctx:    Gfal Context Manager object.
        :param dirPfn: string with the PFN of the directory to be removed
        :return:       Bool: True if the removal was successful, False otherwise
        NOTE: An attempt to delete an already missing directory is considered a success
        """
        try:
            # NOTE: For gfal2 rmdir() exit status of 0 is success
            self.logger.info("Attempting to delete directory: %s", dirPfn)
            rmdirSuccess = ctx.rmdir(dirPfn) == 0
            if rmdirSuccess:
                self.logger.info("Successfully deleted directory: %s", dirPfn)
        except gfal2.GError as gfalExc:
            if gfalExc.code == errno.ENOENT:
                self.logger.warning("MISSING directory: %s", dirPfn)
                rmdirSuccess = True
            else:
                self._trackGfalError(gfalExc)
                rmdirSuccess = False
        return rmdirSuccess

    def _rmSubDirsInParallel(self, ctx, dirPfn, dir_executor):
        """
        Attempt to delete sub-directories in parallel, without listing their contents.
        Uses the provided executor to avoid creating new ones and causing memory leaks.
        :param ctx:    Gfal Context Manager object.
        :param dirPfn: string with the PFN of the directory to be removed
        :param dir_executor: ThreadPoolExecutor object for directory operations
        :return:       Bool: True if the removal was successful, False otherwise
        """
        self.logger.info("Listing sub-directories for deletion in directory: %s", dirPfn)
        listSubDirs = []
        try:
            # List only the immediate sub-directories
            for entry in ctx.listdir(dirPfn):
                if not entry.endswith('.root'):
                    listSubDirs.append(os.path.join(dirPfn, entry))
        except gfal2.GError as ex:
            self.logger.error("Failed to list directory content for: %s", dirPfn)
            self._trackGfalError(ex)
            return False
        except Exception as ex:
            self.logger.exception("Unknown exception while listing sub-directories for deletion: %s", str(ex))
            return False

        if not listSubDirs:
            self.logger.info("No sub-directories found for deletion in directory: %s", dirPfn)
            return True

        def deleteBatchDir(subDir):
            return self._rmDir(ctx, subDir)

        self.logger.info("Found a total of %d sub-directories for deletion.", len(listSubDirs))
        self.logger.info("To be deleted with the dedicated directory executor.")

        successCount = 0
        failCount = 0

        try:
            # Submit all directories for deletion using the provided executor
            jobs = (dir_executor.submit(deleteBatchDir, subDir) for subDir in listSubDirs)

            # Process results as they complete
            for future in concurrent.futures.as_completed(jobs):
                try:
                    if future.result():
                        successCount += 1
                    else:
                        failCount += 1
                except Exception as ex:
                    self.logger.error("Error processing directory deletion result: %s", str(ex))
                    failCount += 1
                finally:
                    # Clean up future using the helper method
                    self._cleanupFuture(future)
        except Exception as ex:
            self.logger.exception("Unknown exception while deleting sub-directories: %s", str(ex))

        self.logger.info("Sub-directories deletion completed. Success: %d, Failed: %d", successCount, failCount)
        return failCount == 0

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
        :return:    rse object it if can proceeed with cleanup,
                    else it raises an MSUnmergedPlineExit exception
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
            self.logger.info("RSE: %s with new consistency record in Rucio Consistency Monitor.", rseName)
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
        self.logger.info("Fetching data from Rucio ConMon for RSE: %s.", rse['name'])
        for lfn in self.rucioConMon.getRSEUnmerged(rse['name'], zipped=True):
            dirPath = self._cutPath(lfn)
            # Check if what is left is still under /store/unmerged/*
            if not self.regStoreUnmergedLfn.match(dirPath):
                msg = f"Retrieved file from RucioConMon that does not belong to the unmerged area: {lfn}. Skipping it."
                self.logger.critical(msg)
                continue

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

    def _listFilesForDeletion(self, ctx, dirPfn, batchSize=1000):
        """
        Generator that lists and yields files in batches to control memory usage.
        :param ctx: Gfal context manager object
        :param dirPfn: string with a directory pfn
        :param batchSize: int with the batch size for file paths to be deleted
        :return: generator with batches of file PFNs
        """
        self.logger.info("Listing files for deletion in directory: %s", dirPfn)

        def processEntry(entry, currentPath):
            fullPath = os.path.join(currentPath, entry)
            if entry.endswith('.root'):
                yield fullPath
            else:
                try:
                    # Since we know it's at most 2 levels deep, we can directly yield .root files
                    for subEntry in ctx.listdir(fullPath):
                        if subEntry.endswith('.root'):
                            yield os.path.join(fullPath, subEntry)
                except gfal2.GError as ex:
                    if ex.code != errno.ENOENT:  # Ignore if directory doesn't exist
                        self._trackGfalError(ex)
                except Exception as ex:
                    self.logger.exception("Unknown exception while listing files for deletion: %s", str(ex))

        try:
            currentBatch = []
            for entry in ctx.listdir(dirPfn):
                for filePath in processEntry(entry, dirPfn):
                    currentBatch.append(filePath)
                    if len(currentBatch) >= batchSize:
                        # yield currentBatch[:]
                        # currentBatch.clear()
                        yield currentBatch
                        currentBatch = []

            # Yield any remaining files in the last batch
            if currentBatch:
                # yield currentBatch[:]
                # currentBatch.clear()
                yield currentBatch
                currentBatch = []
        except gfal2.GError as ex:
            self._trackGfalError(ex)
        except Exception as ex:
            self.logger.exception("Unknown exception while listing files for deletion: %s", str(ex))

    def _logMemoryUsage(self, context=""):
        """
        Log current memory usage of the process using RSS (Resident Set Size).
        This is a simplified version that only uses RSS to avoid any threading issues.
        :param context: Optional string to identify where the memory check is happening
        """
        try:
            process = psutil.Process(os.getpid())
            memoryInfo = process.memory_info()
            # Get a count of active threads
            threadCount = len(process.threads())
            self.logger.info("Memory usage%s: RSS=%.1fMB, Threads=%d",
                           f" ({context})" if context else "",
                           memoryInfo.rss / 1024 / 1024,
                           threadCount)
        except Exception as ex:
            self.logger.warning("Failed to log memory usage: %s", str(ex))

    def deleteFilesInParallel(self, ctx, fileList, file_executor):
        """
        Delete files in parallel using the provided executor and gfal2's bulk operations.
        Uses dedicated executor to avoid creating new ones and causing memory leaks.
        :param ctx: gfal2 context
        :param fileList: list of files to delete
        :param file_executor: dedicated executor for file operations
        :return: tuple of (successCount, failCount)
        """
        successCount = 0
        failCount = 0
        numFiles = len(fileList)

        batchSize = self.msConfig['parallelFileDeletionBatchSize']
        msg = f"Have a list of {numFiles} files to delete, "
        msg += f"with a batch size of {batchSize} using the dedicated file executor."
        self.logger.info(msg)

        def deleteBatch(batch):
            """Delete a batch of files and return a list of results"""
            try:
                self.logger.info("Attempting to delete batch of %d files", len(batch))
                results = ctx.unlink(batch)
                self.logger.info("Completed deletion of batch of %d files", len(batch))
                return results
            except Exception as ex:
                self.logger.error("Failed to delete batch: %s", str(ex))
                return ex

        def batchGenerator():
            """Generator to create batches on demand"""
            for i in range(0, len(fileList), batchSize):
                yield fileList[i:i + batchSize]

        self._logMemoryUsage("before parallel deletion")

        try:
            # Submit all directories for deletion using the provided executor
            jobs = (file_executor.submit(deleteBatch, batch) for batch in batchGenerator())

            # Process results as they complete
            for future in concurrent.futures.as_completed(jobs):
                try:
                    results = future.result()
                    if isinstance(results, Exception):
                        self.logger.error("Batch file deletion failed: %s", str(results))
                    else:
                        # Process individual results from the batch
                        for result in results:
                            numFiles -= 1
                            if result is None:
                                successCount += 1
                            else:
                                failCount += 1
                                self._trackGfalError(result)
                except Exception as ex:
                    self.logger.error("Error processing batch deletion result: %s", str(ex))
                finally:
                    # Clean up future immediately
                    self._cleanupFuture(future)
        except Exception as ex:
            self.logger.exception("Unknown exception while deleting files: %s", str(ex))
        finally:
            # any files left in numFiles represent a failure in the file cleanup
            failCount += numFiles

        self.logger.info("Completed parallel file deletion. Success: %d, Failed: %d",
                         successCount, failCount)
        self._logMemoryUsage("after parallel deletion")
        return successCount, failCount

    def _cleanupFuture(self, future):
        """
        Clean up a single future to prevent memory leaks.
        :param future: The future to clean up
        """
        try:
            # Then explicitly cancel the future
            future.cancel()
        except Exception as ex:
            self.logger.debug("Error canceling future: %s", str(ex))
        finally:
            # Finally, delete the future object
            del future
