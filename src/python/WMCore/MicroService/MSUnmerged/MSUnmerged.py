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
from datetime import datetime

# WMCore modules
from WMCore.MicroService.DataStructs.DefaultStructs import UNMERGED_REPORT
from WMCore.MicroService.MSCore import MSCore
from WMCore.MicroService.MSUnmerged.MSUnmergedRSE import MSUnmergedRSE
from WMCore.Services.RucioConMon.RucioConMon import RucioConMon
from WMCore.Services.WMStatsServer.WMStatsServer import WMStatsServer
# from WMCore.Services.AlertManager.AlertManagerAPI import AlertManagerAPI
from WMCore.WMException import WMException
from Utils.Pipeline import Pipeline, Functor

import random


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

    def __init__(self, msConfig, logger=None):
        """
        Runs the basic setup and initialization for the MSUnmerged module
        :param msConfig: micro service configuration
        """
        super(MSUnmerged, self).__init__(msConfig, logger=logger)

        self.msConfig.setdefault("verbose", True)
        self.msConfig.setdefault("interval", 60)
        # self.msConfig.setdefault('limitRSEsPerInstance', 100)
        # self.msConfig.setdefault('limitTiersPerInstance', ['T1', 'T2', 'T3'])
        # self.msConfig.setdefault("rucioAccount", "FIXME_RUCIO_ACCT")
        self.msConfig.setdefault("rseExpr", "*")
        self.msConfig.setdefault("rucioConMon", "https://cmsweb-testbed.cern.ch/rucioconmon/")
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
                                                Functor(self.cleanRSE),
                                                Functor(self.updateRSECounters, pName),
                                                Functor(self.updateRSETimestamps, start=False, end=True),
                                                Functor(self.purgeRseObj, dumpRSE=True)])
        # Initialization of the deleted files counters:
        self.rseCounters = {}
        self.plineCounters = {}
        self.rseTimestamps = {}
        self.rseConsStats = {}
        self.protectedLFNs = []

    def execute(self):
        """
        Executes the whole MSUnmerged logic
        :return: summary
        """
        # start threads in MSManager which should call this method
        summary = dict(UNMERGED_REPORT)

        # refresh statistics on every poling cycle
        self.rseConsStats = self.rucioConMon.getRSEStats()
        self.protectedLFNs = self.wmstatsSvc.getProtectedLFNs()

        try:
            rseList = self.getRSEList()
            random.shuffle(rseList)
            msg = "  retrieved %s RSEs. " % len(rseList)
            msg += "Service set to process up to %s RSEs per instance." % self.msConfig["limitRSEsPerInstance"]
            self.logger.info(msg)
        except Exception as err:  # general error
            msg = "Unknown exception while trying to estimate the final RSEs to work on. Error: %s", str(err)
            self.logger.exception(msg)
            self.updateReportDict(summary, "error", msg)

        try:
            totalNumRses, totalNumFiles, numRsesCleaned, numFilesDeleted = self._execute(rseList)
            msg = "\nTotal number of processed RSEs: %s."
            msg += "\nTotal number of files to be deleted: %s."
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
                pline.run(MSUnmergedRSE(rseName))
            except MSUnmergedPlineExit as ex:
                msg = "%s: Run on RSE: %s was interrupted due to: %s. "
                msg += "\nWill retry again in the next cycle."
                self.logger.exception(msg, pline.name, rseName, ex.message)
                continue
            except Exception as ex:
                msg = "%s: General error from pipeline. RSE: %s. Error:  \n%s."
                msg += "\nWill retry again in the next cycle."
                self.logger.exception(msg, pline.name, rseName, str(ex))
                continue
        return self.plineCounters[pline.name]['totalNumRses'], self.plineCounters[pline.name]['totalNumFiles'], self.plineCounters[pline.name]['rsesCleaned'], self.plineCounters[pline.name]['deletedSuccess']

    def cleanRSE(self, rse):
        """
        The method to implement the actual deletion of files for an RSE.
        :param rse: MSUnmergedRSE object to be cleaned
        :return:    The MSUnmergedRSE object
        """
        try:
            # for fileUnmerged in rse['files']['toDelete']:
            #     try:
            #         self.gfalCommand(rse['delInterface'], fileUnmerged)
            #         rse['counters']['numFilesDeleted'] += 1
            #         rse['files']['deletedSuccess'].append(fileUnmerged)
            #     except Exception as ex:
            #         rse['files']['deletedFail'].append(fileUnmerged)
            #         msg = "Error while trying to delete file: %s for RSE: %s"
            #         msg += "Will retry in the next cycle. Err: %s"
            #         self.logger.debug(msg, fileUnmerged, rse['name'], str(ex))
            rse['isClean'] = self._checkClean(rse)
        except Exception as ex:
            msg = "Error while cleaning RSE: %s"
            msg += "Will retry in the next cycle. Err: %s"
            self.logger.debug(msg, rse['name'], str(ex))

        return rse

    def _checkClean(self, rse):
        """
        A simple function to check if every file in an RSE's unmerged area have
        been deleted
        :param rse: The RSE to be checked
        :return:    Bool: True if all files found have been deleted, False otherwise
        """
        return rse['counters']['totalNumFiles'] == rse['counters']['deletedSuccess']

    def consRecordAge(self, rse):
        """
        A method to heck the duration of the consistency record for the RSE
        :param rse: The RSE to be checked
        :return:    rse
        """
        rseName = rse['name']
        isConsDone = self.rseConsStats[rseName]['status'] == 'done'
        isConsNewer = self.rseConsStats[rseName]['end_time'] > self.rseTimestamps[rseName]['prevStartTime']
        if isConsDone and isConsNewer:
            return rse
        else:
            msg = "Old consistency record for RSE: %s. Skipping it in the current run."
            self.logger.info(msg, rseName)
            raise MSUnmergedPlineExit()

    def getUnmergedFiles(self, rse):
        """
        Fetches all the records of unmerged files per RSE from Rucio Consistency Monitor
        and puts the list in the rse obj.
        :param rse: The RSE to work on
        :return:    rse
        """
        rse['files']['allUnmerged'] = self.rucioConMon.getRSEUnmerged(rse['name'])
        return rse

    def filterUnmergedFiles(self, rse):
        """
        This method is applying set compliment operation to the set of unmerged
        files per RSE in order to exclude the protected LFNs.
        :param rse: The RSE to work on
        :return:    rse
        """
        return rse

    def purgeRseObj(self, rse, dumpRSE=False):
        """
        Cleaning all the records in an RSE object. The final method to be used
        before an RSE exits a pipeline.
        :param rse: The RSE to be checked
        :return:    rse
        """
        if dumpRSE:
            msg = "\n----------------------------------------------------------"
            msg += "\nMSUnmergedRSE: %s"
            msg += "\n----------------------------------------------------------"
            self.logger.debug(msg, pformat(rse))
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
        currTime = datetime.now().timestamp()

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
        return rseList
