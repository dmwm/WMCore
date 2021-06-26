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

# WMCore modules
from WMCore.MicroService.DataStructs.DefaultStructs import UNMERGED_REPORT
from WMCore.MicroService.MSCore import MSCore
from WMCore.MicroService.MSUnmerged.MSUnmergedRSE import MSUnmergedRSE
# from WMCore.Services.AlertManager.AlertManagerAPI import AlertManagerAPI
from WMCore.WMException import WMException
from Utils.Pipeline import Pipeline, Functor


class MSUnmergedException(WMException):
    """
    General Exception Class for MSUnmerged Module in WMCore MicroServices
    """
    def __init__(self, message):
        self.myMessage = "MSUnmergedException: %s" % message
        super(MSUnmergedException, self).__init__(self.myMessage)


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
        # TODO: Add 'alertManagerUrl' to msConfig'
        # self.alertServiceName = "ms-unmerged"
        # self.alertManagerAPI = AlertManagerAPI(self.msConfig.get("alertManagerUrl", None), logger=logger)

        # Building all the Pipelines:
        pName = 'plineUnmerged'
        self.plineUnmerged = Pipeline(name=pName,
                                      funcLine=[Functor(self.cleanFiles)])
        # Initialization of the deleted files counters:
        self.rseCounters = {}
        self.plineCounters = {}

    def execute(self):
        """
        Executes the whole MSUnmerged logic
        :return: summary
        """
        # start threads in MSManager which should call this method
        summary = dict(UNMERGED_REPORT)

        self.resetCounters()

        try:
            rseList = self.getRSEList()
            self.updateReportDict(summary, "total_num_rses", len(rseList))
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
        totalNumRses = 0
        totalNumFiles = 0
        numRsesCleaned = 0
        numFilesDeleted = 0

        # Call the workflow dispatcher:
        for rse in rseList:
            try:
                rse = MSUnmergedRSE(rse)
                self.plineUnmerged.run(rse)
                msg = "\n----------------------------------------------------------"
                msg += "\nMSUnmergedRSE: %s"
                msg += "\n----------------------------------------------------------"
                self.logger.debug(msg, pformat(rse))
                totalNumRses += 1
                if rse['isClean']:
                    numRsesCleaned += 1
                totalNumFiles += rse['counters']['totalNumFiles']
                numFilesDeleted += rse['counters']['numFilesDeleted']
            except Exception as ex:
                msg = "%s: General error from pipeline. RSE: %s. Error:  \n%s. "
                msg += "\nWill retry again in the next cycle."
                self.logger.exception(msg, self.plineUnmerged.name, rse['rse'], str(ex))
                continue
        return totalNumRses, totalNumFiles, numRsesCleaned, numFilesDeleted

    def cleanFiles(self, rse):
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
        return rse['counters']['totalNumFiles'] == rse['counters']['numFilesDeleted']

    def resetCounters(self):
        """
        A simple function for zeroing the deleted files counters.
        """

        for rse in self.rseCounters:
            self.rseCounters[rse]['deletedSuccess'] = 0
            self.rseCounters[rse]['deletedFail'] = 0

        for pline in self.plineCounters:
            self.plineCounters[pline.name]['deletedSuccess'] = 0
            self.plineCounters[pline.name]['deletedFail'] = 0

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
