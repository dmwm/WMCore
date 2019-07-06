#!/usr/bin/env python
"""
_Step.Executor.AlcaHarvest_

Implementation of an Executor for a AlcaHarvest step

"""
from __future__ import print_function

import logging
import os
import shutil
import stat
import subprocess

from Utils.Utilities import rootUrlJoin
from WMCore.FwkJobReport.Report import Report
from WMCore.Services.UUIDLib import makeUUID
from WMCore.WMSpec.Steps.Executor import Executor
from WMCore.WMSpec.Steps.WMExecutionFailure import WMExecutionFailure


class AlcaHarvest(Executor):
    """
    _AlcaHarvest_

    Execute a AlcaHarvest Step

    """

    def pre(self, emulator=None):
        """
        _pre_

        Pre execution checks

        """
        # Are we using an emulator?
        if emulator != None:
            return emulator.emulatePre(self.step)

        logging.info("Steps.Executors.%s.pre called", self.__class__.__name__)
        return None

    def execute(self, emulator=None):
        """
        _execute_

        """
        # Are we using emulators again?
        if emulator != None:
            return emulator.emulate(self.step, self.job)

        logging.info("Steps.Executors.%s.execute called", self.__class__.__name__)

        # Search through steps for analysis files
        for step in self.stepSpace.taskSpace.stepSpaces():
            if step == self.stepName:
                # Don't try to parse your own report; it's not there yet
                continue
            stepLocation = os.path.join(self.stepSpace.taskSpace.location, step)
            logging.info("Beginning report processing for step %s", step)
            reportLocation = os.path.join(stepLocation, 'Report.pkl')
            if not os.path.isfile(reportLocation):
                logging.error("Cannot find report for step %s in space %s", step, stepLocation)
                continue

            # First, get everything from a file and 'unpersist' it
            stepReport = Report()
            stepReport.unpersist(reportLocation, step)

            # Don't upload nor stage out files from bad steps.
            if not stepReport.stepSuccessful(step):
                continue

            # Pulling out the analysis files from each step
            analysisFiles = stepReport.getAnalysisFilesFromStep(step)

            # make sure all conditions from this job get the same uuid
            uuid = makeUUID()

            condFiles2copy = []
            lumiFiles2copy = []

            # Working on analysis files
            for analysisFile in analysisFiles:

                # deal with sqlite files
                if analysisFile.FileClass == "ALCA":

                    sqlitefile = analysisFile.fileName.replace('sqlite_file:', '', 1)

                    filenamePrefix = "Run%d@%s@%s" % (self.step.condition.runNumber,
                                                      analysisFile.inputtag, uuid)
                    filenameDB = filenamePrefix + ".db"
                    filenameTXT = filenamePrefix + ".txt"

                    shutil.copy2(os.path.join(stepLocation, sqlitefile), filenameDB)

                    textoutput = "prepMetaData %s\n" % analysisFile.prepMetaData
                    textoutput += "prodMetaData %s\n" % analysisFile.prodMetaData

                    with open(filenameTXT, "w") as fout:
                        fout.write(textoutput)

                    os.chmod(filenameDB,
                             stat.S_IREAD | stat.S_IWRITE | stat.S_IRUSR | stat.S_IWUSR | stat.S_IRGRP | stat.S_IWGRP | stat.S_IROTH | stat.S_IWOTH)
                    os.chmod(filenameTXT,
                             stat.S_IREAD | stat.S_IWRITE | stat.S_IRUSR | stat.S_IWUSR | stat.S_IRGRP | stat.S_IWGRP | stat.S_IROTH | stat.S_IWOTH)

                    condFiles2copy.append(filenameDB)
                    condFiles2copy.append(filenameTXT)

                # deal with text files containing lumi info
                elif analysisFile.FileClass == "ALCATXT":

                    shutil.copy2(os.path.join(stepLocation, analysisFile.fileName), analysisFile.fileName)
                    lumiFiles2copy.append(analysisFile.fileName)

            # copy conditions files out and fake the job report
            addedOutputFJR = False
            if self.step.condition.lfnbase:
                logging.info("Copy out conditions files to %s", self.step.condition.lfnbase)
                for file2copy in condFiles2copy:

                    logging.info("==> copy %s", file2copy)

                    targetLFN = os.path.join(self.step.condition.lfnbase, file2copy)
                    targetPFN = "root://eoscms//eos/cms%s" % targetLFN

                    command = "env XRD_WRITERECOVERY=0 xrdcp -s -f %s %s" % (file2copy, targetPFN)

                    p = subprocess.Popen(command, shell=True,
                                         stdout=subprocess.PIPE,
                                         stderr=subprocess.STDOUT)
                    output = p.communicate()[0]
                    if p.returncode > 0:
                        msg = 'Failure during condition copy to EOS:\n'
                        msg += '   %s\n' % output
                        logging.error(msg)
                        raise WMExecutionFailure(60319, "AlcaHarvestFailure", msg)

                    # add fake output file to job report
                    addedOutputFJR = True
                    stepReport.addOutputFile(self.step.condition.outLabel,
                                             aFile={'lfn': targetLFN,
                                                    'pfn': targetPFN,
                                                    'module_label': self.step.condition.outLabel})

            # copy luminosity files out
            if self.step.luminosity.url:
                logging.info("Copy out luminosity files to %s", self.step.luminosity.url)
                for file2copy in lumiFiles2copy:

                    logging.info("==> copy %s", file2copy)

                    targetPFN = rootUrlJoin(self.step.luminosity.url, file2copy)
                    if not targetPFN:
                        msg = 'No valid URL for lumi copy:\n'
                        msg += '   %s\n' % self.step.luminosity.url
                        logging.error(msg)
                        raise WMExecutionFailure(60319, "AlcaHarvestFailure", msg)

                    command = "env XRD_WRITERECOVERY=0 xrdcp -s -f %s %s" % (file2copy, targetPFN)

                    p = subprocess.Popen(command, shell=True,
                                         stdout=subprocess.PIPE,
                                         stderr=subprocess.STDOUT)
                    output = p.communicate()[0]
                    if p.returncode > 0:
                        msg = 'Failure during copy to EOS:\n'
                        msg += '   %s\n' % output
                        logging.error(msg)
                        raise WMExecutionFailure(60319, "AlcaHarvestFailure", msg)

            if not addedOutputFJR:
                # no conditions from AlcaHarvest is a valid result, can
                # happen if calibration algorithms produced no output
                # due to not enough statistics or other reasons
                #
                # add fake placeholder output file to job report
                logging.info("==> no sqlite files from AlcaHarvest job, creating placeholder file record")
                stepReport.addOutputFile(self.step.condition.outLabel,
                                         aFile={'lfn': "/no/output",
                                                'pfn': "/no/output",
                                                'module_label': self.step.condition.outLabel})

            # Am DONE with report
            # Persist it
            stepReport.persist(reportLocation)

        return

    def post(self, emulator=None):
        """
        _post_

        Post execution checkpointing

        """
        # Another emulator check
        if emulator is not None:
            return emulator.emulatePost(self.step)

        logging.info("Steps.Executors.%s.post called", self.__class__.__name__)
        return None
