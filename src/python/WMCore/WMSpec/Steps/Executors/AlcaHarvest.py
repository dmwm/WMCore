#!/usr/bin/env python
"""
_Step.Executor.AlcaHarvest_

Implementation of an Executor for a AlcaHarvest step

"""
from __future__ import print_function
import os
import sys
import stat
import shutil
import tarfile
import logging
import subprocess

from WMCore.WMSpec.Steps.Executor import Executor
from WMCore.FwkJobReport.Report import Report
from WMCore.Services.UUID import makeUUID

from WMCore.WMSpec.Steps.WMExecutionFailure import WMExecutionFailure


class AlcaHarvest(Executor):
    """
    _AlcaHarvest_

    Execute a AlcaHarvest Step

    """
    def pre(self, emulator = None):
        """
        _pre_

        Pre execution checks

        """
        #Are we using an emulator?
        if emulator != None:
            return emulator.emulatePre(self.step)

        print("Steps.Executors.AlcaHarvest.pre called")
        return None

    def execute(self, emulator = None):
        """
        _execute_

        """
        #Are we using emulators again?
        if emulator != None:
            return emulator.emulate(self.step, self.job)

        # Search through steps for analysis files
        for step in self.stepSpace.taskSpace.stepSpaces():
            if step == self.stepName:
                #Don't try to parse your own report; it's not there yet
                continue
            stepLocation = os.path.join(self.stepSpace.taskSpace.location, step)
            logging.info("Beginning report processing for step %s" % step)
            reportLocation = os.path.join(stepLocation, 'Report.pkl')
            if not os.path.isfile(reportLocation):
                logging.error("Cannot find report for step %s in space %s" \
                              % (step, stepLocation))
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

            files2copy = []

            # Working on analysis files
            for analysisFile in analysisFiles:

                # only deal with sqlite files
                if analysisFile.FileClass == "ALCA":

                    sqlitefile = analysisFile.fileName.replace('sqlite_file:', '', 1)

                    filenamePrefix = "Run%d@%s@%s" % (self.step.condition.runNumber,
                                                      analysisFile.inputtag, uuid)
                    filenameDB = filenamePrefix + ".db"
                    filenameTXT = filenamePrefix + ".txt"

                    shutil.copy2(os.path.join(stepLocation, sqlitefile), filenameDB)

                    textoutput = "prepMetaData %s\n" % analysisFile.prepMetaData
                    textoutput += "prodMetaData %s\n" % analysisFile.prodMetaData

                    fout = open(filenameTXT, "w")
                    fout.write(textoutput)
                    fout.close()

                    os.chmod(filenameDB, stat.S_IREAD | stat.S_IWRITE | stat.S_IRUSR | stat.S_IWUSR | stat.S_IRGRP | stat.S_IWGRP | stat.S_IROTH | stat.S_IWOTH)
                    os.chmod(filenameTXT, stat.S_IREAD | stat.S_IWRITE | stat.S_IRUSR | stat.S_IWUSR | stat.S_IRGRP | stat.S_IWGRP | stat.S_IROTH | stat.S_IWOTH)

                    files2copy.append(filenameDB)
                    files2copy.append(filenameTXT)

            # copy files out and fake the job report
            logging.info("Copy out conditions files to %s" % self.step.condition.dir)
            for file2copy in files2copy:

                logging.info("==> copy %s" % file2copy)

                targetLFN = os.path.join(self.step.condition.dir, file2copy)
                targetPFN = "root://eoscms//eos/cms%s" % targetLFN

                command = "xrdcp -s -f %s %s" % (file2copy, targetPFN)

                p = subprocess.Popen(command, shell = True,
                                     stdout=subprocess.PIPE,
                                     stderr=subprocess.STDOUT)
                output = p.communicate()[0]
                if p.returncode > 0:
                    msg = 'Failure during copy to EOS:\n'
                    msg += '   %s\n' % output
                    logging.error(msg)
                    raise WMExecutionFailure(60319, "AlcaHarvestFailure", msg)

                # add fake output file to job report
                stepReport.addOutputFile(self.step.condition.outLabel,
                                         file = { 'lfn' : targetLFN,
                                                  'pfn' : targetPFN,
                                                  'module_label' : self.step.condition.outLabel })

            if len(files2copy) == 0:

                # no output from AlcaHarvest is a valid result, can
                # happen if calibration algorithms produced no output
                # due to not enough statistics or other reasons
                #
                # add fake placeholder output file to job report
                logging.info("==> no sqlite files from AlcaHarvest job, creating placeholder file record")
                stepReport.addOutputFile(self.step.condition.outLabel,
                                         file = { 'lfn' : "/no/output",
                                                  'pfn' : "/no/output",
                                                  'module_label' : self.step.condition.outLabel })

            # Am DONE with report
            # Persist it
            stepReport.persist(reportLocation)

        return

    def post(self, emulator = None):
        """
        _post_

        Post execution checkpointing

        """
        # Another emulator check
        if emulator is not None:
            return emulator.emulatePost(self.step)

        print("Steps.Executors.AlcaHarvest.post called")
        return None
