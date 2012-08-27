#!/usr/bin/env python
"""
_Step.Executor.AlcaHarvest_

Implementation of an Executor for a AlcaHarvest step

"""
import os
import sys
import stat
import shutil
import tarfile
import logging

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

        print "Steps.Executors.AlcaHarvest.pre called"
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

            # are we in validation mode ?
            dropboxValidation = False

            # make sure all conditions from this job get the same uuid
            uuid = makeUUID()

            files2copy = []

            # Working on analysis files
            for analysisFile in analysisFiles:
                # only deal with sqlite files
                if analysisFile.FileClass == "ALCA":

                    sqlitefile = analysisFile.fileName.replace('sqlite_file:', '', 1)

                    filenamePrefix = "Run%d@%s@%s" % (self.step.condition.runNumber,
                                                      analysisFile.tag, uuid)
                    filenameDB = filenamePrefix + ".db"
                    filenameTXT = filenamePrefix + ".txt"
                    filenameTAR = filenamePrefix + ".tar.bz2"

                    shutil.copy2(os.path.join(stepLocation, sqlitefile), filenameDB)

                    # if we run in validation mode, upload to different destination
                    if dropboxValidation != "False":
                        analysisFile.destDB = analysisFile.destDBValidation

                    textoutput = "destDB %s\n" % analysisFile.destDB
                    textoutput += "tag %s\n" % analysisFile.tag
                    textoutput += "inputtag %s\n" % analysisFile.inputtag
                    textoutput += "since\n"
                    textoutput += "Timetype %s\n" % analysisFile.Timetype
                    textoutput += "IOVCheck %s\n" % getattr(analysisFile, 'IOVCheck', "offline")
                    textoutput += "DuplicateTagHLT %s\n" % getattr(analysisFile, 'DuplicateTagHLT', "")
                    textoutput += "DuplicateTagEXPRESS %s\n"  % getattr(analysisFile, 'DuplicateTagEXPRESS', "")
                    textoutput += "DuplicateTagPROMPT %s\n" % analysisFile.DuplicateTagPROMPT
                    textoutput += "Source %s\n" % getattr(analysisFile, 'Source', "")
                    textoutput += "Fileclass ALCA\n"

                    fout = open(filenameTXT, "w")
                    fout.write(textoutput)
                    fout.close()

                    os.chmod(filenameDB, stat.S_IREAD | stat.S_IWRITE | stat.S_IRUSR | stat.S_IWUSR | stat.S_IRGRP | stat.S_IWGRP | stat.S_IROTH | stat.S_IWOTH)
                    os.chmod(filenameTXT, stat.S_IREAD | stat.S_IWRITE | stat.S_IRUSR | stat.S_IWUSR | stat.S_IRGRP | stat.S_IWGRP | stat.S_IROTH | stat.S_IWOTH)

                    if dropboxValidation == "False":

                        fout = tarfile.open(filenameTAR, "w:bz2")
                        fout.add(filenameDB)
                        fout.add(filenameTXT)
                        fout.close()

                        os.chmod(filenameTAR, stat.S_IREAD | stat.S_IWRITE | stat.S_IRUSR | stat.S_IWUSR | stat.S_IRGRP | stat.S_IWGRP | stat.S_IROTH | stat.S_IWOTH)

                        files2copy.append(filenameTAR)

                    else:

                        files2copy.append(filenameDB)
                        files2copy.append(filenameTXT)

            # check and create target directory
            if not os.path.isdir(self.step.condition.dir):
                msg = 'Conditions copy failed with response:\n'
                msg += 'The target dir %s does not exist or is not a directory\n'
                logging.error(msg)
                raise WMExecutionFailure(60319, "AlcaHarvestFailure", msg)

            # copy files out and fake the job report
            logging.info("Copy out conditions files to %s" % self.step.condition.dir)
            for file2copy in files2copy:

                logging.info("==> copy %s" % file2copy) 

                targetFile = os.path.join(self.step.condition.dir, file2copy)

                try:
                    shutil.copy2(file2copy, targetFile)
                except Exception, ex:
                    msg = 'Conditions copy failed with response:\n'
                    msg += 'Error: %s\n' % str(ex)
                    logging.error(msg)
                    raise WMExecutionFailure(60319, "AlcaHarvestFailure", msg)

                # add fake output file to job report
                stepReport.addOutputFile(self.step.condition.outLabel,
                                         file = { 'lfn' : targetFile,
                                                  'pfn' : targetFile })

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
        
        print "Steps.Executors.AlcaHarvest.post called"
        return None
