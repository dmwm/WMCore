#!/usr/bin/env python
"""
_Step.Executor.StageOut_

Implementation of an Executor for a StageOut step

"""

__revision__ = "$Id: StageOut.py,v 1.5 2009/12/09 21:52:53 mnorman Exp $"
__version__ = "$Revision: 1.5 $"

import inspect
import os
import os.path
import logging

from WMCore.WMSpec.Steps.Executor           import Executor
from WMCore.WMSpec.Steps.WMExecutionFailure import WMExecutionFailure
from WMCore.FwkJobReport.Report             import Report
import WMCore.Storage.StageOutMgr as StageOutMgr

class StageOut(Executor):
    """
    _StageOut_

    Execute a StageOut Step

    """        

    def pre(self, emulator = None):
        """
        _pre_

        Pre execution checks

        """

        #Are we using an emulator?
        if (emulator != None):
            return emulator.emulatePre( self.step )


        
        print "Steps.Executors.StageOut.pre called"
        return None


    def execute(self, emulator = None, **overrides):
        """
        _execute_


        """
        #Are we using emulators again?
        if (emulator != None):
            return emulator.emulate( self.step, self.job )
        
        # naw man, this is real
        # iterate over all the incoming files
        manager = StageOutMgr.StageOutMgr(**overrides)
        manager.numberOfRetries = self.step.retryCount
        manager.retryPauseTime  = self.step.retryDelay

        #We need to find a list of steps in our task
        #And eventually a list of jobReports for out steps

        #Search through steps for report files
        #Search through steps for report files
        filesTransferred = []
        for step in self.stepSpace.taskSpace.stepSpaces():
            if step == self.stepName:
                #Don't try to parse your own report; it's not there yet
                continue
            stepLocation = os.path.join(self.stepSpace.taskSpace.location, step)
            logging.info("Beginning report processing for step %s" %(step))
            reportLocation = os.path.join(stepLocation, 'Report1.pkl')
            if os.path.isfile(reportLocation):
                #First, get everything from a file and 'unpersist' it
                stepReport = Report(step)
                stepReport.unpersist(reportLocation)
                taskID = getattr(stepReport.data, 'id', None)
                #print stepReport.data
                if hasattr(stepReport.data, step):
                    #Once you have the file, make sure you can get the step out of the jobReport
                    stepSegment = getattr(stepReport.data, step)
                    if hasattr(stepSegment, 'output'):
                        #Once you have the step, you need the output
                        stepOutput = getattr(stepSegment, 'output')
                        for output in stepOutput:
                            if hasattr(output, 'files'):
                                stepFiles = getattr(output, 'files')
                                for file in stepFiles:
                                    if hasattr(file, 'LFN') and hasattr(file, 'PFN'):
                                        #Save the input PFN in case we need it
                                        #Undecided whether to move file.PFN to the output PFN
                                        file.InputPFN        = file.PFN
                                        fileForTransfer = {'LFN': getattr(file, 'LFN'), \
                                                           'PFN': getattr(file, 'PFN'), \
                                                           'SEName' : None, \
                                                           'StageOutCommand': None}
                                        try:
                                            manager(fileForTransfer)
                                            #Afterwards, the file should have updated info.
                                            filesTransferred.append(fileForTransfer)
                                            file.StageOutCommand = fileForTransfer['StageOutCommand']
                                            file.SEName          = fileForTransfer['SEName']
                                            file.OutputPFN       = fileForTransfer['PFN']

                                        except:
                                            print "Exception raised in stageout executor, how do we handle that?"
                                            raise
                                    else:
                                        msg = "Not a file"


                            else:
                                msg = "Could not find any files in output"
                                logging.error(msg)
                    else:
                        msg = "Could not find an output module in step %s" % (step)
                        logging.error(msg)

                    #The file has to be written back

                else:
                    #This is weird, because the step isn't in its own taskSpace
                    msg = "Could not find any mention of step %s in report in the step TaskSpace" %(step)
                    logging.error(msg)

                #Am DONE with report
                #Persist it
                stepReport.persist(reportLocation)

            else:
                logging.error("Cannot find report for step %s in space %s" %(step, stepLocation))

        #Done with all steps, and should have a list of stagedOut files in fileForTransfer
        print filesTransferred
        return
    


    def post(self, emulator = None):
        """
        _post_

        Post execution checkpointing

        """
        #Another emulator check
        if (emulator != None):
            return emulator.emulatePost( self.step )
        
        print "Steps.Executors.StageOut.post called"
        return None


