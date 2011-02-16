#!/usr/bin/python

"""
__Resubmitter__

This is the ACDC-based resubmission class
"""

import os.path
import logging
import traceback

from WMCore.WMException                 import WMException
from WMCore.WMBS.File                   import File
from WMCore.ACDC.DataCollectionService  import DataCollectionService
from WMCore.WorkQueue.WMBSHelper        import WMBSHelper
from WMCore.WMRuntime.SandboxCreator    import SandboxCreator


def resubmitWorkflow(wmSpec):
    """
    __resubmitWorkflow__
    
    Takes a WMSpecHelper and transforms it as necessary for a resubmit
    
    1) Change name from name to name_resubmit
    2) Change output dataset name from name to name_resubmit
    3) Mark the workload resubmit flag
    """
    
    wmSpec.setResubmitFlag(value = True)

    name = wmSpec.name()
    wmSpec.data._internal_name = '%s_resubmit' % name
    
    for task in wmSpec.taskIterator():
        for stepName in task.listAllStepNames():
            for outputModule in task.getOutputModulesForStep(stepName = stepName):
                if not hasattr(outputModule, 'processedDataset'):
                    # Then, really, we can't do anything
                    continue
                proc = outputModule.processedDataset
                outputModule.processedDataset = '%s_resubmit' % (proc)
                
    return wmSpec

class ResubmitterException(WMException):
    """
    _ResubmitterException_

    One day this will actually do something
    """
    pass

    
class Resubmitter:
    """
    _Resubmitter_

    This class loads jobs from failed workflows
    from ACDC and inserts the files, subscriptions,
    etc. back into WMBS
    """



    def __init__(self, config = None):
        """
        __init__

        Nothing to look at here
        """

        self.dataCS = DataCollectionService(url = config.ACDC.couchurl,
                                            database = config.ACDC.database)

        return


    def chunkWorkflow(self, wmSpec, taskName, chunkSize = 100):
        """
        _chunkWorkflow_

        Take a workflow and divide it into chunks from ACDC
        """

        try:
            name         = self.getAdjustedName(adjustedName = wmSpec.name())
            collection   = self.dataCS.getDataCollection(collName = name)
            chunks       = self.dataCS.chunkFileset(collection = collection,
                                                    taskName = taskName,
                                                    chunkSize = chunkSize)
        except WMException:
            raise
        except Exception, ex:
            msg =  "Exception while getting data from ACDC\n"
            msg += str(ex) + '\n'
            msg += str(traceback.format_exc()) + '\n'
            logging.error(msg)
            logging.debug("WMSpec: %s" % wmSpec.data)
            raise ResubmitterException(msg)

        return chunks



    def loadWorkflowChunk(self, wmSpec, taskName, chunkOffset, chunkSize = 100):
        """
        __loadWorkflowChunk__

        Return workflow information from ACDC using offset and chunk size
        """

        try:
            # First, get the real name
            name         = self.getAdjustedName(adjustedName = wmSpec.name())
            collection   = self.dataCS.getDataCollection(collName = name)
            chunks       = self.dataCS.getChunkFiles(collection = collection,
                                                     taskName = taskName,
                                                     chunkOffset = chunkOffset,
                                                     chunkSize = chunkSize)
        except WMException:
            raise
        except Exception, ex:
            msg =  "Exception while getting data from ACDC\n"
            msg += str(ex) + '\n'
            msg += str(traceback.format_exc()) + '\n'
            logging.error(msg)
            logging.debug("WMSpec: %s" % wmSpec.data)
            raise ResubmitterException(msg)

        return chunks


    def getAdjustedName(self, adjustedName):
        """
        _getAdjustedName_

        Get the proper wmSpec name.  If the name
        is not a resubmit, raise an exception.
        """

        if not adjustedName.endswith('_resubmit'):
            # Then we have a non-resubmitted workload.
            # We're not supposed to do this!
            msg =  "Attempting to load non-resubmit workload %s from ACDC\n" % adjustedName
            msg += "Critical error: failing\n"
            logging.error(msg)
            logging.debug("Spec data: %s" % wmSpec.data)
            raise ResubmitterException(msg)
        
        name = adjustedName.rpartition('_resubmit')[0]
        return name

        
