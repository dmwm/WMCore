#!/usr/bin/env python
"""
Base handler for taskEnd.
"""
__all__ = []
__revision__ = "$Id: TaskEndHandler.py,v 1.6 2009/09/29 12:23:03 delgadop Exp $"
__version__ = "$Revision: 1.6 $"

from WMCore.WMFactory import WMFactory

from TQComp.Constants import taskStates
#from TQComp.Constants import reportUrlDir, uploadRoot

from traceback import extract_tb
import sys
from os.path import dirname, basename, isdir
from os import makedirs, unlink
import tarfile

import threading

class TaskEndHandler(object):
    """
    Handler for pilot's taskEnd message.
    """
   
    def __init__(self, params = None):
        """
        Constructor. The params argument can be used as a dict for any
        parameter (if needed). Basic things can be obtained from currentThread.

        The required params are as follow:
           uploadBaseUrl, specBasePath
        """
#        required = ["uploadBaseUrl", "specBasePath"]
        required = []
        for param in required:
            if not param in params:
                messg = "GetTaskHandler object requires params['%s']" % param
                # TODO: What number?
                numb = 0
                raise WMException(messg, numb)

#        self.uploadBaseUrl = params["uploadBaseUrl"]
#        self.specBasePath = params["specBasePath"]
        myThread = threading.currentThread()
        factory = WMFactory("default", \
                  "TQComp.Database."+myThread.dialect)
        self.queries = factory.loadObject("Queries")
        self.logger = myThread.logger
    
    
    # this we overload from the base handler
    def __call__(self, event, payload):
        """
        Handles the event with payload.
        """
        # load queries for backend.
        myThread = threading.currentThread()
   
        # Now handle the message
        fields = {}
        if event in ['taskEnd']:
   
            self.logger.debug('TaskEndHandler:TaskEnd:payload: %s' % payload)
            
            # Extract message attributes
            required = ['pilotId', 'taskId', 'exitStatus']
            for param in required:
                if not param in payload:
                    result = 'Error'
                    fields = {'Error': "errorReport message requires \
'%s' field in payload" % param}
#                    myThread.transaction.rollback()
                    return {'msgType': result, 'payload': fields}

            pilotId = payload['pilotId']
            taskId = payload['taskId']
            status = payload['exitStatus']
            possibleStatus = ('Done', 'Failed')
            if not (status in possibleStatus):
                result = 'Error'
                fields = {'Error': 'exitStatus should be one of %s' % \
                                   str(possibleStatus)}
                self.logger.warning("Incorrect exitStatus: %s" % status)
#                    myThread.transaction.rollback()
                return {'msgType': result, 'payload': fields}
              
            try:
                myThread.transaction.begin()

                # Retrieve the task spec and check the task is in this pilot
                res = self.queries.selectWithFilter('tq_tasks', \
                         {'id': taskId}, fields = ['spec', 'pilot'])
#                res = self.queries.getTasksWithFilter({'id': taskId}, \
#                                          fields = ['spec', 'pilot'])
                self.logger.debug("Res: %s" % res)
                if (not res) or (res[0][1] != pilotId):
                    result = 'Error'
                    if (not res):
                        fields = {'Error': 'No task with id %s' % (taskId)}
                    else:
                        fields = {'Error': 'Task %s not in pilot %s' % \
                                           (taskId, pilotId)}
                    myThread.transaction.rollback()
                    return {'msgType': result, 'payload': fields}
             
                spec = res[0][0]

                # If the output dir tarball was uploaded, untar it
                self.__checkOutputDirTar(spec, status)

                # Mark the task as Done/Failed in the DB
                vars = {'state': taskStates[status]}
                self.queries.updateOneTask(taskId, vars)
                self.logger.debug("Task updated as %s." % status)
        
                # Log in the tq_pilot_log table
                self.queries.logPilotEvent(pilotId, 'TaskEnd', \
                  'Task status: %s' % (status), taskId)

                # Give the result back
                fields['TaskId'] = taskId
                fields['Info'] = 'Task updated as %s.' % status
                result = 'TaskEndACK'
      
                # Commit
                myThread.transaction.commit()
              
            except:
                ttype, val, tb = sys.exc_info()
                myThread.transaction.rollback()
                messg = 'Error in TaskEnd, due to: %s - %s '% (ttype, val)
                self.logger.warning(messg + "Trace: %s"% extract_tb(tb,limit=5))
                result = 'Error'
                fields = {'Error': messg}
              
            return {'msgType': result, 'payload': fields}
   
        else:
            # unexpected message, scream?
            pass
  


    def __checkOutputDirTar(self, spec, status):
        """
        Checks if there is a tarball with the correct name in 
        the job's dir, and, if so, untars it into the appropriate dir
        structure. For the moment, we don't shout if the file is not 
        there or is wrong.
        """
        # Tarball should be on the same dir as specFile
        specDir = dirname(spec)
        jobId = basename(spec).replace('-JobSpec.xml', '')
        tarFile = specDir + '/' + 'outdir_' + jobId + '.tgz'
        try:
            # Try to open the tarfile
            f = tarfile.open(tarFile)
        except: 
            # File not there, forget it
            messg = "Tar file for job %s not present. Skipping." % jobId
            self.logger.debug('TaskEndHandler: %s' % messg)
            return

        # File there, go on: create dir to untar
        if status == 'Done':
            tarDir = specDir + '/JobTracking/Success'
        else:
            tarDir = specDir + '/JobTracking/Failed'

        if not isdir(tarDir):
            try:
                makedirs(tarDir)
            except:
                ttype, val, tb = sys.exc_info()
                messg = "Error when creating dir for job %s's output" % jobId
                messg += ": %s - %s"  % (ttype, val)
                self.logger.debug('TaskEndHandler: %s' % messg)
                f.close()
                return
      
        # Arrived here... extract tar
        for member in f.getmembers():
            # Do not extract dangerous files
            if member.name.strip().startswith('/') or ('..' in member.name):
                messg = "Skipping dangerous file (%s) " % (member.name)
                messg += "from job %s' output dir" % jobID
                self.logger.warning('TaskEndHandler: %s' % messg)
            else:
                f.extract(member.name, tarDir)
        
        # Done with it, close and remove
        f.close()
        unlink(tarFile)

        # And that's it
        return

        


#    def __buildReportUrl(self, thefile):
#        # TODO: maybe need to change the file name/location for error reports?
#        path = thefile.replace(self.specBasePath, reportUrlDir+'/')
#        path = path.replace(thefile.split('/')[-1], 'FrameworkJobReport.xml')
#        return self.uploadBaseUrl+'/'+uploadRoot+'/'+path
        
