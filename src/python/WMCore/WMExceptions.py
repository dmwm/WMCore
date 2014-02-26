#!/usr/bin/python
"""
_WMExceptions_

- List of standard exception ids and their
mappings to a human readable message.
"""

WMEXCEPTION = {'WMCore-1' : 'Not allowed to instantiate ',
   'WMCore-2' : 'Problem creating database table ',
   'WMCORE-3' : 'Could not find in library class ',
   'WMCORE-4' : 'Problem with loading the class ',
   'WMCORE-5' : 'Dialect not specified in configuration ',
   'WMCORE-6' : 'Message name is same as diagnostic message name! ',
   'WMCORE-7' : 'Component name is reserved word. ',
   'WMCORE-8' : 'No config section for this component! ',
   'WMCORE-9' : 'Problem inserting a trigger flag. ',
   'WMCORE-10': 'Problem setting trigger action. ',
   'WMCORE-11': 'Security exception. ',
   'WMCORE-12': 'Database connection problem ',
   'WMCORE-13': 'Number of retries exceeded'}

"""
WMJobErrorCodes
List of job error codes present in WMCore, some of them coming from CMSSW, and a description
of the error it represents.
"""
WMJobErrorCodes = {50660 :
                   "Application terminated by wrapper for using too much RSS",
                   50661 :
                   "Application terminated by wrapper for using too much VSize",
                   50664 :
                   "Application terminated by wrapper for using too much wallclock time",
                   60450 :
                   "No output files present in the report",
                   60451 :
                   "No Adler32 checksum available in file",
                   60452 :
                   "No run/lumi information in file",
                   61101 :
                   "No sites are available to submit the job because the location of its input(s)"
                   "do not pass the site whitelist/blacklist restrictions",
                   61102 :
                   "The job can only run at a site that is currently in Down/Aborted state",
                   61103 :
                   "The JobSubmitter component could not load the job pickle",
                   61104 :
                   "The job can run only at a site that is currently in Draining state",
                   61300 :
                   "The job was killed by the WMAgent, reason is unknown.",
                   61301 :
                   "The job was killed by the WMAgent because the site it was running at was set to Aborted",
                   61302 :
                   "The job was killed by the WMAgent because the site it was running at was set to Draining",
                   61303 :
                   "The job was killed by the WMAgent because the site it was running at was set to Down"}

"""
WMJobPermanentSystemErrors
List of job errors produced by WMCore that are internal to the application and are used
to indicate that a job should not be retried since the error is permanent
"""
WMJobPermanentSystemErrors = [61102]
