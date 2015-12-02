#!/usr/bin/python
"""
_WMExceptions_

- List of standard exception ids and their
mappings to a human readable message.
"""

WMEXCEPTION = {'WMCore-1': 'Not allowed to instantiate ',
               'WMCore-2': 'Problem creating database table ',
               'WMCORE-3': 'Could not find in library class ',
               'WMCORE-4': 'Problem with loading the class ',
               'WMCORE-5': 'Dialect not specified in configuration ',
               'WMCORE-6': 'Message name is same as diagnostic message name! ',
               'WMCORE-7': 'Component name is reserved word. ',
               'WMCORE-8': 'No config section for this component! ',
               'WMCORE-9': 'Problem inserting a trigger flag. ',
               'WMCORE-10': 'Problem setting trigger action. ',
               'WMCORE-11': 'Security exception. ',
               'WMCORE-12': 'Database connection problem ',
               'WMCORE-13': 'Number of retries exceeded'}

"""
WM_JOB_ERROR_CODES
List of job error codes present in WMCore, some of them coming from CMSSW, and a description of the error it represents.

IMPORTANT:
Any change which is done for WM_JOB_ERROR_CODES has to be done also in the following twiki page:
https://twiki.cern.ch/twiki/bin/view/CMSPublic/JobExitCodes
----------------
Range(1 - 512)
    standard ones in Unix and indicate a CMSSW abort that the cmsRun did not catch as exception
----------------
Range(7000 - 9000)
    cmsRun (CMSSW) exit codes. These codes may depend on specific CMSSW version
    https://github.com/cms-sw/cmssw/blob/CMSSW_5_0_1/FWCore/Utilities/interface/EDMException.h#L26
----------------
Range(10000 - 19999)
    Failures related to the environment setup
----------------
Range(50000 - 59999)
    Failures related executable file
----------------
Range(60000 - 69999)
    Failures related staging-OUT
----------------
Range(70000 - 79999)
    Failures related only for WMAgent. (which does not fit into ranges before)
----------------
Range(80000 - 89999)
    Failures related only for CRAB3. (which does not fit into ranges before)
----------------
Range(90000 - 99999)
    Other problems which does not fit to any range before.
----------------
"""

WM_JOB_ERROR_CODES = {
        -1: "Error return without specification.",
        1: "Job failed to bootstrap CMSSW; likely a worker node issue.",
        2: "Interrupt (ANSI).",
        4: "Illegal instruction (ANSI).",
        5: "Trace trap (POSIX).",
        6: "Abort (ANSI) or IOT trap (4.2BSD).",
        7: "BUS error (4.2BSD).",
        8: "Floating point exception (ANSI).",
        9: "killed, unblockable (POSIX) kill -9.",
        11: "segmentation violation (ANSI) (most likely user application crashed).",
        15: "Termination (ANSI).",
        24: "Soft CPU limit exceeded (4.2 BSD).",
        25: "File size limit exceeded (4.2 BSD).",
        30: "Power failure restart (System V.).",
        50: "Required application version not found at the site.",
        64: "I/O error: cannot open data file (SEAL).",
        65: "End of job from user application (CMSSW).",
        66: "Application exception.",
        73: "Failed writing to read-only file system.",
        81: "Job did not find functioning CMSSW on worker node.",
        84: "Job failed to open local and fallback files.",
        85: "Job failed to open local and fallback files.",
        90: "Application exception.",
        92: "Job failed to open local and fallback files.",
        126: "Permission problem or command is not an executable",
        127: "Command not found",
        129: "Hangup (POSIX).",
        132: "Illegal instruction (ANSI).",
        133: "Trace trap (POSIX).",
        134: "Abort (ANSI) or IOT trap (4.2 BSD) (most likely user application crashed).",
        135: "Bus error (4.2 BSD).",
        137: "SIGKILL; likely an unrelated batch system kill.",
        139: "Segmentation violation (ANSI).",
        143: "Termination (ANSI)(or incorporate in the msg text).",
        147: "Error during attempted file stageout.",
        151: "Error during attempted file stageout.",
        152: "CPU limit exceeded (4.2 BSD).",
        153: "File size limit exceeded (4.2 BSD).",
        155: "Profiling alarm clock (4.2 BSD).",
        195: "Job did not produce a FJR; will retry.",
        243: "Timeout during attempted file stageout.",
        7000: "Exception from command line processing.",
        7001: "Configuration File Not Found.",
        7002: "Configuration File Read Error.",
        8001: "Other CMS Exception.",
        8002: "std::exception (other than bad_alloc).",
        8003: "Unknown Exception.",
        8004: "std::bad_alloc (memory exhaustion).",
        8005: "Bad Exception Type (e.g throwing a string).",
        8006: "ProductNotFound.",
        8007: "DictionaryNotFound.",
        8008: "InsertFailure.",
        8009: "Configuration.",
        8010: "LogicError.",
        8011: "UnimplementedFeature.",
        8012: "InvalidReference.",
        8013: "NullPointerError.",
        8014: "NoProductSpecified.",
        8015: "EventTimeout.",
        8016: "EventCorruption.",
        8017: "ScheduleExecutionFailure.",
        8018: "EventProcessorFailure.",
        8019: "FileInPathError.",
        8020: "FileOpenError (Likely a site error).",
        8021: "FileReadError (May be a site error).",
        8022: "FatalRootError.",
        8023: "MismatchedInputFiles.",
        8024: "ProductDoesNotSupportViews.",
        8025: "ProductDoesNotSupportPtr.",
        8026: "NotFound (something other than a product or dictionary not found).",
        8027: "FormatIncompatibility.",
        8028: "FileOpenError with fallback.",
        8030: "Exceeded maximum allowed VSize (ExceededResourceVSize).",
        8031: "Exceeded maximum allowed RSS (ExceededResourceRSS).",
        8032: "Exceeded maximum allowed time (ExceededResourceTime).",
        9000: "cmsRun caught (SIGINT and SIGUSR2) signal.",
        10031: "Directory VO_CMS_SW_DIR not found. ", # (CRAB3)
        10032: "Failed to source CMS Environment setup script such as cmssset_default.sh, grid system or site equivalent script.", # (CRAB3)
        10034: "Required application version is not found at the site.", # (CRAB3)
        10040: "failed to generate cmsRun cfg file at runtime.", # (CRAB3)
        10042: "Unable to stage-in wrapper tarball.", # (CRAB3)
        10043: "Unable to bootstrap WMCore libraries (most likely site python is broken).", # (CRAB3)
        50110: "Executable file is not found.", # (WMA)
        50111: "Executable file has no exe permissions.", # (WMA)
        50113: "Executable did not get enough arguments.", # (CRAB3)
        50115: "cmsRun did not produce a valid job report at runtime (often means cmsRun segfaulted).", # (WMA, CRAB3)
        50116: "Could not determine exit code of cmsRun executable at runtime.", # (WMA)
        50513: "Failure to run SCRAM setup scripts.", # (WMA, CRAB3)
        50660: "Application terminated by wrapper because using too much RAM (RSS).", # (WMA, CRAB3)
        50661: "Application terminated by wrapper because using too much Virtual Memory (VSIZE).", # (WMA)
        50662: "Application terminated by wrapper because using too much disk.", # (CRAB3)
        50664: "Application terminated by wrapper because using too much Wall Clock time.", # (WMA, CRAB3)
        50665: "Application terminated by wrapper because it stay idle too long.", # (CRAB3)
        50669: "Application terminated by wrapper for not defined reason.", # (CRAB3)
        60302: "Output file(s) not found.", # (CRAB3)
        60307: "Failed to copy an output file to the SE (sometimes caused by timeout issue).", # (WMA, CRAB3)
        60311: "Local Stage Out Failure using site specific plugin.", # (WMA, CRAB3)
        60312: "Failure in staging in log files during log collection (WMAgent).", # (WMA)
        60315: "StageOut initialisation error (Due to TFC, SITECONF etc).", # (WMA)
        60318: "Internal error in Crab cmscp.py stageout script.", # (CRAB3)
        60319: "Failed to do AlcaHarvest stageout.", # (WMA)
        60320: "Failure to communicate with ASO server.", # (CRAB3)
        60321: "Site related issue: no space, SE down, refused connection.", # (To be used in CRAB3/ASO)
        60322: "User is not authorized to write to destination site.", # (To be used in CRAB3/ASO)
        60323: "User quota exceeded.", # (To be used in CRAB3/ASO)
        60324: "Other stageout exception.", # (To be used in CRAB3/ASO)
        60401: "Failure to assemble LFN in direct-to-merge by size (WMAgent).", # (WMA)
        60402: "Failure to assemble LFN in direct-to-merge by event (WMAgent).", # (WMA)
        60403: "Timeout during attempted file transfer- status unknown.", # (WMA, CRAB3)
        60404: "Timeout during staging of log archives- status unknown (WMAgent).", # (WMA)
        60405: "General failure to stage out log archives (WMAgent).", # (WMA)
        60407: "Timeout in staging in log files during log collection (WMAgent).", # (WMA)
        60408: "Failure to stage out of log files during log collection (WMAgent).", # (WMA)
        60409: "Timeout in stage out of log files during log collection (WMAgent).", # (WMA)
        60450: "No output files present in the report", # (WMA)
        60451: "Output file lacked adler32 checksum (WMAgent).", # (WMA)
        71101: "No sites are available to submit the job because the location of its input(s) do not pass the site whitelist/blacklist restrictions (WMAgent).", # (WMA) TODO: was 61101 changed to 71101
        71102: "The job can only run at a site that is currently in Aborted state (WMAgent).", # (WMA) TODO: was 61102 changed to 71102
        71103: "The JobSubmitter component could not load the job pickle (WMAgent).", # (WMA) TODO: was 61103 changed to 71103
        71104: "The job can run only at a site that is currently in Draining state (WMAgent).", # (WMA) TODO: was 61104 changed to 71104
        71300: "The job was killed by the WMAgent, reason is unknown (WMAgent).", # (WMA) TODO: was 61300 changed to 71300
        71301: "The job was killed by the WMAgent because the site it was running at was set to Aborted (WMAgent).", # (WMA) TODO: was 61301 changed to 71301
        71302: "The job was killed by the WMAgent because the site it was running at was set to Draining (WMAgent).", # (WMA) TODO: was 61302 changed to 71302
        71303: "The job was killed by the WMAgent because the site it was running at was set to Down (WMAgent).", # (WMA) TODO: was 61303 changed to 71303
        71304: "The job was killed by the WMAgent for using too much wallclock time (WMAgent).", # (WMA) TODO: was 61304 changed to 71304
        70318: "Failure in DQM upload.", # (WMA) TODO: Change in WMCore code was 60318, new 70318
        70452: "No run/lumi information in file (WMAgent).", # Was wrong exitCode was 60451 move to 70452 (WMA) # TODO: Change in code 60451
        80000: "Internal error in CRAB job wrapper.", # CRAB3
        80453: "Unable to determine pset hash from output file (CRAB3).", # Was wrong 60453 moved to 80453 (CRAB3) # TODO: Change in CRAB3 code
        90000: "Error in CRAB3 post-processing step (includes basically errors in stage out and file metadata upload).", # CRAB3 TODO: Need to separate. new stageout errors are 60321-60324 and other needed for file metadata.
        99109: "Uncaught exception in WMAgent step executor." # WMA TODO: Maybe move to 7****?
        }

# ======================================================================
# Removed error codes and messages which are not used. (Review date: 2015-12-04)
# ======================================================================
# 10001: "LD_LIBRARY_PATH is not defined.",
# 10002: "Failed to setup LCG_LD_LIBRAR_PATH.",
# 10016: "OSG $WORKING_DIR could not be created.",
# 10017: "OSG $WORKING_DIR could not be deleted.",
# 10018: "OSG $WORKING_DIR could not be deleted.",
# 10020: "Shell script cmsset_default.sh to setup cms environment is not found.",
# 10021: "Failed to scram application project using the afs release area.",
# 10022: "Failed to scram application project using CMS sw disribution on the LCG2.",
# 10030: "Middleware not identified.",
# 10033: "Platform is incompatible with the scram version.",
# 10035: "Scram Project Command Failed.", Not used
# 10036: "Scram Runtime Command Failed.",
# 10037: "Failed to find cms_site_config file in software area.",
# 10038: "Failed to find cms_site_catalogue.sh file in software area.",
# 10039: "cms_site_catalogue.sh failed to provide the catalogue.",
# 10041: "fail to find valid client for output stage out.",
# 50112: "User executable shell file is not found.",
# 50114: "OSG $WORKING_DIR could not be deleted.",
# 50117: "Could not update exit code in job report (a variation of 50115).",
# 50663: "Application terminated by wrapper because using too much CPU time.", Not used
# 50700: "Job Wrapper did not produce any usable output file.", Not used
# 50800: "Application segfaulted (likely user code problem).", Not used
# 50998: "Problem calculating file details (i.e. size, checksum etc).", Not used
# 50999: "OSG $WORKING_DIR could not be deleted.", Not used
# 60300: "Either OutputSE or OutputSE_DIR not defined.",
# 60301: "Neither zip nor tar exists.",
# 60303: "File already exists on the SE.",
# 60304: "Failed to create the summary file (production).",
# 60305: "Failed to create a zipped archive (production).",
# 60306: "Failed to copy and register output file.",
# 60308: "An output file was saved to fall back local SE after failing to copy to remote SE.",
# 60309: "Failed to create an output directory in the catalogue.",
# 60310: "Failed to register an output file in the catalogue.",
# 60313: "Failed to delete the output from the previous run via lcg-del command.", # Mh.. only lcg-del? Not used
# 60314: "Failed to invoke ProdAgent StageOut Script.", Not used
# 60316: "Failed to create a directory on the SE.", Not used
# 60317: "Forced timeout for stuck stage out.", Not used
# 60406: "Failure in staging in log files during log collection (WMAgent).", Not used
# 60410: "Failure in deleting log files in log collection (WMAgent).", # Not used
# 60411: "Timeout in deleting log files in log collection (WMAgent).", # Not used
# 60514: "PreScriptFailure.", # Not used anymore
# 60515: "PreScriptScramFailure.", # Not used anymore
# 60999: "SG $WORKING_DIR could not be deleted.", # Not used
# 70000: "Output_sandbox too big for WMS: output can not be retrieved.", Not used
# 70500: "Warning: problem with ModifyJobReport.", # More details here: https://twiki.cern.ch/twiki/bin/view/CMSPublic/SWGuideCrabFaq#Exit_code_70500 Not used!
# ======================================================================

"""
STAGEOUT_ERRORS
key - error message which is exposed by gfal-copy/rm or lcg-cp;
value - is a dictionary, which has following keys:
    exit-code - Exit code. # TODO: to be used for reporting to dashboard and to end users in crab status command
    error-msg - Error msg which will be shown to users in crab status output
    isPermanent - true or false, which is used in CRAB3/ASO for following reasons:
           a) CRAB3 decides should it submit a task to gridScheduler; If it is not fatal,
              task will be submitted, but also error message shown in crab status output
           b) CRAB3 Postjob decides should it retry job or not;
           c) ASO decides should it resubmit transfer to FTS;
"""
STAGEOUT_ERRORS = {".*cancelled aso transfer after timeout.*": {
                      "exit-code": 60321,
                      "error-msg": "Transfer canceled due to timeout.",
                      "isPermanent": True},
                   ".*reports could not open connection to.*": {
                      "exit-code": 60321,
                      "error-msg": "Storage element is not accessible.",
                      "isPermanent": False},
                   ".*451 operation failed\: all pools are full.*": {
                      "exit-code": 60321,
                      "error-msg": "Destination site does not have enough space on their storage.",
                      "isPermanent": True},
                   ".*system error in connect\: connection refused.*": {
                      "exit-code": 60321,
                      "error-msg": "Destination site storage refused connection.",
                      "isPermanent": False},
                   ".*permission denied.*": {
                      "exit-code": 60322,
                      "error-msg": "Permission denied.",
                      "isPermanent": True},
                   ".*operation not permitted.*": {
                      "exit-code": 60322,
                      "error-msg": "Operation not allowed.",
                      "isPermanent": True},
                   ".*mkdir\(\) fail.*": {
                      "exit-code": 60322,
                      "error-msg": "Can`t create directory.",
                      "isPermanent": True},
                   ".*open/create error.*": {
                      "exit-code": 60322,
                      "error-msg": "Can`t create directory/file on destination site.",
                      "isPermanent": True},
                   ".*mkdir\: cannot create directory.*": {
                      "exit-code": 60322,
                      "error-msg": "Can`t create directory.",
                      "isPermanent": True},
                   ".*530-login incorrect.*": {
                      "exit-code": 60322,
                      "error-msg": "Permission denied to write to destination site.",
                      "isPermanent": True},
                   ".*does not have enough space.*": {
                      "exit-code": 60323,
                      "error-msg": "User quota exceeded.",
                      "isPermanent": True},
                   ".*disk quota exceeded.*": {
                      "exit-code": 60323,
                      "error-msg": "Disk quota exceeded.",
                      "isPermanent": True},
                      }
