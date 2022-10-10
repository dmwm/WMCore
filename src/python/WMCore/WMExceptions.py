#!/usr/bin/python
"""
_WMExceptions_

- List of standard exception ids and their
mappings to a human readable message.
"""

WMEXCEPTION = {'WMCORE-1': 'Not allowed to instantiate ',
               'WMCORE-2': 'Problem creating database table ',
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

WM_JOB_ERROR_CODES = {-1: "Error return without specification.",
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
                      70: "Invalid Arguments: Not all the required arguments for cmsRun is passed",
                      71: "Failed to initiate Scram project",
                      72: "Failed to enter Scram project directory",
                      73: "Failed to get Scram runtime",  # Keeping old error here, as not sure from there previously it was taken: "Failed writing to read-only file system"
                      74: "Unable to untar sandbox",
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
                      8029: "The job configuration has no input files specified for secondary input source.",
                      8030: "Exceeded maximum allowed VSize (ExceededResourceVSize).",
                      8031: "Exceeded maximum allowed RSS (ExceededResourceRSS).",
                      8032: "Exceeded maximum allowed time (ExceededResourceTime).",
                      8033: "Could not write output file (FileWriteError) (usually local disk problem).",
                      8034: "FileNameInconsistentWithGUID",
                      8501: "EventGenerationFailure",
                      9000: "cmsRun caught (SIGINT and SIGUSR2) signal.",
                      10031: "Directory VO_CMS_SW_DIR not found. ",  # (CRAB3)
                      10032: "Failed to source CMS Environment setup script such as cmssset_default.sh, grid system or site equivalent script.",  # (CRAB3)
                      10034: "Required application version is not found at the site.",  # (CRAB3)
                      10040: "failed to generate cmsRun cfg file at runtime.",  # (CRAB3)
                      10042: "Unable to stage-in wrapper tarball.",  # (CRAB3)
                      10043: "Unable to bootstrap WMCore libraries (most likely site python is broken).",  # (CRAB3)
                      11001: "Error during job bootstrap: A sandbox must be specified",  # (WMA)
                      11002: "Error during job bootstrap: A job index must be specified",  # (WMA)
                      11003: "Error during job bootstrap: VO_CMS_SW_DIR, OSG_APP, CVMFS  or /cvmfs were not found.",  # (WMA)
                      11004: "Error during job bootstrap: job environment does not contain the init.sh script.",  # (WMA)
                      11005: "Error during job bootstrap: python isn't available on the worker node.",  # (WMA)
                      50110: "Executable file is not found.",  # (WMA)
                      50111: "Executable file has no exe permissions.",  # (WMA)
                      50113: "Executable did not get enough arguments.",  # (CRAB3)
                      50115: "cmsRun did not produce a valid job report at runtime (often means cmsRun segfaulted).",  # (WMA, CRAB3)
                      50116: "Could not determine exit code of cmsRun executable at runtime.",  # (WMA)
                      50513: "Failure to run SCRAM setup scripts.",  # (WMA, CRAB3)
                      50660: "Application terminated by wrapper because using too much RAM (PSS).",  # (WMA, CRAB3)
                      50662: "Application terminated by wrapper because using too much disk.",  # (CRAB3)
                      50664: "Application terminated by wrapper because using too much Wall Clock time.",  # (WMA, CRAB3)
                      50665: "Application terminated by wrapper because it stay idle too long.",  # (CRAB3)
                      50669: "Application terminated by wrapper for not defined reason.",  # (CRAB3)
                      60302: "Output file(s) not found.",  # (CRAB3)
                      60307: "General failure during files stage out.",  # (WMA, CRAB3)
                      60311: "Local Stage Out Failure using site specific plugin.",  # (WMA, CRAB3)
                      60312: "Failure in staging in log files during log collection (WMAgent).",  # (WMA)
                      60313: "Failed to clean up any files that were no longer needed (WMAgent).",  # (WMA)
                      60315: "StageOut initialisation error (Due to TFC, SITECONF etc).",  # (WMA)
                      60317: "Forced timeout for stuck stage out.",  # (To be used in CRAB3/ASO)
                      60318: "Internal error in Crab cmscp.py stageout script.",  # (CRAB3)
                      60319: "Failed to do AlcaHarvest stageout.",  # (WMA)
                      60320: "Failure to communicate with ASO server.",  # (CRAB3)
                      60321: "Site related issue: no space, SE down, refused connection.",  # (To be used in CRAB3/ASO)
                      60322: "User is not authorized to write to destination site.",  # (To be used in CRAB3/ASO)
                      60323: "User quota exceeded.",  # (To be used in CRAB3/ASO)
                      60324: "Other stageout exception.",  # (To be used in CRAB3/ASO)
                      60401: "Failure to assemble LFN in direct-to-merge (WMAgent).",  # (WMA)
                      60403: "Timeout during files stage out.",  # (WMA, CRAB3)
                      60404: "Timeout during staging of log archives- status unknown (WMAgent).",  # (WMA)
                      60405: "General failure to stage out log archives (WMAgent).",  # (WMA)
                      60407: "Timeout in staging in log files during log collection (WMAgent).",  # (WMA)
                      60408: "Failure to stage out of log files during log collection (WMAgent).",  # (WMA)
                      60409: "Timeout in stage out of log files during log collection (WMAgent).",  # (WMA)
                      60450: "No output files present in the report",  # (WMA)
                      60451: "Output file lacked adler32 checksum (WMAgent).",  # (WMA)
                      70318: "Failure in DQM upload.",
                      70452: "No run/lumi information in file (WMAgent).",
                      71101: "No sites are available to submit the job because the location of its input(s) do not pass the site whitelist/blacklist restrictions (WMAgent).",
                      71102: "The job can only run at a site that is currently in Aborted state (WMAgent).",
                      71103: "The job can run only at a site that is currently in Draining state (WMAgent).",
                      71104: "JobSubmitter component could not find a job pickle object.",
                      71105: "JobSubmitter component loaded an empty job pickle object.",
                      71300: "The job was killed by the WMAgent, reason is unknown (WMAgent).",
                      71301: "The job was killed by WMAgent because the site it was supposed to run at was set to Aborted (WMAgent).",
                      71302: "The job was killed by WMAgent because the site it was supposed to run at was set to Draining (WMAgent).",
                      71303: "The job was killed by WMAgent because the site it was supposed to run at was set to Down (WMAgent).",
                      71304: "The job was killed by the WMAgent for using too much wallclock time (WMAgent) Job status was Running.",
                      71305: "The job was killed by the WMAgent for using too much wallclock time (WMAgent) Job status was Pending.",
                      71306: "The job was killed by the WMAgent for using too much wallclock time (WMAgent) Job status was Error.",
                      71307: "The job was killed by the WMAgent for using too much wallclock time (WMAgent) Job status was Unkown.",
                      80000: "Internal error in CRAB job wrapper.",  # CRAB3
                      80001: "No exit code set by job wrapper.",  # CRAB3
                      80453: "Unable to determine pset hash from output file (CRAB3).",
                      90000: "Error in CRAB3 post-processing step (includes basically errors in stage out and file metadata upload).",  # CRAB3 TODO: Need to separate. new stageout errors are 60321-60324 and other needed for file metadata.
                      99108: "Skipping this step due to a failure in a previous one.",  # WMA
                      99109: "Uncaught exception in WMAgent step executor.",  # WMA TODO: Maybe move to 7****?
                      99303: "Job failed and the original job report got lost",
                      99304: "Could not find jobCache directory. Job will be failed",
                      99305: "Found single input file with too many events to be processed in a pilot lifetime",
                      99400: "Job is killed by condor_rm or SYSTEM_PERIODIC_REMOVE",
                      99401: "Job is killed by unknown reason ",
                      99996: "Failed to find a step report in the worker node",
                      99997: "Failed to load job report",
                      99998: "Job report with size 0",
                      99999: "Some generic error with the job report"
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
# 60317: "Forced timeout for stuck stage out.",  # (To be used in CRAB3/ASO)
# 60402: "Failure to assemble LFN in direct-to-merge by event (WMAgent).", # (WMA)
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
key - exitCode which is also defined in WM_JOB_ERROR_CODES # TODO: to be used for reporting to dashboard and to end users in crab status command
value - is a list, which has dictionaries with the following content:
    regex - Error message which is exposed by gfal-copy/del.
    error-msg - Error msg which will be shown to users in crab status output
    isPermanent - True or False, which is used in CRAB3/ASO for following reasons:
           a) CRAB3 decides should it submit a task to gridScheduler; If it is not permanent,
              task will be submitted, but also error message will be shown in crab status output
           b) CRAB3 Postjob decides should it retry job or not;
           c) ASO decides should it resubmit transfer to FTS;
"""
STAGEOUT_ERRORS = {60317: [{"regex": ".*Cancelled ASO transfer after timeout.*",
                            "error-msg": "ASO Transfer canceled due to timeout.",
                            "isPermanent": True}
                          ],
                   60321: [{"regex": ".*reports could not open connection to.*",
                            "error-msg": "Storage element is not accessible.",
                            "isPermanent": False},
                           {"regex": ".*451 operation failed\\: all pools are full.*",
                            "error-msg": "Destination site does not have enough space on their storage.",
                            "isPermanent": True},
                           {"regex": ".*system error in connect\\: connection refused.*",
                            "error-msg": "Destination site storage refused connection.",
                            "isPermanent": False}
                          ],
                   60322: [{"regex": ".*permission denied.*",
                            "error-msg": "Permission denied.",
                            "isPermanent": True},
                           {"regex": ".*Permission refused.*",
                            "error-msg": "Permission denied.",
                            "isPermanent": True},
                           {"regex": ".*operation not permitted.*",
                            "error-msg": "Operation not allowed.",
                            "isPermanent": True},
                           {"regex": ".*mkdir\\(\\) fail.*",
                            "error-msg": "Can`t create directory.",
                            "isPermanent": True},
                           {"regex": ".*open/create error.*",
                            "error-msg": "Can`t create directory/file on destination site.",
                            "isPermanent": True},
                           {"regex": ".*mkdir\\: cannot create directory.*",
                            "error-msg": "Can`t create directory.",
                            "isPermanent": True},
                           {"regex": ".*530-login incorrect.*",
                            "error-msg": "Permission denied to write to destination site.",
                            "isPermanent": True},
                          ],
                   60323: [{"regex": ".*does not have enough space.*",
                            "error-msg": "User quota exceeded.",
                            "isPermanent": True},
                           {"regex": ".*disk quota exceeded.*",
                            "error-msg": "Disk quota exceeded.",
                            "isPermanent": True},
                           {"regex": ".*HTTP 507.*",
                            "error-msg": "HTTP 507: Disk quota exceeded or Disk full.",
                            "isPermanent": True},
                          ]}
