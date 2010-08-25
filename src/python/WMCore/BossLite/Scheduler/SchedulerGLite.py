#!/usr/bin/env python
"""
gLite CLI interaction class through JSON formatted output
"""

__revision__ = "$Id: SchedulerGLite.py,v 1.1 2010/05/18 14:32:06 spigafi Exp $"
__version__ = "$Revision: 1.1 $"
__author__ = "filippo.spiga@cern.ch"

import os
import tempfile
import re

from WMCore.BossLite.Scheduler.SchedulerInterface import SchedulerInterface
from WMCore.BossLite.Common.Exceptions import SchedulerError
from WMCore.BossLite.DbObjects.Job import Job
from WMCore.BossLite.DbObjects.Task import Task
from WMCore.BossLite.DbObjects.RunningJob import RunningJob

# manage json library using the appropriate WMCore wrapper
from WMCore.Wrappers import JsonWrapper as json

##########################################################################

class BossliteJsonDecoder(json.JSONDecoder):
    """
    Override JSON decode
    """
    
    def __init__(self):
        
        # call super
        super(BossliteJsonDecoder, self).__init__()
        
        # cache pattern to optimize reg-exp substitution
        self.pattern1 = re.compile('([^ \t\n\r\f\v\{\}]+)\s')
        self.pattern2 = re.compile(':"(["|\{])')
        self.pattern3 = re.compile('"[\s]*"')
    
    def decodeSubmit(self, jsonString):
        """
        specialized method to decode JSON output of glite-wms-job-submit
        """
        
        # pre-processing the string before decoding        
        toParse = jsonString.replace( '\n' , ' ' )
        toParse = self.pattern1.sub(r'"\1"', toParse )
        toParse = self.pattern2.sub(r'":\1', toParse )
        toParse = self.pattern3.sub(r'","', toParse )
        
        parsedJson = self.decode(toParse)
        
        return parsedJson  

##########################################################################
    
def hackTheEnv(prependCommand = ''):
    """
    HaCk ThE eNv  *IMPORTANT NOTES* 
    - this hack is necessary if the SYSTEM python under  '/usr/bin') and 
        the EXTERNAL python have the same version
    - the hack is necessary only for CLI which are python(2) script
    - the hack reverts PATH & LD_LYBRARY_PATH if an external PYTHON is present
    - during the hack, replicate entries will be dropped
    - the hack MUST be placed between the 'proxyString'and the CLI command
    """
    
    newEnv = prependCommand + ' '
    
    try :
        pythonCategory = os.environ['PYTHON_CATEGORY']
        
        # revert PATH & LD_LYBRARY_PATH
        pyVersionToRemove = os.environ['PYTHON_VERSION']
        
        originalPath = os.environ['PATH']
        originalLdLibPath = os.environ['LD_LIBRARY_PATH']
        
        newPath = ''
        newLdLibPath = ''
        
        # build new PATH
        for x in list(set(originalPath.split(':'))) :
            if x.find(pyVersionToRemove) == -1 :
                newPath += x + ':'
        newEnv += 'PATH=' + newPath[:-1] + ' '       
         
        # build new LD_LIBRARY_PATH
        for x in list(set(originalLdLibPath.split(':'))) :
            if x.find(pyVersionToRemove) == -1 :
                newLdLibPath += x + ':'   
        newEnv += 'LD_LIBRARY_PATH=' + newLdLibPath[:-1] + ' '
        
    except :
        # revert not necessary or something went wrong during the hacking
        pass
    
    return newEnv

##########################################################################

class SchedulerGLite(SchedulerInterface) :
    """
    basic class to handle gLite jobs using CLI + JSON 
    formatted output to interact with the WMS
    """
    
    def __init__( self, **args):
        
        # call super class init method
        super(SchedulerGLite, self).__init__(**args)
        
        # some initializations
        self.warnings = []
        
        # typical options
        self.vo = args.get( "vo", "cms" )
        self.service = args.get( "service", "" )
        self.config = args.get( "config", "" )
        self.delegationId = args.get( "proxyname", "bossproxy" )
        
        # rename output files with submission number
        self.renameOutputFiles = args.get( "renameOutputFiles", 0 )
        self.renameOutputFiles = int( self.renameOutputFiles )
        
        # x509 string & hackEnv for CLI commands
        if self.cert != '':
            self.proxyString = "env X509_USER_PROXY=" + self.cert + ' '
            self.hackEnv = hackTheEnv()
        else :
            self.proxyString = ''
            self.hackEnv = hackTheEnv('env')
            
        # this section requires an improvement....    
        if os.environ.get('CRABDIR') :
            self.commandQueryPath = os.environ.get('CRABDIR') + \
                                    '/external/ProdCommon/BossLite/Scheduler/'
        elif os.environ.get('PRODCOMMON_ROOT') :
            self.commandQueryPath = os.environ.get('PRODCOMMON_ROOT') + \
                                        '/lib/ProdCommon/BossLite/Scheduler/'
        else :
            # Impossible to locate GLiteQueryStatus.py ...
            raise SchedulerError('Impossible to locate GLiteQueryStatus.py ')      
        
        # cache pattern to optimize reg-exp substitution
        self.pathPattern = re.compile('location:([\S]+)$', re.M)
        self.patternCE = re.compile('(?<= - ).*(?=:)', re.M)
        
        # init BossliteJsonDecoder specialized class
        self.myJSONDecoder = BossliteJsonDecoder()

        # Raise an error if UI is old than 3.2 ...
        version, ret = self.ExecuteCommand( 'glite-version' )
        version = version.strip()
        if version.find( '3.2' ) != 0 :
            raise SchedulerError( 'SchedulerGLite is allowed on UI >3.2' )


    ##########################################################################

    def delegateProxy( self, wms = '' ):
        """
        delegate proxy to _all_ wms or to specific one (if explicitly passed)
        """

        command = "glite-wms-job-delegate-proxy -d " + self.delegationId
     
        if wms :
            command += " -e " + wms
            
        if len(self.config) != 0 :
            command += " -c " + self.config
        
        msg, ret = self.ExecuteCommand( self.proxyString + command )

        if ret != 0 or msg.find("Error -") >= 0 :
            self.logging.warning( "Warning : %s" % msg )

    ##########################################################################
    
    def submit( self, obj, requirements='', config ='', service='' ):
        """
        submit a jdl to glite
        ends with a call to retrieve wms and job,gridid asssociation
        """

        # decode object
        jdl = self.decode( obj, requirements )
        
        # check config file
        if not config :
            config = self.config
        
        # write a jdl into tmpFile
        tmp, fname = tempfile.mkstemp( suffix = '.jdl', prefix = obj['name'],
                                       dir = os.getcwd() )
        tmpFile = os.fdopen(tmp, "w")
        tmpFile.write( jdl )
        tmpFile.close()
        
        # delegate proxy
        if self.delegationId != "" :
            command = "glite-wms-job-submit --json -d " \
                                                + self.delegationId
            self.delegateProxy(service)
        else :
            command = "glite-wms-job-submit --json -a "
        
        if len(config) != 0 :
            command += " -c " + config

        # the '-e' override the ...
        if service != '' :
            command += ' -e ' + service

        command += ' ' + fname
        out, ret = self.ExecuteCommand( self.proxyString + command )
        
        os.unlink( fname )
        
        if ret != 0 :
            raise SchedulerError('error executing glite-wms-job-submit', out)
        
        try:
            
            jOut = self.myJSONDecoder.decodeSubmit(out)
            
        except ValueError:
            raise SchedulerError('error parsing JSON output',  out)
        

        returnMap = {}
        if type(obj) == Task:
            self.logging.debug("Your job identifier is: %s" % jOut['parent'])
            
            for child in jOut['children'].keys() :
                returnMap[str(child.replace('NodeName_', '', 1))] = \
                                                str(jOut['children'][child])
                                                
            # submission converts . to _ in job name - convert back
            for job in obj.jobs:
                if job['name'].count('.'):
                    returned_name = job['name'].replace('.', '_')
                    returnMap[job['name']] = returnMap.pop(returned_name)

            return returnMap, str(jOut['parent']), str(jOut['endpoint']) 
        elif type(obj) == Job:
            # usually we submit collections.....
            self.logging.debug("Your job identifier is: %s" % jOut['jobid'])
            
            returnMap[str(child.replace('NodeName_', '', 1))] = \
                                                str(jOut['children'][child])
            # submission converts . to _ in job name - convert back
            if obj['name'].count('.'):
                returned_name = obj['name'].replace('.', '_')
                returnMap[obj['name']] = returnMap.pop(returned_name)

            return returnMap, str(jOut['parent']), str(jOut['endpoint'])
        else : 
            raise SchedulerError( 'unexpected error',  type(obj) )

        
    ##########################################################################

    def getOutput( self, obj, outdir='' ):
        """
        retrieve job output
        """
        
        # sanity check: if outdir is '' or None perform getoutput operation in
        # the current working directory
        if outdir == '' or outdir is None :
            outdir = '.'
        
        if type(obj) == Job :
            
            # check for the RunningJob integrity
            if not self.valid( obj.runningJob ):
                raise SchedulerError('invalid object', str( obj.runningJob ))
            
            # the object passed is a valid Job, let's go on ...
                
            command = "glite-wms-job-output --json --noint --dir " + outdir + " " \
                                + obj.runningJob['schedulerId']
            
            out, ret = self.ExecuteCommand( self.proxyString + command ) 
                
            if ret != 0 :
                if out.find("Proxy File Not Found") != -1 :
                    # Proxy missing
                    # # adapting the error string for JobOutput requirements
                    obj.runningJob.errors.append("Proxy Missing")
                elif out.find("Output files already retrieved") != -1 : 
                    # Output files already retrieved --> Archive!
                    self.logging.warning( obj.runningJob['schedulerId'] + \
                      ' output already retrieved.' )
                    obj.runningJob.warnings.append("Job has been purged, " + \
                                                        "recovering status")
                else : 
                    self.logging.error( out )
                    obj.runningJob.errors.append( out )
                                           
            elif ret == 0 and out.find("result: success") == -1 :
                # Excluding all the previous cases however something went wrong
                self.logging.error( obj.runningJob['schedulerId'] + \
                      ' problems during getOutput operation.' )
                obj.runningJob.errors.append(out)     
            
            else :
                # Output successfully retrieved without problems
                
                # let's move outputs in the right place...
                # required ONLY for local file stage-out
                tmp = re.search(self.pathPattern, out)
                
                if tmp :
                    command = "mv " + tmp.group(1) + "/* " + outdir + "/"
                    os.system( command )
                    
                    command = "rm -rf " + tmp.group(1)
                    os.system( command )
                # 
                
                self.logging.debug("Output of %s successfully retrieved" 
                                        % str(obj.runningJob['schedulerId'])) 
            
            if obj.runningJob.isError() :
                raise SchedulerError( obj.runningJob.errors[0][0], \
                                           obj.runningJob.errors[0][1] )
                    
        elif type(obj) == Task :
            
            # the object passed is a Task
            
            for selJob in obj.jobs:
                
                if not self.valid( selJob.runningJob ):
                    continue
                
                command = "glite-wms-job-output --json --noint --dir " + outdir + " " \
                          + selJob.runningJob['schedulerId']
                
                out, ret = self.ExecuteCommand( self.proxyString + command )

                if ret != 0 :
                    if out.find("Proxy File Not Found") != -1 :
                        # Proxy missing
                        # adapting the error string for JobOutput requirements
                        selJob.runningJob.errors.append("Proxy Missing")
                    elif out.find("Output files already retrieved") != -1 : 
                        # Output files already retrieved --> Archive!
                        self.logging.warning( 
                                    selJob.runningJob['schedulerId'] + \
                                                ' output already retrieved.' )
                        selJob.runningJob.warnings.append(
                                    "Job has been purged, recovering status")
                    else : 
                        self.logging.error( out )
                        selJob.runningJob.errors.append( out )
                                               
                elif ret == 0 and out.find("result: success") == -1 :
                    # Excluding all previous cases however something went wrong
                    self.logging.error( selJob.runningJob['schedulerId'] + \
                          ' problems during getOutput operation.' )
                    selJob.runningJob.errors.append(out)   
                
                else :
                    # Output successfully retrieved without problems
                    
                    # let's move outputs in the right place...
                    # required ONLY for local file stage-out
                    tmp = re.search(self.pathPattern, out)
                    
                    if tmp :
                        command = "mv " + tmp.group(1) + "/* " + outdir + "/"
                        os.system( command )
                        
                        command = "rm -rf " + tmp.group(1)
                        os.system( command )
                    # 
                    
                    self.logging.debug("Output of %s successfully retrieved" 
                                % str(selJob.runningJob['schedulerId']))
        
        else:
             # unknown object type
            raise SchedulerError('wrong argument type', str( type(obj) ))      


    ##########################################################################
    
    def purgeService( self, obj ):
        """
        Purge job (even bulk) from wms
        """
        
        # Implement as getOutput where the "No output files ..."
        # is not an error condition but the expected status
      
        if type(obj) == Job and self.valid( obj.runningJob ):
            
            # the object passed is a valid Job
                
            command = "glite-wms-job-output --json --noint --dir /tmp/ " \
                      + obj.runningJob['schedulerId']
            
            out, ret = self.ExecuteCommand( self.proxyString + command )
            
            if ( out.find("No output files to be retrieved") != -1 or \
                  out.find("Output files already retrieved") != -1 ) :
                # now this is the expected exit condition... 
                self.logging.debug("Purge of %s successfully" 
                                   % str(obj.runningJob['schedulerId']))
            else : 
                obj.runningJob.errors.append(out)
            
            
        elif type(obj) == Task :
            
            # the object passed is a Task
            
            for job in obj.jobs:
                
                if not self.valid( job.runningJob ):
                    continue
                
                command = "glite-wms-job-output --json --noint --dir /tmp/ " \
                          + job.runningJob['schedulerId']
                
                out, ret = self.ExecuteCommand( self.proxyString + command )
                
                if   ( out.find("No output files to be retrieved") != -1 or \
                      out.find("Output files already retrieved") != -1 ) :
                    # now this is the expected exit condition... 
                    self.logging.debug("Purge of %s successfully" 
                                       % str(job.runningJob['schedulerId']))
                else : 
                    job.runningJob.errors.append(out)
                
                

    ##########################################################################

    def kill( self, obj ):
        """
        kill job
        """
        
        # the object passed is a job
        if type(obj) == Job and self.valid( obj.runningJob ):
            
            # check for the RunningJob integrity
            schedIdList = str( obj.runningJob['schedulerId'] ).strip()
        
        # the object passed is a Task
        elif type(obj) == Task :
            
            schedIdList = ""
            for job in obj.jobs:
                if not self.valid( job.runningJob ):
                    continue
                schedIdList += " " + \
                               str( job.runningJob['schedulerId'] ).strip()
        
        command = "glite-wms-job-cancel --json --noint " + schedIdList
        
        out, ret = self.ExecuteCommand( self.proxyString + command )
        
        if ret != 0 :
            raise SchedulerError('error executing glite-wms-job-cancel', out)
        elif ret == 0 and out.find("result: success") == -1 :
            raise SchedulerError('error', out)


    ##########################################################################

    def matchResources( self, obj, requirements='', config='', service='' ):
        """
        execute a resources discovery through WMS
        IMPORTANT NOTE: glite-wms-job-list-match doesn't accept collections!
        """
        
        # write a fake jdl file
        tmp, fname = tempfile.mkstemp( "", "glite_list_match_", os.getcwd() )
        
        tmpFile = os.fdopen(tmp, "w")
        
        if not config :
            config = self.config
            
        fakeJdl = "[\n"
        fakeJdl += 'Type = "job";\n'
        fakeJdl += 'Executable = "/bin/echo";\n'
        # fakeJdl += 'Arguments  = "";\n'
        
        try :
            requirements = requirements.strip()
            while requirements[0] == '[':
                requirements = requirements[1:-1].strip()
            fakeJdl += '\n' + requirements + '\n'
        except :
            pass
        
        fakeJdl += 'SignificantAttributes = {"Requirements", "Rank", "FuzzyRank"};'
        fakeJdl += "\n]\n"
        
        tmpFile.write( fakeJdl )
        tmpFile.close()
        
        # delegate proxy
        if self.delegationId == "" :
            command = "glite-wms-job-list-match -d " + self.delegationId
            self.delegateProxy(service)
        else :
            command = "glite-wms-job-list-match -a "
        
        # Is it necessary ? 
        if len(config) != 0 :
            command += " -c " + config

        if service != '' :
            command += ' -e ' + service

        command += " " + fname
        
        outRaw, ret = self.ExecuteCommand( self.proxyString + command )
        
        os.unlink( fname )
        
        if ret == 0 : 
            out = self.patternCE.findall(outRaw)
        else :
            raise SchedulerError( 'Error matchResources', outRaw )
        
        
        # return CE without duplicate
        listCE=list(set(out))
        if len(listCE)==0:
            self.logging.debug('List match performed with following requirements:\n %s'%str(fakeJdl))  
        return listCE
    
    
    ##########################################################################

    def postMortem( self, schedulerId, outfile, service):
        """
        perform scheduler logging-info
        """
        
        command = "glite-wms-job-logging-info -v 3 " + schedulerId + \
                  " > " + outfile
        
        out, ret = self.ExecuteCommand( self.proxyString + self.hackEnv + command )
            
        return out


    ##########################################################################
    
    def query(self, obj, service='', objType='node') :
        """
        query status and eventually other scheduler related information
        """
        
        # jobId for remapping
        jobIds = {}

        # parent Ids for status query
        parentIds = []

        # counter for job position in list
        count = 0
                
        # the object passed is a Task:
        if type(obj) == Task :

            if objType == 'node' :
                
                # loop!
                for job in obj.jobs :
                                  
                    if job.runningJob is None \
                           or job.runningJob.active != True \
                           or job.runningJob['schedulerId'] is None \
                           or job.runningJob['closed'] == "Y" \
                           or job.runningJob['status'] in self.invalidList :
                        count += 1
                        continue
                    
                    # append in joblist
                    jobIds[ str(job.runningJob['schedulerId']) ] = count
                        
                    count += 1
                
                if jobIds :
                    formattedJobIds = ','.join(jobIds)
                                   
                    command = self.commandQueryPath \
                        + 'GLiteStatusQuery.py --jobId=%s' % formattedJobIds
                    
                    outJson, ret = self.ExecuteCommand(self.proxyString + self.hackEnv + command)
                                           
                    # Check error
                    if ret != 0 :
                        # obj.errors doesn't exist for Task object...
                        obj.warnings.append( "Errors: " + str(outJson.strip()) )
                        raise SchedulerError(
                            'error executing GLiteStatusQuery', \
                                                 str(outJson.strip()))
                    else :
                        # parse JSON output
                        try:
                            out = json.loads(outJson)
                            # DEBUG # print json.dumps( out,  indent=4 )
                        except ValueError:
                            raise SchedulerError('error parsing JSON', outJson )
                  
            elif objType == 'parent' :
                
                # loop!
                for job in obj.jobs :
                                   
                    # consider just valid jobs
                    if self.valid( job.runningJob ) :
                        
                        # append in joblist
                        jobIds[ str(job.runningJob['schedulerId']) ] = count
                        
                        # update unique parent ids list
                        if job.runningJob['schedulerParentId'] \
                                not in parentIds:
                            parentIds.append( 
                                str(job.runningJob['schedulerParentId']))
                        
                    count += 1
            
                if jobIds :
                    formattedParentIds = ','.join(parentIds)
                    formattedJobIds = ','.join(jobIds)
                    
                    command = self.commandQueryPath \
                        + 'GLiteStatusQuery.py --parentId=%s --jobId=%s' \
                            % (formattedParentIds, formattedJobIds)
                    
                    outJson, ret = self.ExecuteCommand(self.proxyString + self.hackEnv + command)
                                           
                    # Check error
                    if ret != 0 :
                        # obj.errors doesn't exist for Task object...
                        obj.warnings.append( "Errors: " + str(outJson.strip()) )
                        raise SchedulerError(
                            'error executing GLiteStatusQuery', \
                                                 str(outJson.strip()))
                    else :
                        # parse JSON output
                        try:
                            out = json.loads(outJson)                     
                            # DEBUG # print json.dumps( out,  indent=4 )
                        except ValueError:
                            raise SchedulerError('error parsing JSON', outJson )
            
            if jobIds :
                # Refill objects...  
                for jobId in jobIds.values() : 
                    
                    jobIdToUpdate = obj.jobs[jobId].runningJob['schedulerId']
                    
                    updatedJobInfo = out[jobIdToUpdate]
                    
                    for currentKey in updatedJobInfo.keys() :
                        if updatedJobInfo[currentKey] is None :
                            pass
                        else :
                            obj.jobs[jobId].runningJob[currentKey] = \
                                updatedJobInfo[currentKey]
                
        # unknown object type
        else:
            raise SchedulerError('wrong argument type', str( type(obj) ))


    ##########################################################################

    def jobDescription (self, obj, requirements='', config='', service = ''):
        """
        retrieve scheduler specific job description
        """
        
        return self.decode( obj, requirements )

        
    ##########################################################################

    def decode  ( self, obj, requirements='' ) :
        """
        prepare file for submission
        """
        
        if type(obj) == RunningJob or type(obj) == Job :
            return self.jdlFile ( obj, requirements ) 
        elif type(obj) == Task :
            return self.collectionJdlFile ( obj, requirements ) 


    ##########################################################################

    def jdlFile( self, job, requirements='' ) :
        """
        build a job jdl
        """
        
        # general part
        jdl = "[\n"
        jdl += 'Type = "job";\n'
        jdl += 'Executable = "%s";\n' % job[ 'executable' ]
        jdl += 'Arguments  = "%s";\n' % job[ 'arguments' ]
        if job[ 'standardInput' ] != '':
            jdl += 'StdInput = "%s";\n' % job[ 'standardInput' ]
        jdl += 'StdOutput  = "%s";\n' % job[ 'standardOutput' ]
        jdl += 'StdError   = "%s";\n' % job[ 'standardError' ]

        # input files handling
        infiles = ''
        for infile in job['fullPathInputFiles'] :
            if infile != '' :
                infiles += '"file://' + infile + '",'
        if len( infiles ) != 0 :
            jdl += 'InputSandbox = {%s};\n'% infiles[:-1]

        # output bypass WMS?
        #if task['outputDirectory'] is not None and \
        #       task['outputDirectory'].find('gsiftp://') >= 0 :
        #    jdl += 'OutputSandboxBaseDestURI = "%s";\n' % \
        #           task['outputDirectory']

        # output files handling
        outfiles = ''
        for filePath in job['outputFiles'] :
            if filePath == '' :
                continue
            if self.renameOutputFiles :
                outfiles += '"' + filePath + '_' + \
                            str(job.runningJob[ 'submission' ]) + '",'
            else :
                outfiles += '"' + filePath + '",'

        if len( outfiles ) != 0 :
            jdl += 'OutputSandbox = {%s};\n' % outfiles[:-1]

        # extra job attributes
        if job.runningJob is not None \
               and job.runningJob[ 'schedulerAttributes' ] is not None :
            jdl += job.runningJob[ 'schedulerAttributes' ]

        # blindly append user requirements
        jdl += requirements + '\n]\n'

        # return values
        return jdl


    ##########################################################################
    
    def collectionJdlFile ( self, task, requirements='' ):
        """
        build a collection jdl easy to be handled by the wmproxy API interface
        and gives back the list of input files for a better handling
        """
        
        # general part for task
        jdl = "[\n"
        jdl += 'Type = "collection";\n'

        # global task attributes :
        # \\ the list of files for the JDL common part
        globalSandbox = ''
        # \\ the list of common files to be put in every single node
        #  \\ in the form root.inputsandbox[ISBindex]
        commonFiles = ''
        isbIndex = 0

        # task input files handling:
        if task['startDirectory'] is None or task['startDirectory'][0] == '/':
            # files are stored locally, compose with 'file://'
            if task['globalSandbox'] is not None :
                for ifile in task['globalSandbox'].split(','):
                    if ifile.strip() == '' :
                        continue
                    filename = os.path.abspath( ifile )
                    globalSandbox += '"file://' + filename + '",'
                    commonFiles += "root.inputsandbox[%d]," % isbIndex
                    isbIndex += 1
        else :
            # files are elsewhere, just add their composed path
            if task['globalSandbox'] is not None :
                jdl += 'InputSandboxBaseURI = "%s";\n' % task['startDirectory']
                for ifile in task['globalSandbox'].split(','):
                    if ifile.strip() == '' :
                        continue
                    if ifile.find( 'file:/' ) == 0:
                        globalSandbox += '"' + ifile + '",'
                        
                        commonFiles += "root.inputsandbox[%d]," % isbIndex
                        isbIndex += 1
                        continue
                    commonFiles += '"' + ifile + '",'

        # output bypass WMS?
        if task['outputDirectory'] is not None and \
               task['outputDirectory'].find('gsiftp://') >= 0 :
            jdl += 'OutputSandboxBaseDestURI = "%s";\n' % \
                   task['outputDirectory']

        # single job definition
        jdl += "Nodes = {\n"
        for job in task.jobs :
            jdl += '[\n'
            jdl += 'NodeName   = "NodeName_%s";\n' % job[ 'name' ]
            jdl += 'Executable = "%s";\n' % job[ 'executable' ]
            jdl += 'Arguments  = "%s";\n' % job[ 'arguments' ]
            if job[ 'standardInput' ] != '':
                jdl += 'StdInput = "%s";\n' % job[ 'standardInput' ]
            jdl += 'StdOutput  = "%s";\n' % job[ 'standardOutput' ]
            jdl += 'StdError   = "%s";\n' % job[ 'standardError' ]

            # extra job attributes
            if job.runningJob is not None \
                   and job.runningJob[ 'schedulerAttributes' ] is not None :
                jdl += job.runningJob[ 'schedulerAttributes' ]

            # job output files handling
            outfiles = ''
            for filePath in job['outputFiles'] :
                if filePath == '' :
                    continue
                if self.renameOutputFiles :
                    outfiles += '"' + filePath + '_' + \
                                str(job.runningJob[ 'submission' ]) + '",'
                else :
                    outfiles += '"' + filePath + '",'

            if len( outfiles ) != 0 :
                jdl += 'OutputSandbox = {%s};\n' % outfiles[:-1]

            # job input files handling:
            # add their name in the global sandbox and put a reference
            localfiles = commonFiles
            if task['startDirectory'] is None \
                   or task['startDirectory'][0] == '/':
                # files are stored locally, compose with 'file://'
                for filePath in job['fullPathInputFiles']:
                    if filePath != '' :
                        localfiles += "root.inputsandbox[%d]," % isbIndex
                    globalSandbox += '"file://' + filePath + '",'
                    isbIndex += 1
            else :
                # files are elsewhere, just add their composed path
                for filePath in job['fullPathInputFiles']:
                    if filePath[0] == '/':
                        filePath = filePath[1:]
                    localfiles += '"' + filePath + '",'

            if localfiles != '' :
                jdl += 'InputSandbox = {%s};\n'% localfiles[:-1]
            jdl += '],\n'
        jdl  = jdl[:-2] + "\n};\n"

        # global sandbox definition
        if globalSandbox != '' :
            jdl += "InputSandbox = {%s};\n"% (globalSandbox[:-1])

        # blindly append user requirements
        try :
            requirements = requirements.strip()
            while requirements[0] == '[':
                requirements = requirements[1:-1].strip()
            jdl += '\n' + requirements + '\n'
        except :
            # catch a generic exception (?)
            pass

        # close jdl
        jdl += 'SignificantAttributes = {"Requirements", "Rank", "FuzzyRank"};'
        jdl += "\n]\n"

        # return values
        return jdl


    ##########################################################################
    
    def lcgInfoVo (self, tags, fqan, seList=None, blacklist=None,  
                  whitelist=None, full=False):
        """
        execute a resources discovery through bdii
        returns a list of resulting sites
        *DEPRECATED*: THIS METHOD CAN BE DELETED BECAUSE NOT UTILIZED
        """

        celist = []

        # set to None invalid entries
        if seList == [''] or seList == []:
            seList = None
        # set to None invalid entries
        if whitelist == [''] or whitelist == []:
            whitelist = None
        # set to [] invalid entries so that the lopp does't need checks
        if blacklist == [''] or blacklist == None:
            blacklist = []

        if len( tags ) != 0 :
            query =  ','.join( ["Tag=%s" % tag for tag in tags ] ) + \
                    ',CEStatus=Production'
        else :
            query = 'CEStatus=Production'

        if seList == None :
            command = "lcg-info --vo " + fqan + " --list-ce --query " + \
                       "\'" + query + "\' --sed"
            self.logging.debug('issuing : %s' % command)

            out, ret = self.ExecuteCommand( self.proxyString + command )
            for ce in out.split() :
                # blacklist
                passblack = 1
                if ce.find( "blah" ) == -1:
                    for ceb in blacklist :
                        if ce.find(ceb) >= 0:
                            passblack = 0
                # whitelist if surviving the blacklist selection
                if passblack:
                    if whitelist is None:
                        celist.append( ce )
                    elif len(whitelist) == 0:
                        celist.append( ce )
                    else:
                        for cew in whitelist:
                            if ce.find(cew) != -1:
                                celist.append( ce )
            return celist

        for se in seList :
            singleComm = "lcg-info --vo " + fqan + \
                         " --list-ce --query " + \
                         "\'" + query + ",CloseSE="+ se + "\' --sed"
            self.logging.debug('issuing : %s' % singleComm)

            out, ret = self.ExecuteCommand( self.proxyString + singleComm )
            for ce in out.split() :
                # blacklist
                passblack = 1
                if ce.find( "blah" ) == -1:
                    for ceb in blacklist :
                        if ce.find(ceb) != -1:
                            passblack = 0
                # whitelist if surviving the blacklist selection
                if passblack:
                    if whitelist is None:
                        celist.append( ce )
                    elif len(whitelist) == 0:
                        celist.append( ce )
                    else:
                        for cew in whitelist:
                            if ce.find(cew) >= 0:
                                celist.append( ce )

            # a site matching is enough
            if not full and celist != []:
                break
        return celist


    ##########################################################################
    
    def lcgInfo (self, tags, vos, seList=None, blacklist=None, 
                whitelist=None, full=False):
        """
        execute a resources discovery through bdii
        returns a list of resulting sites
        *DEPRECATED*: THIS METHOD CAN BE DELETED BECAUSE NOT UTILIZED
        """

        result = []
        for fqan in vos :
            res = self.lcgInfoVo( tags, fqan, seList,
                                  blacklist, whitelist, full)
            if not full and res != [] :
                return res
            else :
                for value in res :
                    if value in result :
                        continue
                    result.append( value )

        return result
