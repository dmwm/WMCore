#!/usr/bin/env python

"""
_PilotComponent_



"""





import os
import sys
import traceback
import time
import socket
#for logging
import logging
import popen2
import fcntl, select

from NCommunication import Communication 
#import urllib2 


def getlogger():
    """
    __startlogging__
    this will add logging handle for incoming name
    
    Argument:
        name -> name of logging
             
    Return:
        nothing
    """

    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)

    #create console handler and set level to debug
    handler = logging.StreamHandler()
    handler.setLevel(logging.DEBUG)

    #create formatter
    formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - " \
                                   "%(message)s")
    #add formatter to ch
    handler.setFormatter(formatter)

    #add ch to logger
    logger.addHandler(handler)
 
    return logger

def makeNonBlocking(fd):
    """ 
    __makeNonBlocking__ 
    """
    fl = fcntl.fcntl(fd, fcntl.F_GETFL)
    try:
        fcntl.fcntl(fd, fcntl.F_SETFL, fl | os.O_NDELAY)
    except AttributeError:
        fcntl.fcntl(fd, fcntl.F_SETFL, fl | fcntl.FNDELAY)


#isVariableSet
def isVariableSet( envName ):
    """
    __isVariableSet__

    Check if the given evnName is set as an Environement Variable
    Return:
        True if non-none value found, false if None is found
    """
    print ('looking for %s' %envName)
    if ( os.environ.get ( envName ) != None ):
        print '%s:%s' % (envName, os.environ.get ( envName ))
        return True
    print ('sending false for %s' % envName)
    return False

class PilotJob:  
    """ 
    _PilotJob_
    
    a scripts that will start init n 
    then get the job from taskqueue 

    """
    def __init__(self, config):
        """ 
        __init__ 
        """

        self.config = config
        self.processingRealJob = False

        #configurable
        self.heartBeatMsg = True
        self.cacheFiles = []

        #self.pilotid = config["pilotID"]
        #TODO: get it from TaskQueue
        self.pilotId = 12311
        self.ttl = config["TTL"]
		
        #get hostname of the pilot job
        self.pilotHost = socket.getfqdn()

        #Address of the TaskQueue machine		
        self.taskQAddress = config["tqaddress"] 

        #start communication module
        self.commPlugin = Communication(False, self)
        self.commPlugin.start()
	
        msg = "PilotJob Started:\n"
        print(msg)
    
    ###################################################	
    #TODO: this function will try to recover data cache
    ###################################################	
    def dataCacheRecovery(self):
        """
        __dataCacheRecovery__ 
        """
        #logging.debug( 'dataCacheRecovery()' )
        print ( 'dataCacheRecovery()' )
        print (self.cacheFiles)

    #################################################	
    #TODO: this function will try to recover old jobs 
    #which were not finished by pilot
    #################################################
    def jobRecovery(self):
        """ 
        __jobRecovery__ 
        """
    
        #logging.debug( 'jobRecovery()')
        print ( 'jobRecovery() %s '% self.pilotId)
        #otherwise return some job list
        return None

    ###################################
    # pilotEnvironmentCheck
    ###################################
    def pilotEnvironmentCheck ( self ):
        """ 
        __pilotEnvironmentCheck__ 
        """
        envList = ["VO_CMS_SW_DIR", "HOME"]
        notSetEnv = []
        for env in envList:
            if ( not isVariableSet ( env ) ): 
                notSetEnv.append(env)
        #if notSetEnv list is not empty
        if ( len( notSetEnv ) > 0 ):
            print "Some of env variables are not set"
            print "Env Not Found :%s" % notSetEnv 
            return False

        return True 
    
    ###########################
    # registerPilot
    ###########################
    def registerPilot(self):
        """
	__registerPilot__
        """
        #use plugin to register this pilot with PA
        jsonResult = self.commPlugin.register()
        if ( jsonResult == 'NoData' or jsonResult == 'ConnectionError'):
            #exit and 
            return
        if ( jsonResult['msg']['msgType'] == 'RegisterResponse' ):
            self.pilotId = jsonResult['msg']['payload']['pilotId']
            print 'pilot gets register successfully wid id %s' % self.pilotId 
    
    #############################################    
    # realTaskExecutionScript
    #############################################
    def realTaskExecutionScript(self, taskDir, sandboxUrl, specUrl):
        """
	__realTaskExecutionScript__
        """
        print "taskDir %s " % taskDir
        tarName = 'NoTarName.tar.gz'
        jobspecFile = 'NoSpecName'
        tarNameWOExt = 'NoTarName'
        print tarName
        print jobspecFile

        rind = sandboxUrl.rfind('/')
        if ( rind != -1 ):
            tarName = sandboxUrl[rind+1:]
            #TODO: extract it from the taskqueue information 
            rind = tarName.find('-Processing.')
            tarNameWOExt = tarName[0:rind]

        jind = specUrl.rfind('/')
        if ( jind != -1 ):
            jobspecFile = specUrl[jind+1:]

        print ('tarName %s'% tarName )
        print ('tarNameWOExt %s' % tarNameWOExt)
        print ('jobspecfile %s' %jobspecFile)   
        fwReportFile = 'FrameworkJobReport.xml'

        scriptlines = '#!/usr/bin/bash \n'
        scriptlines += '#for the testing on 32bit machine \n'
        scriptlines += 'source /afs/cern.ch/cms/sw/cmsset_default.sh \n'
        scriptlines += 'PILOT_DIR=`pwd` \n'
        scriptlines += 'JOB_SPEC_FILE="%s" \n' % jobspecFile
        scriptlines += 'wget %s \n' % specUrl
        scriptlines += 'wget %s \n' % sandboxUrl
        scriptlines += 'tar -zxf $PILOT_DIR/%s > /dev/null 2>&1 \n' % tarName
        scriptlines += 'cd %s \n' % tarNameWOExt
        scriptlines += 'ls $PILOT_DIR/$JOB_SPEC_FILE \n'
        scriptlines += '( /usr/bin/time ./run.sh $PILOT_DIR/$JOB_SPEC_FILE 2>&1'
        scriptlines += ' ) | gzip > ./run.log.gz\n'
        #scriptlines += 'ls \n'
        #scriptlines += 'rfcp ./run.log.gz %s:%s/run.log.gz \n' % \
        #               ('vocms13.cern.ch','/data/khawar')
        #scriptlines += 'rfcp ./%s %s:%s/%s\n' % (fwReportFile, \
        #               'vocms13.cern.ch', '/data/khawar', fwReportFile )
         
        #print scriptlines
        result = [scriptlines, tarName, tarNameWOExt]
        return result
    
    # save the script in the filename
    def save(self, filename, script ):
        """ 
	__save__ 

	save pilot job executable script
	"""
        try:
            handle = open(filename, 'w')
            handle.write(script)
            handle.close()
        except IOError, ioinst:
            print 'save():problem in saving : %s, %s' % \
                  (sys.exc_info()[0], sys.exc_info()[1])
            print str(ioinst)
            raise ioinst

    #################################
    #main entry point for the pilot
    #################################
    def startPilot(self):
        """
        __startPilot__ 
        
        start the pilot job
        """

        #test the environment settings
        self.pilotEnvironmentCheck()

        #register pilot and get id from TaskQueue
        #self.registerPilot()

        #first recover the cache    
        self.dataCacheRecovery()

        #recover old jobs if possible
        oldJobs = self.jobRecovery()

        if ( oldJobs ):
            print ( 'Pilot First process old jobs, if possible' )
        
        #stopPilot = False
        stopRequest = False 
        badRequestCount = 0
        emptyRequestCount = 0
        prMsg = ''
        #get job from task queue
        while ( not stopRequest ):

     	    #logging.debug("requesting for job")
            print ("Requesting for job")

            #generate request
            jsonResult = self.commPlugin.requestJob(self.cacheFiles)
            print ( jsonResult )

            if ( not jsonResult ):
                print ("Got Empty Result")
                break

            if ( jsonResult == 'ConnectionError' or jsonResult == 'NoData' ):
                badRequestCount = badRequestCount + 1
                if ( badRequestCount == 4 ): 
                    stopRequest = True
                    break 
	  	#continue if policy says to try again
                continue
                #otherwise break the loop
                #break
		    
            elif ( jsonResult["msg"]["msgType"] == 'Error' ):
                prMsg = "Error from TaskQueue\n"
                prMsg += "Error %s due to %s" % (jsonResult["msg"]["msgType"], \
                jsonResult["msg"]["payload"]["Error"])
                print prMsg
                #now wait for some time
                time.sleep(20)
                #break
                continue;
		
            elif (jsonResult["msg"]["msgType"] == 'NoTaskAvailable'):
                print('No Task Found in the TaskQueue\n waiting')
                
                emptyRequestCount = emptyRequestCount + 1
                if ( emptyRequestCount == 4):
                    prMsg = 'Pilot job tried 4 times but failed.\n'
                    print ('%s shutdown the pilot' % prMsg )
                    stopRequest = True
                    #limit has reached so end this loop
                    break
                    #continue
                
                print ('Pilot ll generate %s request ' % \
                      (emptyRequestCount+1) )
                #sleep for a while and re-generate the request    
                time.sleep( 30 )
                continue
		
	        	
            #process the successful response from taskqueue
            jobinfo = jsonResult["msg"]["payload"]

            if ( jobinfo != None ):
                print ('Pilot %s:%s:%s:%s' % (jobinfo, jobinfo['TaskId'], \
                       jobinfo['SandboxUrl'], jobinfo['SpecUrl'] ))

                #process job
                jobProcResult = self.processJob ( jobinfo['TaskId'], \
                                jobinfo['SandboxUrl'], jobinfo["SpecUrl"] )
                print jobProcResult     
                #if job successfully done
                if ( jobProcResult[0] == 'jobdone' ):
                    #notify taskqueue
                    resp = self.commPlugin.informJobEnd ( jobinfo['TaskId'] )
		    #verify the response  
                    if ( resp['msg']['payload']['ReportUrl'] ):

                        #TODO: its not working properly so hard code its value
                        reportUrl = resp['msg']['payload']['ReportUrl']
                        print 'reportUrl from tq: %s' % reportUrl

                        #reportUrl = reportUrl[reportUrl.find('/upload'):]
                        reportUrl = '/upload/reports/FrameworkJobReport.xml_%s' % \
                                    jobinfo['TaskId']

                        print 'reportUrl %s' % reportUrl 
                        self.commPlugin.uploadFile(\
                         './%s/FrameworkJobReport.xml' % jobProcResult[2], reportUrl)
                #if job gets failed 
                elif ( jobProcResult[0] == 'jobfail'):
                    print 'jobfail.send errorReport to TQ.'

            #sleep for a while before going to get the other job       
            time.sleep(10)
            #break

        #stop the communication module
        self.commPlugin.stopIt = True 

    #######################################    	    
    #gets the jobspec and process it    
    #######################################
    def processJob(self, taskId, sandboxUrl, specUrl):
        """ 
        __processJob__

        process the real job   
        """
        #create script for this real job
        script = self.realTaskExecutionScript(taskId, sandboxUrl, specUrl)
        try:
            print ('process job: need some other libraries')
             
            currentDir = os.getcwd()
            print 'currentDir: %s' % currentDir

            #scriptfile path on execution machine 
            realScriptFile = '%s/%s_real.sh' % (currentDir, taskId)
            print realScriptFile
            
            #save this script file 
            self.save(realScriptFile, script[0])
            
            #now execute this file    
            result = self.executeCommand ( 'sh +x %s' % realScriptFile )

            #jobdone msg, command result, job sandbox directory
            processResult = [ 'jobdone', result, script[2] ]

            #print processResult
            print (">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>")

        except IOError, ioinst:
            print ("ERROR IN workOnJob %s" % sys.exc_info()[0] )
            traceback.print_exc(file=sys.stdout)
            processResult = [ 'jobfail', str(ioinst), script[2] ]
        except RuntimeError, rtinst:
            print 'RuntimeError %s' % sys.exc_info()[0]
            traceback.print_exc(file=sys.stdout)
            processResult = ['jobfail', str(rtinst), script[2] ]

        return processResult 

    ####################
    #shutdown  	    
    ####################
    def shutdown(self):
        """ 
        __shutdown__ 
        """
        #stop or kill any process started by the pilot
        self.commPlugin.stopIt = True
    
    ##############################     
    #execute the script
    ##############################
    def executeCommand(self, command):
        """
	_executeCommand_

	Util it execute the command provided in a popen object

        """
        print 'executeCommand'
        #logging.debug("SubmitterInterface.executeCommand:%s" % command)
        # capture stdout and stderr from command
        child = popen2.Popen3(command, 1) 
        # don't need to talk to child
        child.tochild.close()     
        outfile = child.fromchild
        outfd = outfile.fileno()
        errfile = child.childerr
        errfd = errfile.fileno()
        makeNonBlocking(outfd)
        makeNonBlocking(errfd)
        outdata = errdata = ''
        outeof = erreof = 0
        stdoutBuffer = ""
        stderrBuffer = ""
        while 1:
            ready = select.select([outfd, errfd], [], []) # wait for input
            if outfd in ready[0]:
                outchunk = outfile.read()
                if outchunk == '': 
                    outeof = 1
                stdoutBuffer += outchunk
                sys.stdout.write(outchunk)
            if errfd in ready[0]:
                errchunk = errfile.read()
                if errchunk == '': 
                    erreof = 1
                stderrBuffer += errchunk
                sys.stderr.write(errchunk)
            if outeof and erreof: 
                break
            select.select([], [], [], .1) # give a little time for buffers to fill

        try:
            exitCode = child.poll()
        except Exception, ex:
            msg = "Error retrieving child exit code: %s\n" % ex
            msg += "while executing command:\n"
            msg += command
            msg += "\n"
            print("PilotJob:Failed to Execute Command")
            print(msg)
            raise RuntimeError, msg

        if exitCode:
            msg = "Error executing command:\n"
            msg += command
            msg += "\n"
            msg += "Exited with code: %s\n" % exitCode
            msg += "Returned stderr: %s\n" % stderrBuffer
            print("PilotJob:Failed to Execute Command")
            print(msg)
            raise RuntimeError, msg
 
        return  stdoutBuffer 


