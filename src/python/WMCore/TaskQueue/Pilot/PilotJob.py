#!/usr/bin/env python

"""
_PilotComponent_



"""

__revision__ = "$Id: PilotJob.py,v 1.4 2009/09/16 12:37:43 khawar Exp $"
__version__ = "$Revision: 1.4 $"
__author__ = "Khawar.Ahmad@cern.ch"

import os
import sys
import traceback
import string
import time

import xml.dom.minidom
from xml import xpath
from string import expandtabs

import socket

#for logging
import logging

from NCommunication import Communication 
from Heartbeat import HeartBeat

import CommonUtil
from CommonUtil import CMSSW_INFO, CMS_ARCH, getCMSSWInfo, \
getScramInfo, removeJobDir , remove, getOutputName, \
 isVariableSet, executeCommand

#import urllib2 


def parseJR(fwReport):
    try:
        print fwReport
        lfnText = None
        pfnText = None
        guid = None
        doc = xml.dom.minidom.parse(fwReport)
        #get the output LFN
        fileNode = doc.getElementsByTagName('File')
        #print fileNode[0].toxml()
        print fileNode
        if ( fileNode is not None ):
            print len(fileNode) 
            print type(fileNode)
            fileNode = fileNode[0]
            lfnNode = fileNode.getElementsByTagName('LFN')
            print 'lfnNode %s' % lfnNode
            if ( lfnNode is not None):
                lfnText = getText(lfnNode[0].childNodes)
                #replace all tabs with 0 space
                lfnText = expandtabs(lfnText, 0)
                lfnText = lfnText.replace('\n','')  

            #pfnNode = xpath.Evaluate('descendant::File/PFN', doc.documentElement)
            pfnNode = fileNode.getElementsByTagName('PFN')
            print 'pfnNode %s ' % pfnNode
            if ( pfnNode is not None):
                pfnText = getText(pfnNode[0].childNodes)
                pfnText = expandtabs(pfnText, 0)
                pfnText = pfnText.replace('\n','') 

            #guidNode = xpath.Evaluate('descendant::File/GUID', doc.documentElement)
            guidNode = fileNode.getElementsByTagName('GUID')
            #print 'guidNode %s' % guidNode
            if ( guidNode is not None):
                guid = getText(guidNode[0].childNodes)
                guid = expandtabs(guid, 0)
                guid = guid.replace('\n','')

        #print 'LFN: %s' % lfnText
        #print 'PFN: %s' % pfnText
        #print 'GUID: %s' % guid

        return {'LFN':lfnText, 'PFN': pfnText, 'GUID': guid}
    except:
        print 'problem addToDataCache:%s, %s' % \
               (sys.exc_info()[0], sys.exc_info()[1])
        traceback.print_exc(file=sys.stdout)
        return {'Error':'Parse_Error'}


def parseJobStatus(fwReport):

    print 'reading %s to get job status' % fwReport
    jobStatus = 'Failed'
    try:
        doc = xml.dom.minidom.parse(fwReport)
        fwNode = doc.getElementsByTagName('FrameworkJobReport')
        if ( fwNode ):
            fwNode = fwNode[0]
            #get the attribute
            if fwNode.hasAttributes():
                jobStatus = fwNode.attributes["Status"].value

    except:
        jobStatus = 'NotKnown'
        print 'problem with parsing job report for job status'

    return jobStatus
 

def parseJobSpec(jobspec):
    try:
        doc = xml.dom.minidom.parse(jobspec)
	 
    except:
        print 'Error - %s:%s' % (sys.exc_info()[0], sys.exc_info()[1])
	return {'Error': 'Parse_Error'} 

def getText(nodelist):
    rc = ""
    for node in nodelist:
        if node.nodeType == node.TEXT_NODE:
            rc = rc + node.data
    return rc


##########################################
#create the symlinks for /afs/cern.ch/cms
#used for pilot cache lookup
##########################################
def setupSymlinks():
    symlink_script = "create_cmsconf_links.sh"
    print executeCommand("sh +x %s" % symlink_script)
    print "updated CMS_PATH: %s" % os.environ.get('CMS_PATH')
    os.putenv("CMS_PATH","%s/%s" % (os.getcwd(),"localConf"))
    print "updated CMS_PATH: %s" % os.environ.get('CMS_PATH')  

"""
Class represents PilotJob 
"""
PILOT_WAIT_JOB_POLL=120

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
        #self.processingRealJob = False

        #configurable
        #self.heartBeatMsg = True
        self.cacheFiles = []
        self.otherPilots = {}

        self.pilotId = None 
        self.ttl = config["TTL"]
        #get hostname of the pilot job
        self.pilotHost = socket.getfqdn()
        self.pilotSite = None
        self.pilotCacheDir = None
        self.pilotDir = os.getcwd()
        #Address of the TaskQueue machine		
        self.taskQAddress = config["tqaddress"] 

        #start communication module
        self.commPlugin = Communication(False, self)
	
	#heartbeat thread
	self.heartbeat = HeartBeat(self.commPlugin, self) 
        #self.heartbeat.start()	
	
        msg = "PilotJob Started:\n"
        print(msg)
   

    def getPilotSite(self):
        """ 
        __getPilotSite__ 
        get pilot site information using config file
        """ 
        print 'PilotJob: getPilotSite()' 
        if ( isVariableSet('CMS_PATH') ):
            configPath = os.path.join(os.environ.get('CMS_PATH'),\
                       'SITECONF/local/JobConfig/site-local-config.xml')
            try:
                doc = xml.dom.minidom.parse(configPath)
                sites = doc.getElementsByTagName('site')
                if ( sites != None):
                    site = sites[0]
                    siteName = site.getAttribute('name')
                    localStageOut = site.getElementsByTagName('local-stage-out')[0]
                    node = localStageOut.getElementsByTagName('se-name')[0]
                    self.pilotSite = node.getAttribute('value')
                    return True
                else:
                    print 'there is no site tag in site-local-config.xml'  
                    return False
            except:
                print 'getPilotSite():Problem %s:%s' \
                (sys.exc_info()[0], sys.exc_info()[1])
                return False
        else: 
            print 'could not find CMS_PATH variable' 
            return False

 

    ###################################
    #setPilotCacheDir
    ###################################     
    def setPilotCacheDir( self ):
        """ __setPilotCacheDir__ 

        creates cachearea for this pilot
        and set the pilot variable
        """
        print 'setPilotCacheDir'
        #pilotDir = os.getcwd()
        pilotCacheDir = "%s/%s" % ( self.pilotDir, "CACHE_AREA")
        try:
            os.mkdir(pilotCacheDir)
            self.pilotCacheDir = pilotCacheDir
            print 'cacheDir %s' % self.pilotCacheDir
            return True
        except:
            print 'Error setPilotCacheDir %s,%s'% \
                  (sys.exc_info()[0], sys.exc_info()[1])
            return False
       

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
        envList = ["CMS_PATH", "VO_CMS_SW_DIR", "HOME"]
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
        #add cmssw info with the registration request
        global CMSSW_INFO, CMS_ARCH

        #print CMSSW_INFO 
        if ( CMSSW_INFO is None ):
            return False

        #print 'CMSSW_INFO %s' % CMSSW_INFO
        #print 'SCRAM %s' % CMS_ARCH

        #use plugin to register this pilot with PA
        print 'going for pilot registration'
        jsonResult = self.commPlugin.register(self.pilotCacheDir, self.pilotSite, \
                     self.ttl, CMS_ARCH, CMSSW_INFO)

        print jsonResult
        
        if ( jsonResult == 'NoData' or jsonResult == 'ConnectionError'):
            #exit and 
            return
        if ( jsonResult['msg']['msgType'] == 'registerResponse' and \
             jsonResult['msg']['payload']['registerStatus'] == 'RegisterDone'):
            #print jsonResult
            self.pilotId = jsonResult['msg']['payload']['pilotId']
            self.otherPilots = jsonResult['msg']['payload']['otherPilots']

            print 'pilot gets register successfully wid id %s' % self.pilotId 
    
    #############################################    
    # realTaskExecutionScript
    #############################################
    def realTaskExecutionScript(self, taskDir, sandboxUrl, specUrl, logDir, jobWF):
        """
	__realTaskExecutionScript__
        """
        print "taskDir %s " % taskDir
        tarName = 'NoTarName.tar.gz'
        jobspecFile = 'NoSpecName'
        tarNameWOExt = 'NoTarName'
        #print tarName
        #print jobspecFile

        rind = sandboxUrl.rfind('/')
        if ( rind != -1 ):
            tarName = sandboxUrl[rind+1:]
            #TODO: extract it from the taskqueue information 
            rind = tarName.rfind('-%s'%jobWF)
            if ( rind < 0):
                rind = tarName.rfind("-");

            tarNameWOExt = tarName[0:rind]
            #tarNameWOExt = tarName

        jind = specUrl.rfind('/')
        if ( jind != -1 ):
            jobspecFile = specUrl[jind+1:]

        print ('tarName %s'% tarName )
        print ('tarNameWOExt %s' % tarNameWOExt)
        print ('jobspecfile %s' %jobspecFile)   
        fwReportFile = 'FrameworkJobReport.xml'
        
        scriptlines = '#!/usr/bin/bash \n'
        scriptlines += '#for the testing on 32bit machine \n'
        #scriptlines += 'source /afs/cern.ch/cms/sw/cmsset_default.sh \n'
        scriptlines += 'PILOT_DIR="%s" \n' % self.pilotDir
        scriptlines += 'myDate=`date "+%G%m%d_%k%M%S"` \n'
        scriptlines += 'JOB_SPEC_FILE="%s/%s" \n' % (taskDir, jobspecFile)
        #scriptlines += 'JOBDIR="$PILOT_DIR/%s/%s" \n'%(tarName
        #create task directory
        scriptlines += 'cd $PILOT_DIR \n'
        scriptlines += 'mkdir %s \n' % taskDir
        scriptlines += 'cd %s \n' % taskDir 
        #generate the log collection area
        scriptlines += 'mkdir -p JobLogArea/%s \n' % logDir
        #download spec and sandbox
        scriptlines += 'wget %s \n' % specUrl
        scriptlines += 'wget %s \n' % sandboxUrl
        # untar the sandbox
          
        #scriptlines += 'tar -zxf $PILOT_DIR/%s/%s > /dev/null 2>&1 \n' % (taskDir, tarName)
        scriptlines += 'tar -zxf %s > /dev/null 2>&1 \n' % tarName
        scriptlines += 'cd %s \n' % tarNameWOExt
        scriptlines += 'ls -l $PILOT_DIR/$JOB_SPEC_FILE \n'
        scriptlines += 'echo "$PILOT_DIR/$JOB_SPEC_FILE" \n'
        scriptlines += 'echo "Running the actual job" \n'
        scriptlines += '( /usr/bin/time ./run.sh $PILOT_DIR/$JOB_SPEC_FILE 2>&1'
        scriptlines += ' ) | gzip > ./run.log.gz\n'
        #scriptlines += 'rfcp run.log.gz vocms13.cern.ch:/data/khawar/prototype/run.log.gz \n'
        scriptlines += ' cp run.log.gz ../JobLogArea/%s \n' % logDir
        #scriptlines += ' find . -name "FrameworkJobReport.xml"' 
        #scriptlines += ' find . -name "*root"' 
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


    ###################################
    # performs initial checks
    ###################################
    def initialChecks(self):
        """ __initialChecks__ 

        performs some preliminary checks before fetching a job
        
        """
        global CMSSW_INFO, CMS_ARCH

        #test the environment settings
        print 'Check1: Environment Variable check start \n'
        chk = self.pilotEnvironmentCheck()
        print 'Check1: result: %s \n\n' % chk

        #set pilot site info
        print 'Check2: PilotSite info start\n'
        chk = self.getPilotSite()
        print 'Check2: PilotSite info: %s \n\n' % chk 

        #get CMSSW and SCRAM info
        print 'Check3: CMSSW and Scram start\n'  
        cmsinfo = getCMSSWInfo()
        scraminfo = getScramInfo()   

        if ( cmsinfo.has_key('ERROR') ):
            print 'Error: %s' % cmsinfo['ERROR']
        else:
            cms_sw = cmsinfo['CMSSW']
            CMSSW_INFO = cmsinfo['CMSSW']

        if ( scraminfo.has_key('ERROR') ):
            print 'Error :%s' % cmsinfo['ERROR']
        else:
             scram = scraminfo['SCRAM_ARCH']
             CMS_ARCH = scram

        print 'Scram: %s' % CMS_ARCH
        print 'CMSSW: %s \n\n' % CMSSW_INFO

        #set pilot cache dir
        if ( not self.setPilotCacheDir() ):
            print 'Check4: Could not create CacheArea. So Stop it'
            return           

        print 'Check4: Cache Directory created successfully' 

    #################################
    #main entry point for the pilot
    #################################
    def startPilot(self):
        """
        __startPilot__ 
        
        start the pilot job
        """

        #initial checks
        self.initialChecks()
 
        #register pilot and get id from TaskQueue
        self.registerPilot()
        
        # if registeration process could not succeed
        # due to any reason. Try it once more 
        if ( self.pilotId == None ):
             self.registerPilot()
             #if it remains none: stop the pilot
             if ( self.pilotId == None): 
                 print 'PILOT COULD NOT REGISTER WITH TQ %s' %\
                        self.taskQAddress 
                 #send error msg to TaskQueue
                 return
		 
        #once registration is successful, now start the heartbeat thread
	self.heartbeat.start()
	
	   
        #first recover the cache    
        self.dataCacheRecovery()

        #recover old jobs if possible
        oldJobs = self.jobRecovery()

        if ( oldJobs ):
            print ( 'Pilot First process old jobs, if possible' )
        
        #stopPilot = False
        badRequestThreshold=self.config['badAttempts']
        emptyRequestThreshold=self.config['noTaskAttempts']
        print 'badRequestThreshold: %s' % badRequestThreshold 
        print 'emptyRequestThreshold: %s' % emptyRequestThreshold

        stopRequest = False 
        badRequestCount = 0
        emptyRequestCount = 0
        prMsg = ''
        shutReason = 'normal'
        #get job from task queue
        while ( not stopRequest ):

     	    #logging.debug("requesting for job")
            print ("Requesting for job")

            #generate request
            jsonResult = self.commPlugin.requestJob(self.cacheFiles, \
                         CMS_ARCH, CMSSW_INFO)

            print ( jsonResult )

            if ( not jsonResult ):
                print ("Got Empty Result")
                break

            if ( jsonResult == 'ConnectionError' or jsonResult == 'NoData' ):
                badRequestCount = badRequestCount + 1

                if ( badRequestCount == int(badRequestThreshold) ): 
                    stopRequest = True
                    shutReason = jsonResult
                    time.sleep(60)
                    break 
	  	#continue till badRequestCount reaches value 4
                continue
                		    
            elif ( jsonResult["msg"]["msgType"] == 'Error' ):
                prMsg = "Error from TaskQueue\n"
                prMsg += "Error %s due to %s" % (jsonResult["msg"]["msgType"], \
                jsonResult["msg"]["payload"]["Error"])
                print prMsg
                #now wait for some time
                time.sleep(PILOT_WAIT_JOB_POLL)
                #break
                continue;
		
            elif (jsonResult["msg"]["msgType"] == 'NoTaskAvailable'):
                print('No Task Found in the TaskQueue\n waiting')
                
                emptyRequestCount = emptyRequestCount + 1
                if ( emptyRequestCount == int(emptyRequestThreshold) ):
                    prMsg = 'Pilot job tried %s times but failed.\n' % emptyRequestCount
                    print ('%s shutdown the pilot' % prMsg )
                    stopRequest = True
                    shutReason = 'noTask'
		    
                    #limit has reached so end this loop
                    break
                    #continue
                
                print ('Pilot will generate %s request ' % \
                      (emptyRequestCount+1) )
                #sleep for a while and re-generate the request    
                time.sleep( PILOT_WAIT_JOB_POLL )
                continue
		
	        	
            #process the successful response from taskqueue
            jobinfo = jsonResult["msg"]["payload"]

            if ( jobinfo != None ):
	        # reset counters 
                emptyRequestCount = 0
                badRequestCount = 0  
                
                print ('Pilot %s:%s:%s:%s' % (jobinfo, jobinfo['taskId'], \
                       jobinfo['sandboxUrl'], jobinfo['specUrl'] ))

                jobWF = jobinfo['workflowType']
                print jobWF
        
                #process job
                jobProcResult = self.processJob ( jobinfo['taskId'], \
                                jobinfo['sandboxUrl'], jobinfo["specUrl"], jobWF )
#                print jobProcResult     

                jobReportPath = '%s/%s/%s/FrameworkJobReport.xml'% \
                                    (self.pilotDir, jobinfo['taskId'], jobProcResult[2])

                jobReportUrl = jobinfo['reportUrl']
		print jobReportUrl
                jobStatus = 'Failed'
                if ( os.path.exists ( jobReportPath ) ):
                    jobStatus =  parseJobStatus ( jobReportPath )
                    print 'jobStatus from XML report: %s' % jobStatus  

                #if job successfully done
                if ( jobProcResult[0] == 'jobdone' ):

                    #construct the logurl
                    logUrl = jobReportUrl[:jobReportUrl.rfind("/")]; 
                    logUrl = "%s/%s.tar.gz" % ( logUrl, jobProcResult[3])
                    logTarStatus = self.collectJobLogs(jobinfo['taskId'], \
                                   jobProcResult[3], logUrl)

                    if ( not os.path.exists(jobReportPath) ):
                        print 'Could not find the jobreport at %s. so shutdown pilot' % \
                               jobReportPath
                        #send error msg
                        self.reportError(jobinfo['taskId'], 'uploadJFR', \
                                         102, 'FJR not found')
                        stopRequest = True
                        shutReason='NoFJR'
                        continue

                    #now upload the JR. if it is succesfull then say 'taskend'    
                    if ( jobReportUrl is not None ):

                        print 'reportUrl from tq: %s' % jobReportUrl
                        reportUrl = jobReportUrl[jobReportUrl.find('/upload'):]

                        print 'reportUrl %s' % reportUrl 
                        print jobProcResult

                        #first upload the JR. if it is
                        #succesfull then say 'taskend'    
                        uploadStatus = self.commPlugin.uploadFile(\
                                       '%s' % jobReportPath, reportUrl)
                        print 'JFR upload status %s' % uploadStatus
                        #if ( uploadStatus == True ):

                        if ( jobStatus == 'Success' ):
                            #now inform that the job is done
                            resp = self.commPlugin.informJobEnd ( jobinfo['taskId'], 'Done' )
                            print 'informJobEnd response: %s' % resp  

                        elif ( jobStatus == 'Failed' ):
                            print 'sending job status fail msg'  
                            resp = self.commPlugin.informJobEnd ( jobinfo['taskId'], 'Failed')
 

                        #self.addToDataCache(jobinfo['taskId'], jobProcResult[2], jobReportPath)
                        #once it is done. remove the job directory 
                        removeJobDir("%s/%s" % (self.pilotDir, jobinfo['taskId'] ) )
                        print 'Job Dir is removed successfully'
 
                #if job gets failed
                #get empty report if possible 
                elif ( jobProcResult[0] == 'jobfail'):
                    print 'jobfail.send errorReport to TQ.'
                    self.commPlugin.informJobEnd(jobinfo['taskId'], 'Failed')

            #sleep for a while before going to get the other job       
            time.sleep(10)
            #break

        #shutdown this pilot
        self.shutdown(shutReason)
    
   
    #######################################    	    
    #gets the jobspec and process it    
    #######################################
    def processJob(self, taskId, sandboxUrl, specUrl, jobWF):
        """ 
        __processJob__

        process the real job   
        """
        #create logDir name
        logDate = time.strftime('%Y%m%d_%H%M%S') 
        #create script for this real job
        script = self.realTaskExecutionScript(taskId, sandboxUrl, \
                specUrl, logDate, jobWF)
        try:
            #print ('process job: need some other libraries')
             
            #currentDir = os.getcwd()
            currentDir = self.pilotDir  
            print 'currentDir: %s' % currentDir

            #scriptfile path on execution machine 
            realScriptFile = '%s/%s_real.sh' % (currentDir, taskId)
            print realScriptFile
            
            #save this script file 
            self.save(realScriptFile, script[0])
            
            #now execute this file    
            result = executeCommand ( 'sh +x %s' % realScriptFile )

            #jobdone msg, command result, job sandbox directory, logDate
            processResult = [ 'jobdone', result, script[2], logDate ]

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
        except:
            print 'Some unknown error occured %s'%sys.exc_info()[0]  
            traceback.print_exc(file=sys.stdout)
            processResult = ['jobfail', str(sys.exc_info()), script[2] ]

        return processResult 

    #######################################
    # collect logs that job produces
    #######################################
    def collectJobLogs(self, jobid, logDir, uploadURL):
        """ 
        __collectJobLogs__ 
        """
        #creates the log collection area
        print 'collectJobLogs(): for job %s n logDir %s' %(jobid, logDir)
        jobDir = os.path.join(os.getcwd(), jobid)
        logArea = os.path.join(os.getcwd(), \
                  "%s/JobLogArea" % (jobDir) )
        print 'logArea: %s' % logArea 
        runlog = None
        if ( os.path.exists(logArea) ):
            runlog = os.path.join(logArea, '%s/run.log.gz'%logDir)
            print 'runlog: %s' % runlog

        if ( os.path.exists(runlog) ):
            #create tar of this log
            tarName = "%s/%s.tar.gz" % (logArea, logDir)
            tarCmd = "tar -zcf %s -C %s %s " %(tarName, logArea, logDir)
            os.system(tarCmd) 
            print tarCmd
            tarPath = os.path.join(jobDir, tarName)
            reportUrl = uploadURL[uploadURL.find('/upload'):]
            print 'logUpload Url: %s' % reportUrl 
            #tarPath = os.path.join(jobDir, tarName)
            tarUpStatus = self.commPlugin.uploadFile(tarName, reportUrl) 
            print 'tarUploadStatus: %s' % tarUpStatus
            return True

        return False 

    ################################################
    # addToDataCache
    ################################################
    def addToDataCache(self, taskDir, untarDir, fwReport):
        """ 
        __addToDataCache__ 
        """
        #first get the output name LFN from fwReport
        #then look for that file and get its size
        #apply caching algorithm 
        #remove files if required
        #create hardlink of that file into CACHE_AREA  
        print 'addToDataCache()'
        #CMSRun should be chagned with some dynamic value
        outputDir = '%s/%s/%s/%s' % (self.pilotDir, taskDir, untarDir, 'cmsRun1')
        print outputDir 
        outputFile = getOutputName(outputDir)
        if ( outputFile is not None ): 

            print 'got output file: %s' % outputFile 
            outputFileLocation = "%s/%s"% (outputDir, outputFile)
            fwjResult = parseJR(fwReport)
            print fwjResult 
            #for test purpose
            #ask if the file names are going to repeat
            #if yes then this soluation is okay. this solution is corect coz we need to send lfn 
            #becoz job dependent on lfn not to filename only
            #otherwise it ll put additional computational time
            #fwjResult['LFN']='/store/data/unmerged/2009/6/16/RECO/V2/TEST.root'
 
            #apply cache replacement algo
            #code ll come here

            try: 
                rind = string.rfind(fwjResult['LFN'], "/")
                dirPath = fwjResult['LFN'][1:rind]
                HDLocation = '%s/%s/%s' % (self.pilotCacheDir, dirPath, outputFile) 
                print HDLocation
            
                os.makedirs( "%s/%s" % (self.pilotCacheDir, dirPath) )
                print 'LFN directories created' 
                #now create hard link
                print outputFileLocation
                print HDLocation 
                os.link(outputFileLocation, HDLocation)
                #add this to the data cache util 
                print 'Hard Link created'
            except:
                print 'Error: %s'% sys.exc_info()[0]
                print 'Error: %s' % sys.exc_info()[1]    
 
    ####################
    #shutdown  	    
    ####################
    def shutdown(self, reason):
        """ 
        __shutdown__ 
        """
        #stop or kill any process started by the pilot
        print 'shutdwon this pilot' 
        self.heartbeat.stopIt = True
        self.commPlugin.pilotShutdown(reason)


    def reportError(self, taskId, event, errorCode, errorMsg ):
        """ 
        _sendErrorMsg__ 
        """      
        self.commPlugin.reportError(taskId, event, errorCode, errorMsg)

