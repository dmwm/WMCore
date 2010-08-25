#!/usr/bin/env python
"""
__NCommunication__

provides communication plugin for pilotjob

"""





import threading
import urllib2 
import urllib
from urllib import urlencode
import httplib
#deals with json
import simplejson
import time
import sys

# ANTO: don't send heartbeats so often!
HBEAT_TIME=600

class Communication:
    """
    __Communication__ 
    Communication between pilot and taskqueue 
    """ 
    def __init__(self, serverMode, pilotInstance):
        """ 
        __init__ 
        """ 
        #threading.Thread.__init__(self)    
        self.serverMode = serverMode
        #self.stopIt = False
        self.pilotInstance = pilotInstance
        self.taskQAddress = self.pilotInstance.taskQAddress
        self.commProtocol = 'http'

    ##########################################	
    # making an http request to the taskqueue 
    ##########################################
    def send(self, callurl):
        """ 
        __send__ 
        
        send request to taskqueue server 
        """
        result = 'NoData'
        try:
            f = urllib2.urlopen(callurl)
            result = f.read()
            #convert to json string
	    if ( result != None):
                result = simplejson.loads(result)
		
        except IOError, inst:
            result = "ConnectionError"
            print sys.exc_info()[0]
            print str(inst)

        return result

    ##################################
    # uploadFile
    ##################################
    def uploadFile (self, filename, uploadPath ):
        """
        __uploadfile__ 
        """
        try:
            http = httplib.HTTP(self.taskQAddress)
            userAgent = 'PilotJob'
            print 'reading file %s' % filename
            #reading file
            fileHandle = open(filename)
            data = fileHandle.read()
            fileHandle.close()
            print 'reading file is done'
            #uploadPath = '/upload/reports/FrameworkReport2.xml'
            # write header
            http.putrequest("PUT", uploadPath )
            http.putheader("User-Agent", userAgent )
            http.putheader("Host", self.pilotInstance.pilotHost)
            http.putheader("Content-Type", "application/octet-stream" )
            http.putheader("Content-Length", str( len(data) ) )
            http.endheaders()

            # write body
            http.send(data)

            # get response
            errcode, errmsg, headers = http.getreply()
            print errcode
            print errmsg
            print headers
            if ( errcode != 200 ):
                print ("Problem with uploading file")
                return False
            
        except Exception, inst:
            print sys.exc_info()[0], sys.exc_info()[1]
            print str(inst)
            return False

        #uploading done successfully
        return True
 
    ###############################
    # register the pilot
    ###############################
    def register(self, cacheDir, se, ttl, scram, cms_sw):
        """ 
        __register__

        register pilot with the taskqueue 
        """
        print 'register: %s,%s,%s'%(cacheDir, se, ttl)

        msg = {'se': se, 'ttl': ttl, \
               'cacheDir': cacheDir, 'host': self.pilotInstance.pilotHost, \
               'scram':scram, 'cmssw':cms_sw}
        print msg
        p = simplejson.dumps ( msg )
        fmsg = urlencode ( [ ('ARG', p) ] )

        url = '%s://%s/msg?msgType=registerRequest&payload=%s' % \
              (self.commProtocol, self.taskQAddress, fmsg[4:] )

        #print 'call this url %s to register pilot' % url

        #call TQ to register this Pilot
        return self.send ( url ) 

    ##############################################
    #inform taskqueue about the taskid which is 
    #completed successfully
    #############################################
    def informJobEnd(self, taskid, status):
        """ 
        __informJobEnd__
     
        inform the task queue that a  job has finished
 
        """
        print 'informJobEnd():'
        msg = {'pilotId':self.pilotInstance.pilotId, 'taskId': taskid, \
               'exitStatus': status}
        p = simplejson.dumps( msg )
        fmsg = urlencode( [ ('ARG', p)] )

        url = '%s://%s/msg?msgType=taskEnd&payload=%s' % \
              (self.commProtocol, self.taskQAddress, fmsg[4:])

        return self.send(url)

    ########################################	
    #request to TaskQueue to get actual job
    ########################################
    def requestJob(self, files, cms_arch, cmssw_info):
        """
        __requestJob__

        Request the Task Queue to get the actual job
        """
        print 'files %s would be used when datache is in place' % files
        msg = {'pilotId': self.pilotInstance.pilotId, \
               'host': self.pilotInstance.pilotHost, \
               'se': self.pilotInstance.pilotSite, \
               'swarch': cms_arch, 'cmssw': cmssw_info }
        print msg
        p = simplejson.dumps( msg )
        fmsg = urlencode( [ ('ARG', p)] )

        url = 'http://%s/msg?msgType=getTask&payload=%s' % \
              (self.taskQAddress, fmsg[4:])

        #print url
        return self.send(url)


    ######################
    #pilot shutdown
    ######################
    def pilotShutdown (self, reason):

# ANTO: Move shutdownPilot docstring here
        """ __pilotShutdown__

        This function is called when pilot dies gracefully.
        This will send shutdown msg to TQ just to inform that 
        this pilot is no longer available.
        """       
        msg = {'pilotId':self.pilotInstance.pilotId, \
               'reason':reason }
        print msg
        p = simplejson.dumps( msg )
        fmsg = urlencode( [ ('ARG', p)] )
        url = 'http://%s/msg?msgType=pilotShutdown&payload=%s' % \
              (self.taskQAddress, fmsg[4:])
        self.send(url) 
 
    ######################
    #send Error Report
    ######################
    def reportError (self, jobid, event, errCode, errMsg):
        msg = {'pilotId':self.pilotInstance.pilotId, \
               'jobid': jobid, 'errCode': errCode, \
               'event': event, 'errMsg': errMsg }
        print msg
        p = simplejson.dumps( msg )
        fmsg = urlencode( [ ('ARG', p)] )
        url = 'http://%s/msg?msgType=errorReport&payload=%s' % \
              (self.taskQAddress, fmsg[4:]) 

        return self.send(url)       

    ######################   
    #downloadSandboxFile
    ######################
    def downloadSanboxFile (self, address, filename=""):
        """ 
        __downloadSandboxFile__

        programmatically download the sandbox file 
        using python lib 
        """
        url = "http://%s/static/sandbox/%s" % (address, filename)
        specfilename = ""
        try:
            specfilename = urllib.urlretrieve(url, filename)
            print('specfile loading done successfully')
        except IOError, inst:
            print( 'JobSpecFileRetrieval Error: %s' % str(inst) )

        print("final specfilename information: %s" % specfilename[0])

        return specfilename

    #######################
    # getJobSpecFile
    #######################
    def getJobSpecFile(self, filename):
        """
        __getJobSpecFile__

        download the jobspec file used for real job execution 
        """

        url = "http://%s/static/spec/" % self.taskQAddress
        specfilename = ""
        try:
            specfilename = urllib.urlretrieve(url, filename)
        except IOError, inst:
            print str(inst)

        return specfilename
   
# ANTO: delete pilotShutdown, use shutdownPilot instead
#    ###########################
#    # Shutdown pilot
#    ###########################
#    def shutdownPilot( self ):
#        """ __shutdownPilot__

#	This function is called when pilot dies gracefully.
#	This function will send shutdown msg to TQ just to inform that 
#	this pilot is no longer available.
#        """       
#	msg = {'pilotid':self.pilotInstance.pilotId}
#        p = simplejson.dumps( msg )
#	fmsg = urlencode( [('ARG', p)] )
#	url = 'http://%s/msg?msgType=heartbeat&payload=%s' % \
#               (self.taskQAddress, fmsg[4:])
#        #call this url	
#        self.send ( url )	
	
    ######################	
    # sendHeartbearMsg	
    ######################
    def sendHeartbeatMsg(self):
        """ 
        __sendHeartbeatMsg__ 
        send alive message to task queue
        """
        msg = {'pilotId':self.pilotInstance.pilotId, \
               'ttl': self.pilotInstance.ttl }

        p = simplejson.dumps( msg )
        fmsg = urlencode( [ ('ARG', p)] )

        url = 'http://%s/msg?msgType=heartbeat&payload=%s' % \
               (self.taskQAddress, fmsg[4:])

        try:
            f = urllib2.urlopen(url)
            result = f.read()
            #print result
        except IOError, ioinst:
            print str(ioinst)
            result = "Error"
        return result

    # run    
#    def run(self):
#        """ 
#        _run_ 
#        """
#        # This time also configurable? Not really needed...
#        sleepTime = 30
#        hbeatTimer = 0
#        while ( not self.stopIt ):

            # First verify that pilot has got the ID
#            if ( self.pilotInstance.pilotId == None ):
#                time.sleep(5)
#                continue

            # Check heartbeat
 #           hbeatTimer += sleepTime
 #           if hbeatTimer >= HBEAT_TIME:
 #               self.sendHeartbeatMsg()
 #               hbeatTimer = 0

            # Go to sleep
 #           time.sleep(sleepTime)


