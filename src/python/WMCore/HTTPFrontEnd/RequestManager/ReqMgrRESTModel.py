from WMCore.WebTools.RESTModel import RESTModel
from WMCore.WMSpec.WMWorkload import WMWorkloadHelper
import WMCore.RequestManager.RequestDB.Interface.User.Registration as Registration
import WMCore.RequestManager.RequestDB.Interface.User.Requests as UserRequests
import WMCore.RequestManager.RequestDB.Interface.Request.ListRequests as ListRequests
import WMCore.RequestManager.RequestDB.Interface.Request.GetRequest as GetRequest
import WMCore.RequestManager.RequestDB.Interface.Admin.RequestManagement as RequestAdmin
import WMCore.RequestManager.RequestDB.Interface.Admin.ProdManagement as ProdManagement
import WMCore.RequestManager.RequestDB.Interface.Admin.GroupManagement as GroupManagement
import WMCore.RequestManager.RequestDB.Interface.Admin.UserManagement as UserManagement
import WMCore.RequestManager.RequestDB.Interface.ProdSystem.ProdMgrRetrieve as ProdMgrRetrieve
import WMCore.RequestManager.RequestDB.Interface.Admin.SoftwareManagement as SoftwareAdmin
import WMCore.RequestManager.RequestDB.Interface.Request.ChangeState as ChangeState
import WMCore.RequestManager.RequestMaker.CheckIn as CheckIn
import WMCore.RequestManager.RequestDB.Interface.Group.Information as GroupInfo
#import WMCore.RequestManager.RequestMaker.Processing.RecoRequest 
#import WMCore.RequestManager.RequestMaker.Processing.ReRecoRequest 
#import WMCore.RequestManager.RequestMaker.Processing.FileBasedRequest
from WMCore.RequestManager.RequestMaker.Registry import retrieveRequestMaker
from WMCore.HTTPFrontEnd.RequestManager.ReqMgrWebTools import saveWorkload, removePasswordFromUrl, changePriority, changeStatus
import WMCore.Lexicon
import WMCore.Services.WorkQueue.WorkQueue as WorkQueue
import cherrypy
import json
import threading
import WMCore.Wrappers.JsonWrapper as JsonWrapper
import urllib
import logging

from WMCore.HTTPFrontEnd.RequestManager.ExternalMethods.Overview \
     import getGlobalSummaryView
from WMCore.HTTPFrontEnd.RequestManager.ExternalMethods.ResourceMonitor \
     import getResourceOverview
from WMCore.HTTPFrontEnd.RequestManager.ExternalMethods.AgentMonitor \
     import getAgentOverview
from WMCore.HTTPFrontEnd.RequestManager.ExternalMethods.SiteMonitor \
     import getSiteOverview

class ReqMgrRESTModel(RESTModel):
    """ The REST interface to the ReqMgr database.  Documentation may
    be found at https://twiki.cern.ch/twiki/bin/viewauth/CMS/ReqMgrSystemDesign """
    def __init__(self, config):
        RESTModel.__init__(self, config)
        #self.dialect = config.dialect
        self.hostAddress = config.model.reqMgrHost
        self.couchUrl = config.model.couchUrl
        self.workloadCouchDB = config.model.workloadCouchDB 

        self.addMethod('GET', 'request', self.getRequest, 
                       args = ['requestName'],
                       validation=[self.isalnum], expires = 0)
        self.addMethod('GET', 'assignment', self.getAssignment,
                       args = ['teamName', 'request'],
                       validation = [self.isalnum], expires = 0)
        self.addMethod('GET', 'user', self.getUser,
                       args = ['userName'], 
                       validation = [self.isalnum], expires = 0)
        self.addMethod('GET', 'group', self.getGroup,
                       args = ['group', 'user'], expires = 0)
        self.addMethod('GET', 'version', self.getVersion, args = [], expires = 0)
        self.addMethod('GET', 'team', self.getTeam, args = [], expires = 0)
        self.addMethod('GET', 'workQueue', self.getWorkQueue,
                       args = ['request', 'workQueue'], 
                       validation = [self.isalnum], expires = 0)
        self.addMethod('GET', 'message', self.getMessage,
                       args = ['request'], 
                       validation = [self.isalnum], expires = 0)

        self.addMethod('PUT', 'request', self.putRequest,
                       args = ['requestName', 'status', 'priority'],
                       validation = [self.isalnum, self.intpriority])
        self.addMethod('PUT', 'assignment', self.putAssignment,
                       args = ['team', 'requestName'],
                       validation = [self.isalnum])
        self.addMethod('PUT', 'user', self.putUser,
                       args = ['userName', 'email', 'dnName'],
                       validation = [self.validateUser])
        self.addMethod('PUT', 'group', self.putGroup,
                       args = ['group', 'user'],
                       validation = [self.isalnum])
        self.addMethod('PUT', 'version', self.putVersion,
                       args = ['version'],
                       validation = [self.validateVersion])
        self.addMethod('PUT', 'team', self.putTeam,
                       args = ['team'],
                       validation = [self.isalnum])
        self.addMethod('PUT', 'workQueue', self.putWorkQueue, 
                       args = ['request', 'url'],
                       validation = [self.validatePutWorkQueue])
        self.addMethod('PUT', 'message', self.putMessage,
                       args = ['request'],
                       validation = [self.isalnum])

        self.addMethod('POST', 'request', self.postRequest,
                        args = ['requestName', 'events_written', 
                                'events_merged', 'files_written',
                                'files_merged', 'percent_written', 
                                'percent_success', 'dataset'],
                                 validation = [self.validateUpdates])
        self.addMethod('POST', 'user', self.postUser,
                          args = ['user', 'priority'],
                          validation = [self.isalnum, self.intpriority])
        self.addMethod('POST',  'group', self.postGroup,
                          args = ['group', 'priority'],
                          validation = [self.isalnum, self.intpriority])

        self.addMethod('DELETE', 'request', self.deleteRequest,
                          args = ['requestName'],
                          validation = [self.isalnum])
        self.addMethod('DELETE', 'user', self.deleteUser,
                          args = ['user'],
                          validation = [self.isalnum])
        self.addMethod('DELETE', 'group', self.deleteGroup,
                          args = ['group', 'user'],
                          validation = [self.isalnum])
        self.addMethod('DELETE', 'version', self.deleteVersion,
                          args = ['version'],
                          validation = [self.validateVersion])
        self.addMethod('DELETE', 'team', self.deleteTeam,
                          args = ['team'],
                          validation = [self.isalnum])
        # stop caching for all GET', PUT, POST, and DELETEs
        #for call in ['PUT', 'POST', 'DELETE']:
        #   for method, paramDict in self.methods[call].iteritems():
        #       paramDict['expires'] = 0

        self.addMethod("GET", "overview", getGlobalSummaryView) #expires=16000
        self.addMethod("GET", "resourceInfo", getResourceOverview)
        self.addMethod("GET", "agentoverview", getAgentOverview,
                       args = ['detail'])
        self.addMethod("GET", "siteoverview", getSiteOverview)
        
        cherrypy.engine.subscribe('start_thread', self.initThread)
    
    def initThread(self, thread_index):
        """ The ReqMgr expects the DBI to be contained in the Thread  """
        myThread = threading.currentThread()
        #myThread = cherrypy.thread_data
        # Get it from the DBFormatter superclass
        myThread.dbi = self.dbi

    def isalnum(self, index={}):
        """ Validates that all input is alphanumeric, 
            with spaces and underscores tolerated"""
        for k, v in index.iteritems():
            WMCore.Lexicon.identifier(v)
        return index

    def intpriority(self, index={}):
        """ Casts priority to an integer """
        if index.has_key('priority'):
            index['priority'] = int(index['priority'])
        return index 
    
    def validateUser(self, index={}):
        assert index['userName'].isalnum()
        assert '@' in index['email']
        assert index['email'].replace('@','').replace('.','').isalnum()
        if 'dnName' in index:
            assert index['dnName'].replace(' ','').isalnum()
        return index

    def validateVersion(self, index={}):
        """ Make sure it's a legitimate CMSSW version format """
        WMCore.Lexicon.cmsswversion(index['version'])
        return index

    def findRequest(self, requestName):
        """ Either returns the request object, or None """
        requests = ListRequests.listRequests()
        for request in requests:
            if request['RequestName'] == requestName:
                return request
        return None

    def requestID(self, requestName):
        """ Finds the ReqMgr database ID for a request """
        requests = ListRequests.listRequests()
        for request in requests:
            if request['RequestName'] == requestName:
                return request['RequestID']
        raise RuntimeError("No such request")

    def getRequest(self, requestName=None):
        """ If a request name is specified, return the details of the request. 
        Otherwise, return an overview of all requests """
        if requestName == None:
            return GetRequest.getAllRequestDetails()
        else:
            return GetRequest.getRequestDetails(requestName)

    def getAssignment(self, teamName=None, request=None):
        """ If a team name is passed in, get all assignments for that team.
        If a request is passed in, return a list of teams the request is assigned to """
        result = []
        #self.init.setLogging()
        #self.init.setDatabaseConnection()
        # better to use ReqMgr/RequestDB/Interface/ProdSystem/ProdMgrRetrieve?
        #requestIDs = ProdMgrRetrieve.findAssignedRequests(teamName)
        # Maybe now assigned to team is same as assigned to ProdMgr
        if teamName != None:
            requestIDs = ListRequests.listRequestsByTeam(teamName, "assigned").values()
            result = {}
            for reqID in requestIDs:
                req = GetRequest.getRequest(reqID)
                result[req['RequestName']] = req['RequestWorkflow']
            return result
        if request != None:
            result = GetRequest.getAssignmentsByName(request)
        return result


    def getUser(self, userName=None, group=None):
        """ No args returns a list of all users.  Group returns groups this user is in.  Username
            returs a JSON with information about the user """
        if userName != None:
            result = {}
            result['groups'] = GroupInfo.groupsForUser(userName).keys()
            result['requests'] = UserRequests.listRequests(userName).keys()
            result['priority'] = UserManagement.getPriority(userName)
            return json.dumps(result)
        elif group != None:
            GroupInfo.usersInGroup(group)    
        else:
            return Registration.listUsers()

        

    def getGroup(self, group=None, user=None):
        """ No args lists all groups, one args returns JSON with users and priority """
        if group != None:
            result = {}
            result['users'] =  GroupInfo.usersInGroup(group)
            result['priority'] = GroupManagement.getPriority(group)
            return json.dumps(result)
        elif user != None:   
            return GroupInfo.groupsForUser(user).keys()
        else:
            return GroupInfo.listGroups()

    def getVersion(self):
        """ Returns a list of all CMSSW versions registered with ReqMgr """
        return SoftwareAdmin.listSoftware().keys()
      
    def getTeam(self):
        """ Returns a list of all teams registered with ReqMgr """
        return ProdManagement.listTeams()

    def getWorkQueue(self, request=None, workQueue=None):
        """ If a request is passed in, return the URl of the workqueue.
        If a workqueue is passed in, return all requests acquired by it """
        if workQueue != None:
            return ProdMgrRetrieve.findAssignedRequests(workQueue)
        if request != None:
            return ProdManagement.getProdMgr(request)

    def getMessage(self, request):
        """ Returns a list of messages attached to this request """
        return ChangeState.getMessages(request)

    def putWorkQueue(self, request, url):
        """ Registers the request as "acquired" by the workqueue with the given URL """
        changeStatus(request, "acquired")
        return ProdManagement.associateProdMgr(request, urllib.unquote(url))

    def validatePutWorkQueue(self, index={}):
        assert index['request'].replace('_','').isalnum()
        assert index['url'].startswith('http')
        return index

    def putRequest(self, requestName, status=None, priority=None):
        """ Checks the request n the body with one arg, and changes the status with kwargs """
        result = ""
        request = self.findRequest(requestName)
        if request == None:
            request = self.makeRequest()
        # see if status & priority need to be upgraded
        if status != None:
            changeStatus(requestName, status)
        if priority != None:
            changePriority(requestName, priority) 
        return result

    def makeRequest(self):
        """ Creates a new request, with a JSON-encoded schema that is sent in the
        body of the request """
        body = cherrypy.request.body.read()
        requestSchema = JsonWrapper.loads( body, encoding='latin-1' )
        logging.info(requestSchema)
        maker = retrieveRequestMaker(requestSchema['RequestType'])
        specificSchema = maker.schemaClass()
        specificSchema.update(requestSchema)
        url = cherrypy.url()
        # we only want the first part, before /request/
        url = url[0:url.find('/request')]
        specificSchema.reqMgrURL = url
        specificSchema.validate()

        request = maker(specificSchema)
        helper = WMWorkloadHelper(request['WorkflowSpec'])
        # can't save Request object directly, because it makes it hard to retrieve the _rev
        metadata = {}
        metadata.update(request)
        # don't want to JSONify the whole workflow
        del metadata['WorkflowSpec']
        workloadUrl = helper.saveCouch(self.couchUrl, self.workloadCouchDB, metadata=metadata)
        request['RequestWorkflow'] = removePasswordFromUrl(workloadUrl)
        CheckIn.checkIn(request)
        return requestSchema

    def putAssignment(self, team, requestName):
        """ Assigns this request to this team """
        # see if it's already assigned
        requestNamesAndIDs = ListRequests.listRequestsByTeam(urllib.unquote(team))
        if requestName in requestNamesAndIDs.keys():
            raise cherrypy.HTTPError(400,"Already assigned to this team")
        return ChangeState.assignRequest(requestName, urllib.unquote(team))

    def putUser(self, userName, email, dnName=None):
        """ Needs to be passed an e-mail address, maybe dnName """
        if Registration.isRegistered(userName):
            return "User already exists"
        result = Registration.registerUser(userName, email, dnName)
        return result

    def putGroup(self, group, user=None):
        """ Creates a group, or if a user is passed, adds that user to the group """
        if(user != None):
            # assume group exists and add user to it
            return GroupManagement.addUserToGroup(user, group)
        if GroupInfo.groupExists(group):
            return "Group already exists"
        return GroupManagement.addGroup(group)

    def putVersion(self, version):
        """ Registers a new CMSSW version with ReqMgr """
        return SoftwareAdmin.addSoftware(version)

    def putTeam(self, team):
        """ Registers a team with ReqMgr """
        return ProdManagement.addTeam(urllib.unquote(team))

    def putMessage(self, request):
        """ Attaches a message to this request """
        message = JsonWrapper.loads( cherrypy.request.body.read() )
        result = ChangeState.putMessage(request, message)
        return result

#    def postRequest(self, requestName, events_written=None, events_merged=None, 
#                    files_written=None, files_merged = None, dataset=None):
    def postRequest(self, requestName, **kwargs):
        """
        Add a progress update to the request Id provided, params can
        optionally contain:
        - *events_written* Int
        - *events_merged*  Int
        - *files_written*  Int
        - *files_merged*   int
        - *percent_success* float
        - *percent_complete float
        - *dataset*        string (dataset name)
        """
        return ChangeState.updateRequest(requestName, kwargs)

    def validateUpdates(self, index={}):
        for k in ['events_written', 'events_merged', 
                  'files_written', 'files_merged']:
            if k in index:
                index[k] = int(index[k])
        for k in ['percent_success', 'percent_complete']:
            if k in index:
                index[k] = float(index[k])
        return index

    def postUser(self, user, priority):
        """ Change the user's priority """
        return UserManagement.setPriority(user, priority)

    def postGroup(self, group, priority):
        """ Change the group's priority """
        return GroupManagement.setPriority(group, priority)

    def deleteRequest(self, requestName):
        """ Deletes a request from the ReqMgr """
        request = self.findRequest(requestName)
        if request == None:
            raise cherrypy.HTTPError(404, "No such request")
        return RequestAdmin.deleteRequest(request['RequestID'])

    def deleteUser(self, user):
        """ Deletes a user, as well as deleting his requests and removing
            him from all groups """
        if user in self.getUser():
            requests = json.loads(self.getUser(user))['requests']
            for request in requests:
                self.deleteRequest(request)
            for group in GroupInfo.groupsForUser(user).keys():
                GroupManagement.removeUserFromGroup(user, group)
            return UserManagement.deleteUser(user)

    def deleteGroup(self, group, user=None):
        """ If no user is sent, delete the group.  Otherwise, delete the user from the group """
        if user == None:
            return GroupManagement.deleteGroup(group)
        else:
            return GroupManagement.removeUserFromGroup(user, group) 

    def deleteVersion(self, version):
        """ Un-register this software version with ReqMgr """
        SoftwareAdmin.removeSoftware(version)

    def deleteTeam(self, team):
        """ Delete this team from ReqMgr """
        ProdManagement.removeTeam(urllib.unquote(team))


    

