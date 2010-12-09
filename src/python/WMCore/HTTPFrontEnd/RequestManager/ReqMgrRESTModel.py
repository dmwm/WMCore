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
import WMCore.RequestManager.RequestDB.Settings.RequestStatus as RequestStatus
import WMCore.RequestManager.RequestMaker.WMWorkloadCache as WMWorkloadCache
import WMCore.RequestManager.RequestMaker.CheckIn as CheckIn
import WMCore.RequestManager.RequestDB.Interface.Group.Information as GroupInfo
import WMCore.RequestManager.RequestMaker.Processing.RecoRequest 
import WMCore.RequestManager.RequestMaker.Processing.ReRecoRequest 
import WMCore.RequestManager.RequestMaker.Processing.FileBasedRequest
from WMCore.RequestManager.RequestMaker.Registry import retrieveRequestMaker
from WMCore.HTTPFrontEnd.RequestManager.ReqMgrWebTools import saveWorkload, removePasswordFromUrl
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

class ReqMgrRESTModel(RESTModel):
    """ The REST interface to the ReqMgr database.  Documentation may
    be found at https://twiki.cern.ch/twiki/bin/viewauth/CMS/ReqMgrSystemDesign """
    def __init__(self, config):
        RESTModel.__init__(self, config)
        #self.dialect = config.dialect
        self.urlPrefix = '%s/download?filepath=' % config.model.reqMgrHost
        self.hostAddress = config.model.reqMgrHost
        self.couchUrl = config.model.couchUrl
        self.workloadCouchDB = config.model.workloadCouchDB 
        self.methods = {
            'GET':{'request' : {'call':self.getRequest, 'args':['requestName'], 'expires': 0},
                   'assignment' : {'call':self.getAssignment, 'args':['teamName', 'request'], 'expires': 0},
                   'user' :  {'call':self.getUser, 'args':['userName'], 'expires': 0},
                   'group' :  {'call':self.getGroup, 'args':['group', 'user'], 'expires': 0},
                   'version' :  {'call':self.getVersion, 'args':[], 'expires': 0},
                   'team' :  {'call':self.getTeam, 'args':[], 'expires': 0},
                   'workQueue' : {'call':self.getWorkQueue, 'args':['request', 'workQueue'], 'expires': 0},
                   'message' : {'call':self.getMessage, 'args':['request'], 'expires': 0}
                   },
            'PUT':{'request' : {'call':self.putRequest, 
                                'args':['requestName', 'status', 'priority']},
                   'assignment' : {'call':self.putAssignment, 
                                   'args':['team', 'requestName']},
                   'user' :  {'call':self.putUser, 
                              'args':['userName', 'email', 'dnName']},
                   'group' :  {'call':self.putGroup, 'args':['group', 'user']},
                   'version' :  {'call':self.putVersion, 'args':['version']},
                   'team' :  {'call':self.putTeam, 'args':['team']},
                   'workQueue' : {'call':self.putWorkQueue, 'args':['request', 'url']},
                   'message' : {'call':self.putMessage, 'args':['request']}
                   },
            'POST':{'request' : {'call':self.postRequest,
                                 'args':['requestName', 'events_written', 
                                         'events_merged', 'files_written',
                                         'files_merged', 'dataset']},
                    'user' : {'call':self.postUser, 'args':['user', 'priority']},
                    'group' : {'call':self.postGroup, 'args':['group', 'priority']} 
                    },
            'DELETE':{'request' : {'call':self.deleteRequest, 'args':['requestName']},
                      'user' :  {'call':self.deleteUser, 'args':['user']},
                      'group' :  {'call':self.deleteGroup, 'args':['group', 'user']},
                      'version' :  {'call':self.deleteVersion, 'args':['version']},
                      'team' :  {'call':self.deleteTeam, 'args':['team']}
                      }
            }
        # stop caching for all GET', PUT, POST, and DELETEs
        #for call in ['PUT', 'POST', 'DELETE']:
        #   for method, paramDict in self.methods[call].iteritems():
        #       paramDict['expires'] = 0

        self.addMethod("GET", "overview", self.getGlobalSummary) #expires=16000
        self.addMethod("GET", "resourceInfo", self.getResourceInfo)
        
    def getGlobalSummary(self):
        """ return summary data for requests from
            request manager, workqueue and couchDB"""
        self.initThread()
        return getGlobalSummaryView(self.hostAddress)

    def getResourceInfo(self):
        """ return summary data for requests from
            request manager, workqueue and couchDB"""
        self.initThread()
        return getResourceOverview()
    
    def initThread(self):
        """ The ReqMgr expects the DBI to be contained in the Thread  """
        myThread = threading.currentThread()
        # Get it from the DBFormatter superclass
        myThread.dbi = self.dbi
        #myThread.dialect = self.dialect

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
      
        self.initThread()
        requests = ListRequests.listRequests()
        if requestName == None:
            # add some details
            result = []
            for request in requests:
                requestName = request['RequestName']
                result.append(self.fillRequest(requestName, request['RequestID']))
            return result
        else:
            for request in requests:
                if request['RequestName'] == requestName:
                    # add some details
                    return self.fillRequest(requestName, request['RequestID'])
            raise RuntimeError("Cannot find request" + requestName)

    
    def fillRequest(self, requestName, requestID):
        """ Return a dict with the intimate details of the request """
        request = GetRequest.getRequest(requestID)
        assignments = GetRequest.getRequestAssignments(requestID)
        if assignments != []:
            request['Assignments'] = []
        for assignment in assignments:
            request['Assignments'].append(assignment['TeamName'])

        # show the status and messages
        request['RequestMessages'] = self.getMessage(requestName)
        # updates
        request['RequestUpdates'] = ChangeState.getProgress(requestName)
        # it returns a datetime object, which I can't pass through
        request['percent_complete'] = 0
        request['percent_success'] = 0
        for update in request['RequestUpdates']:
            update['update_time'] = str(update['update_time'])
            if update.has_key('percent_complete'):
                request['percent_complete'] = update['percent_complete']
            if update.has_key('percent_success'):
                request['percent_success'] = update['percent_success']
        return request


    def getAssignment(self, teamName=None, request=None):
        """ If a team name is passed in, get all assignments for that team.
        If a request is passed in, return a list of teams the request is assigned to """
        self.initThread()
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
                #result[req['RequestName']] = self.urlPrefix+req['RequestWorkflow']
                result[req['RequestName']] = req['RequestWorkflow']
            return result
        if request != None:
            reqID = self.requestID(request)
            assignments = GetRequest.getRequestAssignments(reqID)
            result = [assignment['TeamName'] for assignment in assignments] 
        return result


    def getUser(self, userName=None, group=None):
        """ No args returns a list of all users.  Group returns groups this user is in.  Username
            returs a JSON with information about the user """
        self.initThread()
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
        self.initThread()
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
        self.initThread()
        return SoftwareAdmin.listSoftware().keys()
      
    def getTeam(self):
        """ Returns a list of all teams registered with ReqMgr """
        self.initThread()
        return ProdManagement.listTeams()

    def getWorkQueue(self, request=None, workQueue=None):
        """ If a request is passed in, return the URl of the workqueue.
        If a workqueue is passed in, return all requests acquired by it """
        self.initThread()
        if workQueue != None:
            return ProdMgrRetrieve.findAssignedRequests(workQueue)
        if request != None:
            return ProdManagement.getProdMgr(request)

    def abortRequest(self, request):
        """ Changes the state of the request to "aborted", and asks the work queue
        to cancel its work """
        self.initThread()
        response = self.getWorkQueue(request=request)
        url = response[0]
        if url == None or url == "":
            raise cherrypy.HTTPError(400, "Cannot find URL for request " + request)
        workqueue = WorkQueue.WorkQueue({'endpoint': url})     
        workqueue.cancelWork([request], "request_name")
   
    def getMessage(self, request=None):
        """ Returns a list of messages attached to this request """
        self.initThread()
        return ChangeState.getMessages(request)

    def putWorkQueue(self, request, url):
        """ Registers the request as "acquired" by the workqueue with the given URL """
        self.initThread()
        ChangeState.changeRequestStatus(request, "acquired")
        return ProdManagement.associateProdMgr(request, urllib.unquote(url))


    def putRequest(self, requestName, status=None, priority=None):
        """ Checks the request n the body with one arg, and changes the status with kwargs """
        self.initThread()
        result = ""
        request = self.findRequest(requestName)
        if request == None:
#            try:
                request = self.makeRequest()
#            except Exception, ex:
#                cherrypy.response.status = 400
#                print "putRequest Error " + str(ex)
#                return str(ex)
        # see if status & priority need to be upgraded
        if status != None or priority != None:
            self.initThread()
            oldStatus = request['RequestStatus']

            if status != None:
                if not status in RequestStatus.StatusList:
                    raise RuntimeError, "Bad status code " + status
                if not request.has_key('RequestStatus'):
                    raise RuntimeError, "Cannot find status for request " + requestName
                if not status in RequestStatus.NextStatus[oldStatus]:
                    raise RuntimeError, "Cannot change status from %s to %s.  Allowed values are %s" % (
                           oldStatus, status,  RequestStatus.NextStatus[oldStatus])
                if status == 'aborted':
                    # delete from the workqueue
                    self.abortRequest(requestName)
                if priority != None:
                    ChangeState.changeRequestStatus(requestName, status, priority)
                    self.updatePriorityInWorkload(request, priority)
                else:
                    ChangeState.changeRequestStatus(requestName, status)
            else:
                ChangeState.changeRequestStatus(requestName, oldStatus, priority) 
                self.updatePriorityInWorkload(request, priority)
        return result

    def updatePriorityInWorkload(self, request, priority):
        """ Changes the priority that's stored in the workload """
        # fill in all details
        request = GetRequest.getRequest(request['RequestID'])
        print request
        helper = WMWorkloadHelper()
        helper.load(request['RequestWorkflow'])
        helper.data.request.priority = int(priority)
        print "SAVING " + request['RequestWorkflow']
        saveWorkload(helper, request['RequestWorkflow'])
 
        
    def makeRequest(self):
        """ Creates a new request, with a JSON-encoded schema that is sent in the
        body of the request """
        body = cherrypy.request.body.read()
        requestSchema = JsonWrapper.loads( body, encoding='latin-1' )
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
        self.initThread()
        # see if it's already assigned
        requestNamesAndIDs = ListRequests.listRequestsByTeam(urllib.unquote(team))
        if requestName in requestNamesAndIDs.keys():
            raise cherrypy.HTTPError(400,"Already assigned to this team")
        return ChangeState.assignRequest(requestName, urllib.unquote(team))

    def putUser(self, userName, email, dnName=None):
        """ Needs to be passed an e-mail address, maybe dnName """
        self.initThread()
        if Registration.isRegistered(userName):
            return "User already exists"
        result = Registration.registerUser(userName, email, dnName)
        return result

    def putGroup(self, group, user=None):
        """ Creates a group, or if a user is passed, adds that user to the group """
        self.initThread()
        if(user != None):
            # assume group exists and add user to it
            return GroupManagement.addUserToGroup(user, group)
        if GroupInfo.groupExists(group):
            return "Group already exists"
        return GroupManagement.addGroup(group)

    def putVersion(self, version):
        """ Registers a new CMSSW version with ReqMgr """
        self.initThread()
        return SoftwareAdmin.addSoftware(version)

    def putTeam(self, team):
        """ Registers a team with ReqMgr """
        self.initThread()
        return ProdManagement.addTeam(urllib.unquote(team))

    def putMessage(self, request):
        """ Attaches a message to this request """
        self.initThread()
        message = JsonWrapper.loads( cherrypy.request.body.read() )
        result = ChangeState.putMessage(request, message)
        return result

#    def postRequest(self, requestName, events_written=None, events_merged=None, 
#                    files_written=None, files_merged = None, dataset=None):
    def postRequest(self, requestName, **kwargs):
        """
        Add a progress update to teh request Id provided, params can
        optionally contain:
        - *events_written* Int
        - *events_merged*  Int
        - *files_written*  Int
        - *files_merged*   int
        - *percent_success* float
        - *percent_complete float
        - *dataset*        string (dataset name)
        """
 #       try:
        self.initThread()
#        kwargs = self.sanitise_input(requestName, events_written, events_merged, 
#                                     files_written,  files_merged, dataset)
        return ChangeState.updateRequest(requestName, kwargs)

    def postUser(self, user, priority):
        """ Change the user's priority """
        self.initThread()
        return UserManagement.setPriority(user, priority)

    def postGroup(self, group, priority):
        """ Change the group's priority """
        self.initThread()
        return GroupManagement.setPriority(group, priority)

    def deleteRequest(self, requestName):
        """ Deletes a request from the ReqMgr """
        self.initThread()
        request = self.findRequest(requestName)
        if request == None:
            raise cherrypy.HTTPError(404, "No such request")
        return RequestAdmin.deleteRequest(request['RequestID'])

    def deleteUser(self, user):
        """ Deletes a user, as well as deleting his requests and removing
            him from all groups """
        self.initThread()
        if user in self.getUser():
            requests = json.loads(self.getUser(user))['requests']
            for request in requests:
                self.deleteRequest(request)
            for group in GroupInfo.groupsForUser(user).keys():
                GroupManagement.removeUserFromGroup(user, group)
            return UserManagement.deleteUser(user)

    def deleteGroup(self, group, user=None):
        """ If no user is sent, delete the group.  Otherwise, delete the user from the group """
        self.initThread()
        if user == None:
            return GroupManagement.deleteGroup(group)
        else:
            return GroupManagement.removeUserFromGroup(user, group) 

    def deleteVersion(self, version):
        """ Un-register this software version with ReqMgr """
        self.initThread()
        SoftwareAdmin.removeSoftware(version)

    def deleteTeam(self, team):
        """ Delete this team from ReqMgr """
        self.initThread()
        ProdManagement.removeTeam(urllib.unquote(team))

    

