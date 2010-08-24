from WMCore.WebTools.RESTModel import RESTModel
from WMCore.WMSpec.WMWorkload import WMWorkloadHelper
import WMCore.RequestManager.RequestDB.Interface.User.Registration as Registration
import WMCore.RequestManager.RequestDB.Interface.User.Requests as UserRequests
import WMCore.RequestManager.RequestDB.Interface.Request.ListRequests as ListRequests
import WMCore.RequestManager.RequestDB.Interface.Request.GetRequest as GetRequest
import WMCore.RequestManager.RequestDB.Interface.Admin.RequestManagement as RequestAdmin
import WMCore.RequestManager.RequestDB.Interface.Admin.ProdManagement as ProdManagement
import WMCore.RequestManager.RequestDB.Interface.Admin.GroupManagement as GroupAdmin
import WMCore.RequestManager.RequestDB.Interface.Admin.UserManagement as UserAdmin
import WMCore.RequestManager.RequestDB.Interface.ProdSystem.ProdMgrRetrieve as ProdMgrRetrieve
import WMCore.RequestManager.RequestDB.Interface.Admin.SoftwareManagement as SoftwareAdmin
import WMCore.RequestManager.RequestDB.Interface.Request.ChangeState as ChangeState
import WMCore.RequestManager.RequestDB.Settings.RequestStatus as RequestStatus
import WMCore.RequestManager.RequestMaker.WMWorkloadCache as WMWorkloadCache
import WMCore.RequestManager.RequestMaker.CheckIn as CheckIn
import WMCore.RequestManager.RequestDB.Interface.Group.Information as GroupInfo
import WMCore.RequestManager.RequestDB.Interface.ProdSystem.ProdMgrRetrieve as ProdMgrRetrieve
from WMCore.RequestManager.RequestMaker.Processing.RecoRequest import *
from WMCore.RequestManager.RequestMaker.Processing.ReRecoRequest import *
from WMCore.RequestManager.RequestMaker.Registry import retrieveRequestMaker
import cherrypy
import threading
import WMCore.Wrappers.JsonWrapper as JsonWrapper
import urllib

class ReqMgrRESTModel(RESTModel):
    def __init__(self, config = {}):
        RESTModel.__init__(self, config)
        #self.dialect = config.dialect
        self.urlPrefix = '%s/download?filepath=' % config.model.reqMgrHost
        self.cache = WMWorkloadCache.WMWorkloadCache(config.model.workloadCache)

        self.methods = {'GET':{'request' : {'call':self.getRequest, 'args':['requestName']},
                               'assignment' : {'call':self.getAssignment, 'args':['teamName', 'request']},
                               'user' :  {'call':self.getUser, 'args':['userName']},
                               'group' :  {'call':self.getGroup, 'args':['group', 'user']},
                               'version' :  {'call':self.getVersion, 'args':[]},
                               'team' :  {'call':self.getTeam, 'args':[]},
                               'workQueue' : {'call':self.getWorkQueue, 'args':['request', 'workQueue']},
                               'message' : {'call':self.getMessage, 'args':['request']}
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
                                'group' : {'call':self.postUser, 'args':['group', 'priority']} 
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
        requests = ListRequests.listRequests()
        for request in requests:
            if request['RequestName'] == requestName:
                return request['RequestID']
        raise RuntimeError("No such request")

    def getRequest(self, requestName=None):
    #def getRequest(self, *args, **kwargs):
        self.initThread()
        requests = ListRequests.listRequests()
        if requestName == None:
            # add some details
            result = []
            for request in requests:
               result.append(GetRequest.getRequest(request['RequestID']))
            return result
        else:
            for request in requests:
                if request['RequestName'] == requestName:
                    # add some details
                    request = GetRequest.getRequest(request['RequestID'])
                    assignments= GetRequest.getRequestAssignments(self.requestID(requestName))
                    if assignments != []:
                        request['Assignments'] = []
                    for assignment in assignments:
                        request['Assignments'].append(assignment['TeamName'])

                    # show the status and messages
                    request['RequestMessages'] = self.getMessage(requestName)
                    # updates
                    request['RequestUpdates'] = ChangeState.getProgress(requestName)
                    # it returns a datetime object, which I can't pass through
                    for update in request['RequestUpdates']:
                        update['update_time'] = str(update['update_time'])
                    return request
            raise RuntimeError("Cannot find request" + requestName)



    def getAssignment(self, teamName=None, request=None):
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
                result[req['RequestName']] = self.urlPrefix+req['RequestWorkflow']
            return result
        if request != None:
            reqID = self.requestID(request)
            assignments= GetRequest.getRequestAssignments(reqID)
            result = [assignment['TeamName'] for assignment in assignments] 
        return result


    def getUser(self, userName=None):
        """ No args returns a list of all users.  Arg returs all request for this user """
        self.initThread()
        if userName == None:
            return Registration.listUsers()
        else:
            return UserRequests.listRequests(userName).keys()
        

    def getGroup(self, group=None, user=None):
        """ No args lists all groups, one args lists users in group """
        self.initThread()
        if group == None:
            if user == None:
                return GroupInfo.listGroups()
            else:
                # check if the user exists first
                if not user in self.getUser():
                   cherrypy.response.status = 400
                   return "Cannot find user " + user + ".  Please add one using PUT user."
                return GroupInfo.groupsForUser(user).keys()
        else:
            try:
                return GroupInfo.usersInGroup(group)
            except Exception, ex:
                raise RuntimeError, "Error finding group " + group

    def getVersion(self):
        self.initThread()
        return SoftwareAdmin.listSoftware().keys()
      
    def getTeam(self):
        self.initThread()
        return ProdManagement.listTeams()

    def getWorkQueue(self, request=None, workQueue=None):
        self.initThread()
        if workQueue != None:
            return ProdMgrRetrieve.findAssignedRequests(workQueue)
        if request != None:
            return ProdManagement.getProdMgr(request)

    def getMessage(self, request=None):
        self.initThread()
        return ChangeState.getMessages(request)

    def putWorkQueue(self, request, url):
        self.initThread()
        ChangeState.changeRequestStatus(request, "assigned-prodmgr")
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
                if priority != None:
                    ChangeState.changeRequestStatus(requestName, status, priority)
                    self.updatePriorityInWorkflow(requestName, priority)
                else:
                    ChangeState.changeRequestStatus(requestName, status)
            else:
                ChangeState.changeRequestStatus(requestName, oldStatus, priority) 
        return result

    def updatePriorityInWorkflow(self, requestName, priority):
        reqID = self.requestID(requestName)
        request = GetRequest.getRequest(reqID)
        assert(request != None)
        lfn = request['RequestWorkflow']
        pfn = self.cache.cache  + '/' + lfn
        helper = WMWorkloadHelper()
        helper.load(pfn)
        helper.data.request.priority = priority
        helper.save(pfn)
 
        
    def makeRequest(self):
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
        # fill the WorkflowSpec's URL
        helper = WMWorkloadHelper(request['WorkflowSpec'])
        helper.setSpecUrl(self.urlPrefix+self.cache.getLfn(helper.data))
        CheckIn.checkIn(request, self.cache)
        return requestSchema

    def putAssignment(self, team, requestName):
        self.initThread()
        # see if it's already assigned
        requestNamesAndIDs = ListRequests.listRequestsByTeam(urllib.unquote(team))
        if requestName in requestNamesAndIDs.keys():
            cherrypy.response.status = 400
            return "Already assigned to this team"
        return ChangeState.assignRequest(requestName, urllib.unquote(team))

    def putUser(self, userName, email, dnName=None):
        """ Needs to be passed an e-mail address, maybe dnName """
        self.initThread()
        print "PUTUSER"
        if Registration.isRegistered(userName):
            return "User already exists"
        result = Registration.registerUser(userName, email, dnName)
        print self.getUser()
        return result

    def putGroup(self, group, user=None):
        self.initThread()
        if(user != None):
            # assume group exists and add user to it
            return GroupAdmin.addUserToGroup(user, group)
        if GroupInfo.groupExists(group):
            return "Group already exists"
        return GroupAdmin.addGroup(group)

    def putVersion(self, version):
        self.initThread()
        return SoftwareAdmin.addSoftware(version)

    def putTeam(self, team):
        self.initThread()
        return ProdManagement.addTeam(urllib.unquote(team))

    def putMessage(self, request):
        self.initThread()
        message = JsonWrapper.loads( cherrypy.request.body.read() )
        return ChangeState.putMessage(request, message)

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
        # Change the user's priority
        pass

    def postGroup(self, group, priority):
        # Change the group's priority
        pass

    def deleteRequest(self, requestName):
        self.initThread()
        try:
            request = self.findRequest(requestName)
            if request == None:
                raise RuntimeError("No such request")
            return RequestAdmin.deleteRequest(request['RequestID'])
        except Exception, ex:
            cherrypy.response.status = 400
            return str(ex)

    def deleteUser(self, user):
        self.initThread()
        if user in self.getUser():
            for request in self.getUser(user):
                self.deleteRequest(request)
            for group in GroupInfo.groupsForUser(user).keys():
                GroupAdmin.removeUserFromGroup(user, group)
            return UserAdmin.deleteUser(user)

    def deleteGroup(self, group, user=None):
        self.initThread()
        if user == None:
            return GroupAdmin.deleteGroup(group)
        else:
            return GroupAdmin.removeUserFromGroup(user, group) 

    def deleteVersion(self, version):
        self.initThread()
        SoftwareAdmin.removeSoftware(version)

    def deleteTeam(self, team):
        self.initThread()
        ProdManagement.removeTeam(urllib.unquote(team))

    

