import time
import logging

from WMCore.WebTools.RESTModel import RESTModel
from WMCore.WMSpec.WMWorkload import WMWorkloadHelper
from WMCore.HTTPFrontEnd.RequestManager.ReqMgrWebTools import unidecode, removePasswordFromUrl
from WMCore.RequestManager.RequestMaker.Registry import retrieveRequestMaker
import WMCore.RequestManager.RequestDB.Connection as DBConnect
import WMCore.Wrappers.JsonWrapper as JsonWrapper
import WMCore.Lexicon
import WMCore.RequestManager.RequestMaker.CheckIn as CheckIn
import WMCore.RequestManager.RequestMaker.Processing.AnalysisRequest
import WMCore.RequestManager.RequestDB.Interface.Request.ChangeState as ChangeState
import WMCore.RequestManager.RequestDB.Interface.User.Registration as Registration
import WMCore.RequestManager.RequestDB.Interface.Group.Information as GroupInfo
import WMCore.RequestManager.RequestDB.Interface.Admin.GroupManagement as GroupAdmin
import WMCore.RequestManager.RequestDB.Interface.Admin.ProdManagement as ProdManagement
import cherrypy
import threading
import json
import cherrypy

class CRABRESTModel(RESTModel):
    
    def __init__(self, config={}):
        '''
        Initialise the RESTModel and add some methods to it.
        '''
        RESTModel.__init__(self, config)
        
        #self.hostAddress = config.model.reqMgrHost
        self.couchUrl = config.model.couchUrl
        self.workloadCouchDB = config.model.workloadCouchDB
        #/user 
        self._addMethod('POST', 'user', self.addNewUser,
                        args=[],
                        validation=[self.isalnum])
        #/task
        self._addMethod('GET', 'task', self.getTaskStatus,
                        args=['requestID'],
                        validation=[self.isalnum])
        self._addMethod('PUT', 'task', self.putTaskModifies,
                        args=['requestID'],
                        validation=[self.isalnum])
        self._addMethod('DELETE', 'task', self.deleteRequest,
                        args=['requestID'],
                        validation=[self.isalnum])
        self._addMethod('POST', 'task', self.postRequest,
                        args=['requestName'],
                        validation=[self.isalnum])
        #/config
        self._addMethod('POST', 'config', self.postUserConfig,
                        args=['config'],
                        validation=[self.checkConfig])
        
        # Server status
        self._addMethod('GET', 'status', self.getServerStatus)

        cherrypy.engine.subscribe('start_thread', self.initThread)


    def initThread(self, thread_index):
        """ The ReqMgr expects the DBI to be contained in the Thread  """
        myThread = threading.currentThread()
        #myThread = cherrypy.thread_data
        # Get it from the DBFormatter superclass
        myThread.dbi = self.dbi

    def checkConfig(self, pset):
        """ check user configuration """
        return pset

    def postUserConfig(self, pset):
        """ this must act as proxy for CouchDB. Upload user config and return DocID """
        logging.info(pset) 
        result = {}
        result['DocID'] = '1234'
        result['CouchURL'] = 'http://xyz'
        result['CouchDBName'] = 'myconfigcache' 
        return json.dump(result) 

    def getServerStatus(self):
        result = {
                'number_of_tasks':0,
                'my_proxy':'myproxy.cern.ch',
                'server_dn': '',
                'server_se_path': '/path/',
                'couch_url': "CONFIG_CACHE",
                'in_drain': False,
                'admin': "spiga"
                }
        return json.dump(result) 

    def isalnum(self, index):
        """ Validates that all input is alphanumeric, 
            with spaces and underscores tolerated"""
        for v in index.values():
            WMCore.Lexicon.identifier(v)
        return index
    
    def getTaskStatus(self, requestID):
        """     """
        return requestID 
 
    def putTaskModifies(self, requestID):
        """ Modify the task in any possible field : B/W lists, stop/start automation... """
        return requestID

    def deleteRequest(self, requestID):
        """    """
        return requestID  

    def postRequest(self, requestName):
        """ Checks the request n the body with one arg, and changes the status with kwargs """
        result = {} 
        body = cherrypy.request.body.read()
        requestSchema = unidecode(JsonWrapper.loads(body))
        logging.error(requestSchema)

        #requestName must be unique. Unique name is the ID
        currentTime = time.strftime('%y%m%d_%H%M%S',
                                 time.localtime(time.time()))

        requestSchema['RequestName'] = "%s_%s_%s" % (requestSchema["Username"], requestSchema['RequestName'],
                                                  currentTime)
        result['ID'] = requestSchema['RequestName']
        
        maker = retrieveRequestMaker(requestSchema['RequestType'])
        specificSchema = maker.schemaClass()
        specificSchema.update(requestSchema)
        url = cherrypy.url()
        # we only want the first part, before /task/
        url = url[0:url.find('/task')]
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
        # Auto Assign the requests     
        ChangeState.changeRequestStatus(requestName, 'assignment-approved')
        # Auto Assign the requests     
        ### what is the meaning of the Team in the Analysis use case? 
        ChangeState.assignRequest(requestName, requestSchema["Team"])
     
        #return  ID & status
        return json.dumps(result)

    
    def addNewUser(self):
        """
        """

        body = cherrypy.request.body.read()
        requestorInfos = unidecode(JsonWrapper.loads(body))

        user = requestorInfos["Username"]
        group = requestorInfos["Group"]
        team = requestorInfos["Team"]
        email = requestorInfos["Email"] 

        result = {}
        if group != None:
            if not GroupInfo.groupExists(group):
                GroupAdmin.addGroup(group)
                result[group] = 'regitered' 
        if user != None:
            if not Registration.isRegistered(user):
                Registration.registerUser(user, email)
                result[user] = 'regitered' 
        if group != None and user != None:
            factory = DBConnect.getConnection()
            idDAO = factory(classname = "Requestor.ID")
            userId = idDAO.execute(user)
            assocDAO = factory(classname = "Requestor.GetAssociation")
            assoc = assocDAO.execute(userId)
            if len(assoc) == 0:
                GroupAdmin.addUserToGroup(user, group)
        if team != None:
            ProdManagement.addTeam(team)
            result[team] = 'regitered' 
  
        return json.dumps(result)
