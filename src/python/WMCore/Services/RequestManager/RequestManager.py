from WMCore.Wrappers import JsonWrapper
from WMCore.Services.Service import Service

from WMCore.Services.EmulatorSwitch import emulatorHook

# emulator hook is used to swap the class instance
# when emulator values are set.
# Look WMCore.Services.EmulatorSwitch module for the values
@emulatorHook
class RequestManager(Service):

    """
    API for dealing with retrieving information from RequestManager dataservice

    """

    def __init__(self, dict = {}, secure = False):
        """
        responseType will be either xml or json
        """

        if 'endpoint' not in dict:
            #TODO needs to change proper default location
            dict['endpoint'] = "%scmssrv49.fnal.gov:8585/reqMgr/" % \
                                ((secure and "https://" or "http://"))

        dict.setdefault("accept_type", "application/json")
        # cherrypy converts request.body to params when content type is set
        # application/x-www-form-urlencodeds
        dict.setdefault("content_type", 'application/x-www-form-urlencoded')
        dict.setdefault('cacheduration', 0)
        dict.setdefault("accept_type", "application/json")
        # cherrypy converts request.body to params when content type is set
        # application/x-www-form-urlencoded
        dict.setdefault("content_type", 'application/x-www-form-urlencoded')
        self.encoder = JsonWrapper.dumps
        Service.__init__(self, dict)

    def _getResult(self, callname, clearCache = True,
                   args = None, verb = "GET", encoder = None, decoder = JsonWrapper.loads,
                   contentType = None):
        """
        _getResult_

        retrieve JSON/XML formatted information given the service name and the
        argument dictionaries

        TODO: Probably want to move this up into Service
        """
        result = ''
        file = callname.replace("/", "_")
        if clearCache:
            self.clearCache(file, args, verb)

        f = self.refreshCache(file, callname, args, encoder = encoder,
                              verb = verb, contentType = contentType)
        result = f.read()
        f.close()

        if result and decoder:
            result = decoder(result)
        return result

    def getRequest(self, requestName = None):

        """
        _getRequest_

        """
        args = {}
        if requestName:
            args['requestName'] = requestName
        callname = 'request'
        return self._getResult(callname, args = args, verb = "GET")

    def getRequestNames(self):

        """
        _getRequestNames_

        """

        callname = 'requestnames'
        return self._getResult(callname, verb = "GET")

    def getAssignment(self, teamName = None, request = None):

        args = {}
        # server only take teamname if both are specified.
        if teamName:
            args['teamName'] = teamName
        elif request:
            args['request'] = request

        callname = 'assignment'
        return self._getResult(callname, args = args, verb = "GET")

    def getRunningOpen(self, teamName):
        args = {'teamName': teamName, 'status': 'running-open'}
        callname = 'requestsByStatusAndTeam'
        return self._getResult(callname, args = args, verb = "GET")

    def getWorkQueue(self, **args):
        "get list of workqueue urls from requestmanager"
        callname = 'workQueue'
        return self._getResult(callname, args = args, verb = "GET")


    def putWorkQueue(self, requestName, prodAgentUrl = None):
        args = {}

        callname = 'workQueue'
        args['request'] = requestName
        args['url'] = str(prodAgentUrl)
        return self._getResult(callname, args = args, verb = "PUT")

    def getTeam(self):
        """Return teams known to this ReqMgr"""
        return self._getResult('team', verb = 'GET').keys()

    def putTeam(self, team):
        args = {'team': team}
        callname = 'team'
        return self._getResult(callname, args = args, verb = "PUT")

    def reportRequestProgress(self, requestName, **kargs):
        """Update ReqMgr with request progress"""
        callname = 'request/%s' % requestName
        args = {}
        args.update(kargs)

        return self._getResult(callname, args = args, verb = "POST",
                               contentType = 'application/json')

    def reportRequestStatus(self, requestName, status):
        """Update reqMgr about request"""
        callname = 'request'
        args = {}
        args["requestName"] = requestName
        args["status"] = status
        return self._getResult(callname, args = args, verb = "PUT")

    def sendMessage(self, request, msg):
        """Attach a message to the request"""
        callname = "message/%s" %  request
        return self._getResult(callname, args = msg, verb = "PUT",
                               encoder = JsonWrapper.dumps,
                               contentType = 'application/json')

    def makeRequest(self, ScramArch = 'slc5_amd64_gcc434',
                    DbsUrl = 'https://cmsweb.cern.ch/dbs/prod/global/DBSReader',
                    TimePerEvent = 60, Memory = 2147, SizePerEvent = 512,
                    **kwargs):
        """Submit parameters to reqmgr and create request
        See WMCore/HTTPFrontEnd/RequestManager/ReqMgrWebTools.py:makeRequest
        for parameters
        Returns details of created workflow.
        """
        kwargs.update({'ScramArch' : ScramArch, 'DbsUrl' : DbsUrl,
                       'TimePerEvent' : TimePerEvent, 'Memory' : Memory,
                       'SizePerEvent' : SizePerEvent})
        return self._getResult('request', args = kwargs, verb = 'PUT',
                                encoder = JsonWrapper.dumps,
                                contentType = 'application/json',
                                )

    def assign(self, request, team, acquisitionEra = None, processingVersion = None,
                action = 'Assign', **kwargs):
        """Assign request"""
        kwargs['Team' + team] = 'on'
        kwargs['checkbox' + request] = 'on'
        kwargs['action'] = action
        kwargs['AcquisitionEra'] = acquisitionEra
        kwargs['ProcessingVersion'] = processingVersion
        # Can't use api url as assignment page has a lot of unique logic.
        return self._getResult('../assign/handleAssignmentPage',
                                args = kwargs, verb = 'POST', decoder = False)
    
    def updateRequestStatus(self, requestName, status):
        args = {'requestName': requestName, 'status': status}
        callname = 'request'
        return self._getResult(callname, args = args, verb = "PUT")
    
    def putRequestStats(self, request, stats):
        args = {'requestName': request, 'stats': JsonWrapper.dumps(stats)}
        callname = 'request'
        return self._getResult(callname, args = args, verb = "PUT")
        
