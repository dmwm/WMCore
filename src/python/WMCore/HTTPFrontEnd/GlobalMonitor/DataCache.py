import time

class DataCache(object):
    _duration = 600 # ten minitues
    _requestData = {};
    _agentData = {};
    _siteData = {};

    @staticmethod
    def getDuration():
        return DataCache._duration;

    @staticmethod
    def setDuration(sec):
        DataCache._duration = sec;

    @staticmethod
    def getRequestData():
        if (DataCache._requestData):
            return DataCache._requestData["data"]
        else:
            return None

    @staticmethod
    def getAgentData():
        if (DataCache._agentData):
            return DataCache._agentData["data"]
        else:
            return None

    @staticmethod
    def getSiteData():
        if (DataCache._siteData):
            return DataCache._siteData["data"]
        else:
            return None

    @staticmethod
    def setRequestData(requestData):
        DataCache._requestData["time"] = int(time.time())
        DataCache._requestData["data"] = requestData

    @staticmethod
    def setAgentData(agentData):
        DataCache._agentData["time"] = int(time.time())
        DataCache._agentData["data"] = agentData

    @staticmethod
    def setSiteData(siteData):
        DataCache._siteData["time"] = int(time.time())
        DataCache._siteData["data"] = siteData

    @staticmethod
    def isRequestDataExpired():
        if not DataCache._requestData:
            return True

        if (int(time.time()) - DataCache._requestData["time"]) > DataCache._duration:
            return True
        return False

    @staticmethod
    def isSiteDataExpired():
        if not DataCache._siteData:
            return True

        if (int(time.time()) - DataCache._siteData["time"]) > DataCache._duration:
            return True
        return False

    @staticmethod
    def isAgentDataExpired():
        if not DataCache._agentData:
            return True

        if (int(time.time()) - DataCache._agentData["time"]) > DataCache._duration:
            return True
        return False
