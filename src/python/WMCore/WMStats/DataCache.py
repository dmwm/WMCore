import time

class DataCache(object):
    _duration = 600 # ten minitues
    _agentRequestData = {};

    @staticmethod
    def getDuration():
        return DataCache._duration;

    @staticmethod
    def setDuration(sec):
        DataCache._duration = sec;

    @staticmethod
    def getRequestData():
        if (DataCache._agentRequestData):
            return DataCache._agentRequestData["data"]
        else:
            return None

    @staticmethod
    def setRequestData(agentRequestData):
        DataCache._agentRequestData["time"] = int(time.time())
        DataCache._agentRequestData["data"] = agentRequestData

    @staticmethod
    def isRequestDataExpired():
        if not DataCache._agentRequestData:
            return True

        if (int(time.time()) - DataCache._agentRequestData["time"]) > DataCache._duration:
            return True
        return False
