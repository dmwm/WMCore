from builtins import object, str, bytes
from future.utils import viewitems
from memory_profiler import profile
import time
from WMCore.ReqMgr.DataStructs.Request import RequestInfo, protectedLFNs

class DataCache(object):
    # TODO: need to change to  store in  db instead of storing in the memory
    # When mulitple server run for load balancing it could have different result
    # from each server.
    def __init__(self):
        self._duration = 300  # 5 minitues
        self._lastedActiveDataFromAgent = {}
    
#    @staticmethod
    def getDuration(self):
        return self._duration

 #   @staticmethod
    def setDuration(self, sec):
        self._duration = sec

  #  @staticmethod
    @profile
    def getlatestJobData(self):
        if (self._lastedActiveDataFromAgent):
            return self._lastedActiveDataFromAgent["data"]
        else:
            return {}

   # @staticmethod
    def isEmpty(self):
        # simple check to see if the data cache is populated
        return not self._lastedActiveDataFromAgent.get("data")

    #@staticmethod
    @profile
    def setlatestJobData(self, jobData):
        self._lastedActiveDataFromAgent["time"] = int(time.time())
        self._lastedActiveDataFromAgent["data"] = jobData

    #@staticmethod
    def islatestJobDataExpired(self):
        if not self._lastedActiveDataFromAgent:
            return True

        if (int(time.time()) - self._lastedActiveDataFromAgent["time"]) > self._duration:
            return True
        return False

    #@staticmethod
    def filterData(self, filterDict, maskList):
        reqData = self.getlatestJobData()

        for _, reqInfo in viewitems(reqData):
            reqData = RequestInfo(reqInfo)
            if reqData.andFilterCheck(filterDict):
                for prop in maskList:
                    result = reqData.get(prop, [])

                    if isinstance(result, list):
                        for value in result:
                            yield value
                    elif result is not None and result != "":
                        yield result

    #@staticmethod
    def filterDataByRequest(self, filterDict, maskList=None):
        reqData = self.getlatestJobData()

        if maskList is not None:
            if isinstance(maskList, (str, bytes)):
                maskList = [maskList]
            if "RequestName" not in maskList:
                maskList.append("RequestName")

        for _, reqDict in viewitems(reqData):
            reqInfo = RequestInfo(reqDict)
            if reqInfo.andFilterCheck(filterDict):

                if maskList is None:
                    yield reqDict
                else:
                    resultItem = {}
                    for prop in maskList:
                        resultItem[prop] = reqInfo.get(prop, None)
                    yield resultItem

    #@staticmethod
    def getProtectedLFNs(self):
        reqData = self.getlatestJobData()

        for _, reqInfo in viewitems(reqData):
            for dirPath in protectedLFNs(reqInfo):
                yield dirPath
