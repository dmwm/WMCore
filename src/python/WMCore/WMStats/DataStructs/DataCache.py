from builtins import object
import time
from WMCore.ReqMgr.DataStructs.Request import RequestInfo, protectedLFNs

class DataCache(object):
    # TODO: need to change to  store in  db instead of storing in the memory 
    # When mulitple server run for load balancing it could have different result
    # from each server. 
    _duration = 300 # 5 minitues
    _lastedActiveDataFromAgent = {}
    
    @staticmethod
    def getDuration():
        return DataCache._duration

    @staticmethod
    def setDuration(sec):
        DataCache._duration = sec

    @staticmethod
    def getlatestJobData():
        if (DataCache._lastedActiveDataFromAgent):
            return DataCache._lastedActiveDataFromAgent["data"]
        else:
            return {}

    @staticmethod
    def setlatestJobData(jobData):
        DataCache._lastedActiveDataFromAgent["time"] = int(time.time())
        DataCache._lastedActiveDataFromAgent["data"] = jobData

    @staticmethod
    def islatestJobDataExpired():
        if not DataCache._lastedActiveDataFromAgent:
            return True

        if (int(time.time()) - DataCache._lastedActiveDataFromAgent["time"]) > DataCache._duration:
            return True
        return False

    @staticmethod
    def filterData(filterDict, maskList):
        reqData = DataCache.getlatestJobData()

        for _, reqInfo in reqData.items():
            reqData = RequestInfo(reqInfo)
            if reqData.andFilterCheck(filterDict):
                for prop in maskList:
                    result = reqData.get(prop, [])
                    
                    if isinstance(result, list):
                        for value in result:
                            yield value
                    elif result is not None and result != "":
                        yield result
                        
    @staticmethod
    def getProtectedLFNs():
        reqData = DataCache.getlatestJobData()

        for _, reqInfo in reqData.items():
            for dirPath in protectedLFNs(reqInfo):
                yield dirPath