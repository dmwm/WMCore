from builtins import object, str, bytes
from future.utils import viewitems

import time
from WMCore.ReqMgr.DataStructs.Request import RequestInfo, protectedLFNs

class DataCache(object):
    # TODO: need to change to  store in  db instead of storing in the memory
    # When mulitple server run for load balancing it could have different result
    # from each server.
    _duration = 300  # 5 minitues
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
    def isEmpty():
        # simple check to see if the data cache is populated
        return not DataCache._lastedActiveDataFromAgent.get("data")

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

    @staticmethod
    def filterDataByRequest(filterDict, maskList=None):
        reqData = DataCache.getlatestJobData()

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

    @staticmethod
    def getProtectedLFNs():
        reqData = DataCache.getlatestJobData()

        for _, reqInfo in viewitems(reqData):
            for dirPath in protectedLFNs(reqInfo):
                yield dirPath