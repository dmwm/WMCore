import time

from WMCore.ReqMgr.DataStructs.Request import RequestInfo, protectedLFNs

def _updateFilteredResult(result, key, value, curentExcluisveList, childExclusiveList):

    if key not in curentExcluisveList:
        childKeyFlag = False
        if len(childExclusiveList) > 0:
            for prop, cExList in childExclusiveList[0].items():
                if key == prop:
                    childKeyFlag = True
                    result[key] = filterExcludeList(value, cExList, childExclusiveList[1:])

            if not childKeyFlag:
                result[key] = value
    return

def filterExcludeList(wmstatsCache, exclude, childExclusiveList):
    """

    :param wmstatsCache: wmstats cache data
    :param exclude: dict format of {"key": "skip"|"start", "list": list of excluding property]
    :param childExclusiveList: list of exculding property for child level. it is chained
           - first item is a child, second item is grand child, etc (it doesn't support multiple child

    :return: filtered data of wmstatsCache
    """
    result = {}
    for k, v in wmstatsCache.items():
        # when keys are not know ahead and we have to filter next level. i.e. task name, site name as key
        if exclude["key"] == "skip":
            result[k] = {}
            for sk, sv in v.items():
                _updateFilteredResult(result[k], sk, sv, exclude["list"], childExclusiveList)
        else:
            _updateFilteredResult(result, k, v, exclude["list"], childExclusiveList)

    return result

def wmstatsFilter(wmstatsCache):
    """
    filtering out some of the
    :param data:
    :return:
    """
    # list of top level property to exclude
    requestExcludeList = {"key": "start", "list": ["Comments", "DN", "ChainParentageMap", "_id",
                                                   "ValidStatus", "VoGroup", "VoRole"]}
    # list of AgentJobInfo[agent_url] level filtering.
    childExclusiveList = [{"AgentJobInfo": {"key": "skip", "list": ["_id", "_rev", "workflow", "agent_url",
                                                                    "type", "agent_version", "timestamp"]}}]
    # list of ["AgentJobInfo"][agent_url]["task"][taskname] level filtering.
    childExclusiveList.append({"tasks": {"key": "skip", "list": []}})
    # list of ["AgentJobInfo"][agent_url]["task"][taskname]["sites"][sitename] level filtering.
    childExclusiveList.append({"sites": {"key": "skip", "list": ["cmsRunCPUPerformance", "wrappedTotalJobTime"]}})
    return filterExcludeList(wmstatsCache, requestExcludeList, childExclusiveList)

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

        for _, reqInfo in reqData.iteritems():
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
            if isinstance(maskList, basestring):
                maskList = [maskList]
            if "RequestName" not in maskList:
                maskList.append("RequestName")

        for _, reqDict in reqData.iteritems():
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
    def wmstatsCacheData():
        """
        filter out unused data from wmstats
        :return: filtered data
        """

        filteredData = {}
        reqData = DataCache.getlatestJobData()
        for request, reqDict in reqData.iteritems():
            filteredData[request] = wmstatsFilter(reqDict)
        return filteredData

    @staticmethod
    def getProtectedLFNs():
        reqData = DataCache.getlatestJobData()

        for _, reqInfo in reqData.iteritems():
            for dirPath in protectedLFNs(reqInfo):
                yield dirPath