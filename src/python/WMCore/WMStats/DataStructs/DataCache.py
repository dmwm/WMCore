import time

class DataCache(object):
    # TODO: need to change to  store in  db instead of storing in the memory 
    # When mulitple server run for load balancing it could have different result
    # from each server. 
    _duration = 300 # 5 minitues
    _lastedActiveDataFromAgent = {};
    
    @staticmethod
    def getDuration():
        return DataCache._duration;

    @staticmethod
    def setDuration(sec):
        DataCache._duration = sec;

    @staticmethod
    def getlatestJobData():
        if (DataCache._lastedActiveDataFromAgent):
            return DataCache._lastedActiveDataFromAgent["data"]
        else:
            return None


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

