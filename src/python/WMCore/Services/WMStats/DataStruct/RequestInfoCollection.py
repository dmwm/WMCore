
class JobSummary(object):
    """
    job summary data structure from job format in couchdb
    """
    def __init__(self , jobStatus = None):
        self.jobStatus = {
                 "success": 0,
                 "canceled": 0,
                 "transition": 0,
                 "queued": {"first": 0, "retry": 0},
                 "submitted": {"first": 0, "retry": 0},
                 "submitted": {"pending": 0, "running": 0},
                 "failure": {"create": 0, "submit": 0, "exception": 0},
                 "cooloff": {"create": 0, "submit": 0, "job": 0},
                 "paused": {"create": 0, "submit": 0, "job": 0},
         }
        if jobStatus != None:
            self.addJobStatusInfo(jobStatus)
    
    def addJobStatusInfo(self, jobStatus):
        
        #TODO need to validate the structure.
        for key, value in self.jobStatus.items():
            if type(value) == int:
                self.jobStatus[key] += jobStatus.get(key, 0)
            elif type(value) == dict:
                for secondKey, secondValue in value.items():
                    if jobStatus.has_key(key) and jobStatus[key].has_key(secondKey):
                        self.jobStatus[key][secondKey] += jobStatus[key][secondKey]
    
    def addJobSummary(self, jobSummary):
        self.addJobStatusInfo(jobSummary.jobStatus)
                    
    def getTotalJobs(self):
        return (self.getSuccess() +
                self.jobStatus["canceled"] +
                self.jobStatus[ "transition"] +
                self.getFailure() +
                self.getCooloff() +
                self.getPaused() +
                self.getQueued() +
                self.getRunning() +
                self.getPending())
    
    def getSuccess(self):
        return self.jobStatus["success"] 
        
    def getFailure(self):
        
        return (self.jobStatus["failure"]["create"] + 
                self.jobStatus["failure"]["submit"] + 
                self.jobStatus["failure"]["exception"])
    
    def getCompleted(self):
        return self.getSuccess() + self.getFailure()
     
    def getSubmitted(self):
        return (self.jobStatus["submitted"]["first"] + 
                self.jobStatus["submitted"]["retry"])

    def getRunning(self): 
        return self.jobStatus["submitted"]["running"];
    
    def getPending(self):
        return self.jobStatus["submitted"]["pending"];

    def getCooloff(self):
        return (self.jobStatus["cooloff"]["create"] + 
                self.jobStatus["cooloff"]["submit"] + 
                self.jobStatus["cooloff"]["job"]);

    def getPaused(self):
        return (self.jobStatus["paused"]["create"] + 
                self.jobStatus["paused"]["submit"] + 
                self.jobStatus["paused"]["job"]);
    
    def getQueued(self):
        return (self.jobStatus["queued"]["first"] + 
                self.jobStatus["queued"]["retry"])
        
    def getJSONStatus(self):
        return {'sucess': self.getSuccess(),
                'failure': self.getFailure(),
                'cooloff': self.getCooloff(),
                'running': self.getRunning(),
                'queued': self.getQueued(),
                'pending': self.getPending(),
                'paused': self.getPaused(),
                'created': self.getTotalJobs() 
                }

class ProgressSummary(object):
    
    def __init__(self , progressReport = None):
        self.progress = {
                 "totalLumis": 0,
                 "events": 0,
                 "size": 0
         }
        
        if progressReport != None:
            self.addProgressReport(progressReport)
    
    def addProgressReport(self, progressReport):
        
        #TODO need to validate the structure.
        for key in self.progress.keys():
            self.progress[key] += progressReport.get(key, 0)
    
    def getReport(self):
        return self.progress
            
class TaskInfo(object):
    
    def __init__(self, requestName, taskName, data):
        self.requestName = requestName
        self.taskName = taskName 
        self.taskType = data.get('jobtype', "N/A")
        self.jobSummary = JobSummary(data.get('status', {}))
    
    def addTaskInfo(self, taskInfo):
        
        if not (self.requestName == taskInfo.requestName and 
                self.taskName == taskInfo.taskName):
            msg =  "%s: %s, %s: %s, %s: %s" % (self.requestName, taskInfo.requestName, 
                                               self.taskName, taskInfo.taskName,
                                               self.taskType, taskInfo.taskType)
            raise Exception("task doesn't match %s" % msg)
        
        self.jobSummary.addJobSummary(taskInfo.jobSummary)
            
            
class RequestInfo(object):
    
    def __init__(self, data):
        """
        data structure is 
        {'request_name1': 
            {'agent_url1': {'status'
        
        }
        """
        self.setData(data)

        
    def setData(self, data):
        self.requestName = data['workflow']
        self.data = data
        self.jobSummaryByAgent = {}
        self.tasks = {}
        self.tasksByAgent = {}
        self.jobSummary = JobSummary()
        if data.has_key('AgentJobInfo'):
            for agentUrl, agentRequestInfo in data['AgentJobInfo'].items():
                self.jobSummary.addJobStatusInfo(agentRequestInfo.get('status', {}))
                self.jobSummaryByAgent[agentUrl] = JobSummary(agentRequestInfo.get('status', {}))
                
                if agentRequestInfo.has_key('tasks'):
                    self.tasksByAgent[agentUrl] = {}
                    for taskName, data in agentRequestInfo['tasks'].items():
                        if not self.tasks.has_key(taskName):
                            self.tasks[taskName] = TaskInfo(self.requestName, taskName, data)
                        else:
                            self.tasks[taskName].addTaskInfo(TaskInfo(self.requestName, taskName, data))
                        # only one task by one agent - don't need to combine
                        self.tasksByAgent[agentUrl][taskName] = TaskInfo(self.requestName, taskName, data)
                        
    def getJobSummary(self):
        return self.jobSummary
    
    def getJobSummaryByAgent(self, agentUrl = None):
        if agentUrl:
            return self.jobSummaryByAgent[agentUrl]
        else:
            return self.jobSummaryByAgent
    
    def getTasksByAgent(self, agentUrl = None):
        if agentUrl:
            return self.tasksByAgent[agentUrl]
        else:
            return self.tasksByAgent
    
    def getTasks(self):
        return self.tasks
        
    def getTotalTopLevelJobs(self):
        return self.data.get("total_jobs", "N/A")
    
    def getTotalTopLevelJobsInWMBS(self):
        inWMBS = 0
        if self.data.has_key("AgentJobInfo"):
            for agentRequestInfo in self.data["AgentJobInfo"].values():
                inWMBS += agentRequestInfo['status'].get('inWMBS', 0)
        return inWMBS
    
    def getTotalInputLumis(self):
        return self.data.get("input_lumis", "N/A")
    
    def getTotalInputEvents(self):
        return self.data.get("input_events", "N/A")
    
    def getProgressSummaryByOutputDataset(self):
        """
        check sampleResult.json for datastructure
        """
        datasets = {};

        if not self.data.has_key("AgentJobInfo"):
            #ther is no report yet (no agent has reported)
            return datasets
        
        for agentRequestInfo in self.data["AgentJobInfo"].values():
            
            tasks = agentRequestInfo.get("tasks", [])
            for task in tasks:
                for site in tasks[task].get("sites", []):
                    for outputDS in tasks[task]["sites"][site].get("dataset", {}).keys():
                        #TODO: need update the record instead of replacing.
                        datasets.setdefault(outputDS, ProgressSummary())
                        datasets[outputDS].addProgressReport(tasks[task]["sites"][site]["dataset"][outputDS])
                        
        return datasets
     
    def filterRequest(self, conditionFunc):
        return conditionFunc(self.data)
    
             
    def getRequestTransition(self):
        return self.data["request_status"]
    
    def getRequestStatus(self, timeFlag = False):
        
        if timeFlag:
            return self.data["request_status"][-1]
        else:
            return self.data["request_status"][-1]['status']
     
class RequestInfoCollection(object):
    
    def __init__(self, data):
        self.collection = {}
        self.setData(data)
        
    def setData(self, data):
        for requestName, requestInfo in data.items():
            self.collection[requestName] = RequestInfo(requestInfo)
    
    def getData(self):
        return self.collection
    
    def filterRequests(self, conditionFunc):
        filtered = {}
        for name, reqInfo in self.collection.items():
            if reqInfo.filterRequest(conditionFunc):
                filtered[name] = reqInfo
        return filtered
            
    def getJSONData(self):
        result = {}
        for requestInfo in self.collection.values():
            result[requestInfo.requestName] = {}
            for agentUrl, jobSummary in requestInfo.getJobSummaryByAgent().items():
                result[requestInfo.requestName][agentUrl]= jobSummary.getJSONStatus()
        return result
    