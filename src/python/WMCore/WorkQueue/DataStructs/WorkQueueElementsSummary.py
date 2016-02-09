"""
WorkQueueElementsSummary

"""
from __future__ import (print_function, division)

from WMCore.WorkQueue.DataStructs.WorkQueueElementResult import WorkQueueElementResult

class WorkQueueElementsSummary(object):
    """Class to hold the status of a related group of WorkQueueElements"""
    def __init__(self, elements):
        self.elements = elements
        
        elementsByRequest = {}
        for ele in elements:
            elementsByRequest.setdefault(ele["RequestName"], [])
            elementsByRequest[ele["RequestName"]].append(ele)
            
        self.wqResultsByRequest = {}
        for reqName, wqElements in elementsByRequest.items():
            self.wqResultsByRequest[reqName] = WorkQueueElementResult(Elements=wqElements)
    
    def elementsWithHigherPriorityInSameSites(self, requestName):
        
        if requestName not in self.wqResultsByRequest:
            return None
        
        priority = self.wqResultsByRequest[requestName]['Priority']
        
        # this will include all the possible sites on the requests
        # TODO: when different blocks are located in different site it need to handled
        sites = self.getPossibleSitesByRequest(requestName)
        
        sortedElements = []    
        for reqName in self.wqResultsByRequest:
            # skip the workflow
            if reqName == requestName:
                continue
            if self.wqResultsByRequest[reqName]['Priority'] >= priority:
                for element in self.wqResultsByRequest[reqName]['Elements']:
                    if len(sites) > 0:
                        possibleSites = element.possibleSites()
                        if len(set(possibleSites) & sites) > 0:
                            sortedElements.append(element)
                    else:
                        sortedElements.append(element)
        # sort elements to get them in priority first and timestamp order
        sortedElements.sort(key=lambda element: element['CreationTime'])
        sortedElements.sort(key = lambda x: x['Priority'], reverse = True)
        return sortedElements
    
    def getPossibleSitesByRequest(self, requestName):
        
        if requestName not in self.wqResultsByRequest:
            return None
        # this will include all the possible sites on the requests
        # TODO: when different blocks are located in different site it need to handled
        sites = set()
        for ele in self.wqResultsByRequest[requestName]['Elements']:
            sites = sites | set(ele.possibleSites())
        return sites
        
    def getWQElementResultsByReauest(self, requestName = None):
        if requestName:
            return self.wqResultsByRequest.get(requestName, None)
        else:
            return self.wqResultsByRequest
