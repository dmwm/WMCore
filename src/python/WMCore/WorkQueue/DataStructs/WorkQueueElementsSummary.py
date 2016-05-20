"""
WorkQueueElementsSummary

"""
from __future__ import (print_function, division)
from pprint import pprint
from collections import defaultdict
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
    
    def elementsWithHigherPriorityInSameSites(self, requestName, returnFormat="dict"):
        
        if requestName not in self.wqResultsByRequest:
            return None
            
        priority = self.wqResultsByRequest[requestName]['Priority']
        creationTime = self.wqResultsByRequest[requestName]['Elements'][0]['CreationTime']
        
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
                    if element['CreationTime'] > creationTime:
                        continue
                    if len(sites) > 0:
                        possibleSites = element.possibleSites()
                        if len(set(possibleSites) & sites) > 0:
                            sortedElements.append(element)
                    else:
                        sortedElements.append(element)
        # sort elements to get them in priority first and timestamp order
        sortedElements.sort(key=lambda element: element['CreationTime'])
        sortedElements.sort(key = lambda x: x['Priority'], reverse = True)
        if returnFormat == "list":
            return sortedElements
        elif returnFormat == "dict":
            sortedByRequest = defaultdict(list)
            for ele in sortedElements:
                sortedByRequest[ele['RequestName']].append(ele)
            
            for request in sortedByRequest:
                sortedByRequest[request] = WorkQueueElementResult(Elements=sortedByRequest[request])
            return sortedByRequest
    
    def getPossibleSitesByRequest(self, requestName):
        
        if requestName not in self.wqResultsByRequest:
            return None
        # this will include all the possible sites on the requests
        # TODO: when different blocks are located in different site it need to handled
        sites = set()
        for ele in self.wqResultsByRequest[requestName]['Elements']:
            sites = sites | set(ele.possibleSites())
        return sites
        
    def getWQElementResultsByRequest(self, requestName = None):
        if requestName:
            return self.wqResultsByRequest.get(requestName, None)
        else:
            return self.wqResultsByRequest
    
    def printSummary(self, request):
        
        wqResult = self.getWQElementResultsByRequest(request)
        #pprint(elements)
        print("%s prioty: %s, available elements: %s " % (request, wqResult["Priority"], len(wqResult['Elements'])))
        
        sites = self.getPossibleSitesByRequest(request)
        print("possble sites for %s:" % request)
        pprint(list(sites))
        
        higher = self.elementsWithHigherPriorityInSameSites(request)
        total = 0
        for request in higher:
            wqResult = higher[request]
            availableEle = len(wqResult.availableItems())
            total += availableEle
            maxJobEle = wqResult.getMaxJobElement()
            minJobEle = wqResult.getMinJobElement()
            print("""%s: Priority: %s, available elements: %s, total jobs: %s,
                    max jobs: %s (element_id: %s), min jobs: %s (element_id: %s)""" % (
                   request, wqResult["Priority"], availableEle, wqResult['Jobs'],
                   maxJobEle["Jobs"], maxJobEle.id,
                   minJobEle["Jobs"], minJobEle.id))
        print("higher priority elements %s"  % total)

