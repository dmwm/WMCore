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
        for reqName, wqElements in elementsByRequest.iteritems():
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

    def getGlobalStatusSummary(self, status=None):
        """
        Goes through all the request/workqueue elements and print a total overview of:
         *) amount of workqueue elements and
         *) top level jobs in each status
        """
        numberOfWQESummary = {}
        numberOfJobsSummary = {}
        for reqName in self.wqResultsByRequest:
            for elem in self.wqResultsByRequest[reqName]['Elements']:
                numberOfWQESummary.setdefault(elem['Status'], 0)
                numberOfWQESummary[elem['Status']] += 1

                numberOfJobsSummary.setdefault(elem['Status'], 0)
                numberOfJobsSummary[elem['Status']] += elem['Jobs']

        if status:
            numberOfWQESummary = {status: numberOfWQESummary.get(status, 0)}
            numberOfJobsSummary = {status: numberOfJobsSummary.get(status, 0)}

        return numberOfWQESummary, numberOfJobsSummary

    def getGlobalSiteStatusSummary(self, status=None):
        """
        Goes through all the request/workqueue elements that are active
        and print a total overview of:
         *) unique top level jobs per site and per status and (distributed)
         *) possible top level jobs per site and per status (maxxed)

        If status is provided, then skip any workqueue element not in the
        given status. Otherwise filter only active workqueue status.
        """
        activeStatus = ['Available', 'Negotiating', 'Acquired', 'Running']

        uniqueJobsSummary = {}
        possibleJobsSummary = {}
        for st in activeStatus:
            uniqueJobsSummary.setdefault(st, {})
            possibleJobsSummary.setdefault(st, {})

        for reqName in self.wqResultsByRequest:
            for elem in self.wqResultsByRequest[reqName]['Elements']:
                if elem['Status'] not in activeStatus:
                    continue

                possibleSites = elem.possibleSites()
                try:
                    jobsPerSite = int(elem['Jobs']/len(possibleSites))
                except ZeroDivisionError:
                    possibleSites = ['None']
                    jobsPerSite = elem['Jobs']

                for site in possibleSites:
                    uniqueJobsSummary[elem['Status']].setdefault(site, 0)
                    possibleJobsSummary[elem['Status']].setdefault(site, 0)

                    uniqueJobsSummary[elem['Status']][site] += jobsPerSite
                    possibleJobsSummary[elem['Status']][site] += elem['Jobs']

        if status:
            uniqueJobsSummary = {status: uniqueJobsSummary.get(status, 0)}
            possibleJobsSummary = {status: possibleJobsSummary.get(status, 0)}

        return uniqueJobsSummary, possibleJobsSummary

    def printSummary(self, request, detail=False):
        
        wqResult = self.getWQElementResultsByRequest(request)
        
        if wqResult is None:
            print("No WQ element exist for the status given")
            return
        #pprint(elements)
        print("### summary for %s ###" % request )
        print("  Priority: %s, available elements: %s " % (wqResult["Priority"], len(wqResult['Elements'])))
        
        sites = self.getPossibleSitesByRequest(request)
        print("  Possible sites to run: %s" % list(sites))
        
        higher = self.elementsWithHigherPriorityInSameSites(request)
        total = 0
        totalJobs = 0
        for request in higher:
            wqResult = higher[request]
            availableEle = wqResult.availableItems()
            if not availableEle:
                continue
            total += len(availableEle)
            wqAvailResult = WorkQueueElementResult(Elements=availableEle)
            totalJobs += wqAvailResult['Jobs']
            maxJobEle = wqAvailResult.getMaxJobElement()
            minJobEle = wqAvailResult.getMinJobElement()
            if detail:
                print("  Higher priority elements by request:")
                print("""%s: Priority: %s, available elements: %s, total jobs: %s,
                        max jobs: %s (element_id: %s), min jobs: %s (element_id: %s)""" % (
                        request, wqAvailResult["Priority"], availableEle, wqAvailResult['Jobs'],
                        maxJobEle["Jobs"], maxJobEle.id,
                        minJobEle["Jobs"], minJobEle.id))
        print("  Higher priority elements: %s, total available jobs: %s"  % (total, totalJobs))

