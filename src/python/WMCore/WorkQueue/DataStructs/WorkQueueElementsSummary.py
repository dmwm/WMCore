"""
WorkQueueElementsSummary

"""
from __future__ import (print_function, division)

from builtins import str, bytes, object
from future.utils import viewitems

from collections import defaultdict
from math import ceil

from WMCore.WorkQueue.DataStructs.WorkQueueElement import possibleSites
from WMCore.WorkQueue.DataStructs.WorkQueueElementResult import WorkQueueElementResult


def getGlobalSiteStatusSummary(elements, status=None, dataLocality=False):
    """
    _getGlobalSiteStatusSummary_

    Given a dict with workqueue elements keyed by status, such as this format:
    {u'Canceled': [{u'Inputs': {}, u'Jobs': 18,...}, {u'Jobs': 11,...}],
     u'Running': [{'Priority': 190000,..}, ...]}

    Creates a summary of jobs and number of wq elements in each status
    distributed among the sites whitelisted. There are 2 job distribution:
     *) unique top level jobs per site and per status and (equally
        distributed among all sites)
     *) possible top level jobs per site and per status (consider all
        jobs can run in a single location)

    If status is provided, then skip any workqueue element not in the
    given status. Otherwise filter only active workqueue status.

    If dataLocality is set to True, then it considers only sites that pass
    the data location constraint.
    """
    if status and isinstance(status, (str, bytes)):
        activeStatus = [status]
    elif status and isinstance(status, (list, tuple)):
        activeStatus = status
    else:
        activeStatus = list(elements)

    uniqueJobsSummary = {}
    possibleJobsSummary = {}

    for st in activeStatus:
        uniqueJobsSummary.setdefault(st, {})
        possibleJobsSummary.setdefault(st, {})
        uniqueJobs = {}
        possibleJobs = {}
        for elem in elements.get(st, []):
            elem = elem['WMCore.WorkQueue.DataStructs.WorkQueueElement.WorkQueueElement']
            if dataLocality:
                commonSites = possibleSites(elem)
            else:
                commonSites = list(set(elem['SiteWhitelist']) - set(elem['SiteBlacklist']))

            try:
                jobsPerSite = elem['Jobs'] / len(commonSites)
            except ZeroDivisionError:
                commonSites = ['NoPossibleSite']
                jobsPerSite = elem['Jobs']

            for site in commonSites:
                uniqueJobs.setdefault(site, {'sum_jobs': 0, 'num_elem': 0})
                possibleJobs.setdefault(site, {'sum_jobs': 0, 'num_elem': 0})

                uniqueJobs[site]['sum_jobs'] += ceil(jobsPerSite)
                uniqueJobs[site]['num_elem'] += 1
                possibleJobs[site]['sum_jobs'] += ceil(elem['Jobs'])
                possibleJobs[site]['num_elem'] += 1

        uniqueJobsSummary[st].update(uniqueJobs)
        possibleJobsSummary[st].update(possibleJobs)

    return uniqueJobsSummary, possibleJobsSummary


class WorkQueueElementsSummary(object):
    """Class to hold the status of a related group of WorkQueueElements"""
    def __init__(self, elements):
        self.elements = elements

        elementsByRequest = {}
        for ele in elements:
            elementsByRequest.setdefault(ele["RequestName"], [])
            elementsByRequest[ele["RequestName"]].append(ele)

        self.wqResultsByRequest = {}
        for reqName, wqElements in viewitems(elementsByRequest):
            self.wqResultsByRequest[reqName] = WorkQueueElementResult(Elements=wqElements)

    def elementsWithHigherPriorityInSameSites(self, requestName, returnFormat="dict"):

        if requestName not in self.wqResultsByRequest:
            return None

        priority = self.wqResultsByRequest[requestName]['Priority']
        creationTime = self.wqResultsByRequest[requestName]['Elements'][0]['CreationTime']

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
                        commonSites = possibleSites(element)
                        if len(set(commonSites) & sites) > 0:
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
            sites = sites | set(possibleSites(ele))
        return sites

    def getWQElementResultsByRequest(self, requestName = None):
        if requestName:
            return self.wqResultsByRequest.get(requestName, None)
        else:
            return self.wqResultsByRequest

    def printSummary(self, request, detail=False):

        wqResult = self.getWQElementResultsByRequest(request)

        if wqResult is None:
            print("No WQ element exist for the status given")
            return
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

