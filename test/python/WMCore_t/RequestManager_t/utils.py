"""
Common module for helper methods, Classes for RequestManager related unittests.

""" 

import os
import urllib

import WMCore.WMSpec.StdSpecs.ReReco as ReReco

    
def getAndSetupSchema(testInstance, groupName = 'PeopleLikeMe',
                      userName = 'me', teamName = 'White Sox'):
    """
    Set up a test schema so that we can run a test request.
    The function is shared among RequestManager unittests.
    testInstance is a caller - instances of the current unittest class.
    
    """
    schema = getSchema(groupName = groupName, userName = userName)
    testInstance.jsonSender.put('user/%s?email=me@my.com' % userName)
    testInstance.jsonSender.put('group/%s' % groupName)
    testInstance.jsonSender.put('group/%s/%s' % (groupName, userName))
    testInstance.jsonSender.put(urllib.quote('team/%s' % teamName))
    testInstance.jsonSender.put('version/%s/%s' % (schema["CMSSWVersion"], schema["ScramArch"]))
    return schema


def getSchema(groupName = 'PeopleLikeMe', userName = 'me'):
    schema = ReReco.getTestArguments()
    schema['RequestName'] = 'TestReReco'
    schema['RequestType'] = 'ReReco'
    schema['CmsPath'] = "/uscmst1/prod/sw/cms"
    schema['Requestor'] = '%s' % userName
    schema['Group'] = '%s' % groupName
    schema['CustodialSite'] = 'US_T1_FNAL'
    schema['TimePerEvent'] = '12'
    schema['Memory'] = 3000
    schema['SizePerEvent'] = 512
    return schema