"""
Common module for helper methods, Classes for RequestManager related unittests.

""" 

import os
import urllib

from WMCore.WMSpec.StdSpecs.ReReco import ReRecoWorkloadFactory

    
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
    schema = ReRecoWorkloadFactory.getTestArguments()
    schema['RequestName'] = 'TestReReco'
    schema['RequestType'] = 'ReReco'
    schema['Requestor'] = '%s' % userName
    schema['Group'] = '%s' % groupName
    return schema

def getResubmissionSchema(originalRequest, initialTask,
                          groupName = 'PeopleLikeMe', userName = 'me'):
    schema = {}
    schema['RequestName'] = 'TestResubmission'
    schema['RequestType'] = 'Resubmission'
    schema['Requestor'] = '%s' % userName
    schema['Group'] = '%s' % groupName
    schema['TimePerEvent'] = '12'
    schema['Memory'] = 3000
    schema['SizePerEvent'] = 512
    schema['OriginalRequestName'] = originalRequest
    schema['InitialTaskPath'] = initialTask
    schema['ACDCServer'] = os.environ['COUCHURL']
    schema['ACDCDatabase'] = 'bogus'
    return schema
