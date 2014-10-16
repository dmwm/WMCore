#!/usr/bin/env python



"""
_RunJob_

The runJob class object.
It is very simple.
"""

from WMCore.WMBS.Job import Job

class RunJob(dict):
    """
    _RunJob_

    Basically, create an organized dictionary with all
    the necessary fields.
    """

    def __init__(self, id = None, jobid = -1, gridid = None,
                 bulkid = None, retry_count = 0, status = None,
                 location = None, userdn = None, usergroup = '',
                 userrole = '', plugin = None,
                 cache_dir = None, status_time = None, packageDir = None,
                 sandbox = None, priority = None, site_cms_name = None,
                 taskType = None, possibleSites = None, sw_version = None,
                 scram_arch = None, siteName = None, jobName = None,
                 proxyPath = None, requestName = None, jobTime = None,
                 diskUsage = None, memoryUsage = None, taskPriority = None,
                 taskName = None, taskID = None, potentialSites = None,
                 numberOfCores = 1, inputDataset = None, inputDatasetLocations = None
                ):
        """
        Just make sure you init the dictionary fields.

        If the field has no value, leave it as NONE so we can
        overwrite it later.

        """

        self.setdefault('id', id)
        self.setdefault('jobid', jobid)
        self.setdefault('gridid', gridid)
        self.setdefault('bulkid', bulkid)
        self.setdefault('retry_count', retry_count)
        self.setdefault('status', status)
        self.setdefault('location', location)
        self.setdefault('site_cms_name', site_cms_name)
        self.setdefault('userdn', userdn)
        self.setdefault('usergroup', usergroup)
        self.setdefault('userrole', userrole)
        self.setdefault('plugin', plugin)
        self.setdefault('cache_dir', cache_dir)
        self.setdefault('status_time', status_time)
        self.setdefault('packageDir', packageDir)
        self.setdefault('sandbox', sandbox)
        self.setdefault('priority', priority)
        self.setdefault('taskType', taskType)
        self.setdefault('possibleSites', possibleSites)
        self.setdefault('swVersion', sw_version)
        self.setdefault('scramArch', scram_arch)
        self.setdefault('siteName', siteName)
        self.setdefault('name', jobName)
        self.setdefault('proxyPath', proxyPath)
        self.setdefault('requestName', requestName)
        self.setdefault('estimatedJobTime', jobTime)
        self.setdefault('estimatedDiskUsage', diskUsage)
        self.setdefault('estimatedMemoryUsage', memoryUsage)
        self.setdefault('numberOfCores', numberOfCores)
        self.setdefault('taskPriority', taskPriority)
        self.setdefault('taskName', taskName)
        self.setdefault('taskID', taskID)
        self.setdefault('potentialSites', potentialSites)
        self.setdefault('inputDataset', inputDataset)
        self.setdefault('inputDatasetLocations', inputDatasetLocations)

        return


    def buildFromJob(self, job):
        """
        _buildFromJob_

        Build a RunJob from a WMBS Job
        """


        # These two are required
        self['jobid']       = job.get('id', None)
        self['retry_count'] = job.get('retry_count', None)
        self['userdn']      = job.get('owner', None)
        self['usergroup']      = job.get('usergroup', '')
        self['userrole']      = job.get('userrole', '')
        self['siteName']    = job.get('custom', {}).get('location', None)

        # Update the job with all other shared keys
        for key in job.keys():
            if key in self.keys():
                self[key] = job[key]

        return



    def buildWMBSJob(self):
        """
        _buildWMBSJob_

        Does exactly what it sounds like

        Also, attach couch_record (since we usually need one)
        """


        job                 = Job(id = self['jobid'])
        job['retry_count']  = self['retry_count']
        job['couch_record'] = None
        job['owner']        = self['userdn']
        job['usergroup']      = self['usergroup']
        job['userrole']      = self['userrole']

        for key in self.keys():
            if key != 'id':
                job[key] = self[key]


        return job
