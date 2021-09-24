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

    def __init__(self, jobid=-1):
        """
        Just make sure you init the dictionary fields.

        If the field has no value, leave it as None so we can
        overwrite it later.
        """

        self.setdefault('id', None)
        self.setdefault('jobid', jobid)
        self.setdefault('gridid', None)
        self.setdefault('bulkid', None)
        self.setdefault('retry_count', 0)
        self.setdefault('status', None)
        self.setdefault('location', None)
        self.setdefault('site_cms_name', None)
        self.setdefault('userdn', None)
        self.setdefault('usergroup', '')
        self.setdefault('userrole', '')
        self.setdefault('plugin', None)
        self.setdefault('cache_dir', None)
        self.setdefault('status_time', None)
        self.setdefault('packageDir', None)
        self.setdefault('sandbox', None)
        self.setdefault('wf_priority', None)
        self.setdefault('task_type', None)
        self.setdefault('possibleSites', None)
        self.setdefault('swVersion', None)
        self.setdefault('scramArch', None)
        self.setdefault('siteName', None)
        self.setdefault('name', None)
        self.setdefault('proxyPath', None)
        self.setdefault('request_name', None)
        self.setdefault('estimatedJobTime', None)
        self.setdefault('estimatedDiskUsage', None)
        self.setdefault('estimatedMemoryUsage', None)
        self.setdefault('numberOfCores', 1)
        self.setdefault('taskPriority', None)
        self.setdefault('task_name', None)
        self.setdefault('task_id', None)
        self.setdefault('potentialSites', None)
        self.setdefault('inputDataset', None)
        self.setdefault('inputDatasetLocations', None)
        self.setdefault('inputPileup', None)
        self.setdefault('allowOpportunistic', False)
        self.setdefault('activity', None)
        self.setdefault('requiresGPU', 'forbidden')
        self.setdefault('gpuRequirements', None)
        self.setdefault('requestType', None)

        return

    def buildFromJob(self, job):
        """
        _buildFromJob_

        Build a RunJob from a WMBS Job
        """

        # These two are required
        self['jobid'] = job.get('id', None)
        self['retry_count'] = job.get('retry_count', None)
        self['userdn'] = job.get('owner', None)
        self['usergroup'] = job.get('usergroup', '')
        self['userrole'] = job.get('userrole', '')
        self['siteName'] = job.get('custom', {}).get('location', None)

        # Update the job with all other shared keys
        for key in job:
            if key in self:
                self[key] = job[key]

        return

    def buildWMBSJob(self):
        """
        _buildWMBSJob_

        Does exactly what it sounds like

        Also, attach couch_record (since we usually need one)
        """

        job = Job(id=self['jobid'])
        job['retry_count'] = self['retry_count']
        job['couch_record'] = None
        job['owner'] = self['userdn']
        job['usergroup'] = self['usergroup']
        job['userrole'] = self['userrole']

        for key in self:
            if key != 'id':
                job[key] = self[key]

        return job
