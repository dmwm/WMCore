#!/usr/bin/env python



"""
_RunJob_

The runJob class object.
It is very simple.
"""



class RunJob(dict):
    """
    _RunJob_

    Basically, create an organized dictionary with all
    the necessary fields.
    """

    def __init__(self, id = -1, jobid = -1, gridid = -1, bulkid = -1, retry_count = 0, status = None):
        """
        Just make sure you init the dictionary fields

        """

        self.setdefault('id', id)
        self.setdefault('jobid', jobid)
        self.setdefault('gridid', gridid)
        self.setdefault('bulkid', bulkid)
        self.setdefault('retry_count', retry_count)
        self.setdefault('status', status)


        return


    def buildFromJob(self, job):
        """
        _buildFromJob_
        
        Build a RunJob from a WMBS Job
        """

        self['jobid']       = job.get('id', None)
        self['retry_count'] = job.get('retry_count', None)

        return
        
