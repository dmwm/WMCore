#!/usr/bin/env python
"""
_BasicCounter_

Count jobs created so far for a task and insert the count into the job

"""
from WMCore.JobSplitting.Generators.GeneratorInterface import GeneratorInterface



class BasicCounter(GeneratorInterface):
    """
    _BasicCounter_

    Incremental job counter

    TODO: Use WMAgent couch to get count for the particular task as starting value in ctor
          Or use options for better control

    """
    def __init__(self, **options):
        GeneratorInterface.__init__(self, **options)
        self.count = 0

    def __call__(self, wmbsJob):


        wmbsJob['counter'] = self.count
        self.count += 1
