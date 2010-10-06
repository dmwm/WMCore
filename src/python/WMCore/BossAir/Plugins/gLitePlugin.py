#!/usr/bin/env python

"""
gLite Plugin


"""

import logging
import subprocess
import multiprocessing


from WMCore.BossAir.Plugins.BasePlugin import BasePlugin


def outputWorker(jobID):
    """
    _outputWorker_

    Runs a subprocessed command.

    This takes whatever you send it (a single ID)
    executes the command
    and then returns the stdout result

    I planned this to do a glite-job-output command
    in massive parallel, possibly using the bulkID
    instead of the gridID.  Either way, all you have
    to change is the command here, and what is send in
    in the complete() function.
    """


    command = "echo %s" % (jobID)
    pipe = subprocess.Popen(command, stdout = subprocess.PIPE,
                            stderr = subprocess.PIPE, shell = True)

    stdout, stderr = pipe.communicate()

    return stdout





class gLitePlugin:
    """
    Prototype for gLite Plugin

    Written so I can put the multiprocessing pool somewhere
    """



    def __init__(self, config):


        self.config = config

        # These are just the MANDATORY states
        self.states = ['New', 'Timeout']

        self.stateMap = []

        # These are the pool settings.
        # I'm not sure what chunksize will buy you here, probably
        # nothing if you don't have long lists of jobs
        nProcess       = getattr(config.BossAir, 'gLiteProcesses', 10)
        self.chunksize = getattr(config.BossAir, 'gLiteChunksize', 2)

        
        self.pool = multiprocessing.Pool(processes = nProcess)

        return



    def submit(self, jobs, info = None):
        """
        _submit_
        
        Submits jobs
        """

        return


    def track(self, jobs):
        """
        _track_
        
        Tracks jobs
        Returns three lists:
        1) the running jobs
        2) the jobs that need to be updated in the DB
        3) the complete jobs
        """

        return jobs, jobs, []


    def complete(self, jobs):
        """
        _complete_

        Run any complete code
        """
        # Run your command in parallel
        # This sends the outputWorker function
        # Whatever's in gridid as an argument
        # And at the end waits for a return

        # NOTE: This is a blocking function


        input = [x['gridid'] for x in jobs]

        results = self.pool.map(outputWorker, input,
                                chunksize = self.chunksize)

        return results



    def kill(self, jobs):
        """
        _kill_
        
        Kill any and all jobs
        """


        return
