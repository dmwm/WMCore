#!/usr/bin/env python
"""
_RunAndLumiSeeder_


"""
from WMCore.JobSplitting.Generators.GeneratorInterface import GeneratorInterface

class RunAndLumiSeeder(GeneratorInterface):

    def __init__(self, **options):
        self.initialRun    = int(options.get("initial_run",   1))
        self.runIncrement  = int(options.get("run_increment", 1))
        self.initialLumi   = int(options.get("initial_lumi",  0))
        self.lumiIncrement = int(options.get("lumi_increment",1))
        self.lumiPerRun    = int(options.get("lumi_per_run", 10))

        self.currentRun = self.initialRun
        self.currentLumi = self.initialLumi
        self.lumiCount = 0


    def incrementRun(self):
        self.currentRun += self.runIncrement


    def incrementLumi(self):
        if self.lumiCount == self.lumiPerRun:
            self.currentLumi = 0
            self.lumiCount = 0
            self.incrementRun()
        else:
            self.currentLumi += self.lumiIncrement
            self.lumiCount += 1
        return

    def __call__(self, wmbsJob):

        wmbsJob['mask']['FirstRun'] = self.currentRun
        wmbsJob['mask']['FirstLumi'] = self.currentLumi

        self.incrementLumi()
