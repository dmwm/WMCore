#!/usr/bin/env python
# encoding: utf-8
"""
AutomaticSeeding.py

Created by Dave Evans on 2010-08-30.
Copyright (c) 2010 Fermilab. All rights reserved.

Automatic Seeding, add a flag to the job baggage to tell the runtime scripts to generate seeds on the WN using
the CMSSW RandomSeed Helper
"""

from WMCore.JobSplitting.Generators.GeneratorInterface import GeneratorInterface



class AutomaticSeeding(GeneratorInterface):
    def __init__(self, **options):
        GeneratorInterface.__init__(self, **options)

    def __call__(self, wmbsJob):
        wmbsJob.addBaggageParameter("seeding", self.__class__.__name__)
