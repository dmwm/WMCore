#!/usr/bin/env python
# encoding: utf-8
"""
ConfigUpload.py

Created by Dave Evans on 2010-03-27.
Copyright (c) 2010 Fermilab. All rights reserved.
"""


import os
import subprocess

from PSetTweaks.WMTweak import makeTweak
from WMCore.Cache.ConfigCache import WMConfigCache

def edmConfigHash(inputFile):
    """
    _edmConfigHash_
    
    Run edmConfigHash binary to generate a PSetHash
    input file needs to be an open-for-reading file handle
    
    """
    proc = subprocess.Popen(
       ["edmConfigHash"], shell=True, cwd=os.getcwd(),
       stdout=subprocess.PIPE,
       stderr=subprocess.PIPE,
       stdin=inputFile
    )
    stdout, stderr = proc.communicate()
    if proc.returncode == 0:
        hashData = str(stdout).strip()
        return hashData
    msg = "Error running edmConfigHash:\n"
    msg += stdout
    msg += "\n"
    msg += stderr
    raise RuntimeError(msg)


def upload(process):
    """
    _upload_
    
    Import this method from a PSet python main function and call it and your config will get 
    uploaded to a ConfigCache
    
    Note that this is expected to be run within the SCRAM runtime environment, and also have 
    WMCore in the PYTHONPATH
    Dependency on httplib2 can be handled by dropping the httplib2 egg on the python path as well

    # may be useful to do something like this in the PSet.py for the upload
    #sys.path.append('/home/evansde/work/CMSSW/CMSSW_3_7_1/WMCORE/src/python/')
    #sys.path.append(
        '/home/evansde/work/CMSSW/CMSSW_3_7_1/WMCORE/src/python/httplib2-0.6.0-py2.6.egg')
    #TODO: Better way to handle the couch password needed here...
    
    """
    handle = open("PSet.py", 'w')
    handle.write(process.dumpPython())
    handle.close()
    hashval = edmConfigHash(open("PSet.py", 'r'))

    # set up the cache API
    cache = WMConfigCache(
        "config_cache1",
        "http://dmwmwriter:PASSWORD@cmssrv52.fnal.gov:5984")
    # add the config
    doc, rev = cache.addConfig("PSet.py")
    # add the PSet Tweak
    cache.addTweak(doc, rev, makeTweak(process).jsondictionary())
    # add the PSet Hash
    cache.modifyHash(doc, hashval)



