#!/usr/bin/env python
# encoding: utf-8
"""
Service.py

Created by Dave Evans on 2010-03-02.
Copyright (c) 2010 Fermilab. All rights reserved.
"""

import sys
import os

class Service(object):
    """
    docstring for Service
    
    API interface definition for an ACDC Service implementation
    
    """
    def __init__(self, **options):
        super(Service, self).__init__()
        self.options = {}
        self.options.update(options)
        
    def listOwners(self):
        """
        _listOwners_
        
        return a list of all Owners in the backend
        """
        msg = "%s.listOwners is not implemented" % self.__class__.__name__
        raise NotImplementedError(msg)
        

    def listCollections(self, ownerInstance):
        """
        _listCollections_
        
        List collections for the Owner instance provided
        
        """
        msg = "%s.listCollections is not implemented" % self.__class__.__name__
        raise NotImplementedError(msg) 
        
    def listFilesets(self, collectionInstance):
        """
        _listFilesets_

        List filesets for the collection instance provided

        """
        msg = "%s.listFilesets is not implemented" % self.__class__.__name__
        raise NotImplementedError(msg)
        
