#!/usr/bin/env python
# encoding: utf-8
"""
Group.py

Created by Dave Evans on 2010-07-14.
Copyright (c) 2010 Fermilab. All rights reserved.
"""



import json

from WMCore.GroupUser.CouchObject import CouchObject


class Group(CouchObject):
    """
    _Group_

    Dictionary object containing attributes of a group

    """
    def __init__(self, **options):
        CouchObject.__init__(self)
        self.cdb_document_data = "group"
        self.setdefault('name', None)
        self.setdefault('administrators', [])
        self.setdefault('associated_sites', {})
        self.update(options)

    document_id = property(lambda x : "group-%s" % x['name'] )
    name = property(lambda x: x['name'])
