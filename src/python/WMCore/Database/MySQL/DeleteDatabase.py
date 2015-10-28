#!/usr/bin/env python
#-*- coding: utf-8 -*-
#pylint: disable=
"""
File       : DeleteDatabase.py
Author     : Valentin Kuznetsov <vkuznet AT gmail dot com>
Description: Implementation of DeleteDatabase class for MySQL
"""

from __future__ import print_function, division

# CMS modules
from WMCore.Database.DBFormatter import DBFormatter

class DeleteDatabase(DBFormatter):

    def execute(self, dbName = None, subscription = None, conn = None):
        """Destroy database"""
        # if dbname is not given we'll lookup current database name
        if not dbName:
            sql = """SELECT DATABASE() AS dbname"""
            results = self.dbi.processData(sql, {}, conn = conn)
            dbName = self.formatDict(results)[0]['dbname']
        # if no database found, e.g. we connected to MySQL but not to specific database
        # we exit, nothing to delete
        if dbName == None or dbName == 'None':
            return
        # check among list of database if dbName is present
        sql = """SHOW DATABASES"""
        results = self.dbi.processData(sql, {}, conn = conn)
        found = False
        for row in self.formatDict(results):
            if row['database'] == dbName:
                found = True
                break
        # if dbName is found we can delete it
        if found:
            sql = """DROP DATABASE %s""" % dbName
            self.dbi.processData(sql, {}, conn = conn)
