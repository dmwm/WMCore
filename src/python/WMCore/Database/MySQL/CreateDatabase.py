#!/usr/bin/env python
#-*- coding: utf-8 -*-
#pylint: disable=
"""
File       : CreateDatabase.py
Author     : Valentin Kuznetsov <vkuznet AT gmail dot com>
Description: Implementation of CreateDatabase for MySQL
"""

# CMS modules
from WMCore.Database.DBFormatter import DBFormatter

class CreateDatabase(DBFormatter):

    def execute(self, dbName, conn = None):
        """Execute create statement"""
        # check among list of database if dbName is present
        sql = """SHOW DATABASES"""
        results = self.dbi.processData(sql, {}, conn = conn)
        found = False
        for row in self.formatDict(results):
            if row['database'] == dbName:
                found = True
                break
        if  not found:
            try:
                sql = """CREATE DATABASE %s""" % dbName
                self.dbi.processData(sql, {}, conn = conn)
            except Exception as exp:
                print("Create database: %s" % str(exp))
                raise exp
        sql = """USE %s""" % dbName
        self.dbi.processData(sql, {}, conn = conn)
