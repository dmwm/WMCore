#!/usr/bin/env python
"""
_DbObject_

Base class for all objects in the database
"""

__version__ = "$Id: DbObject.py,v 1.2 2010/03/30 15:18:43 mnorman Exp $"
__revision__ = "$Revision: 1.2 $"

import logging
import traceback
import threading

from WMCore.WMConnectionBase import WMConnectionBase

def dbTransaction(func):
    """
    Basic transaction decorator function
    """
    def wrapper(self, *args, **kwargs):
        self.existingTransaction = self.beginTransaction()
        try:
            res = func(self, *args, **kwargs)
            self.commitTransaction(self.existingTransaction)
        except Exception, ex:
            msg = "Failure in DbObject method"
            msg += str(ex)
            msg += str(traceback.format_exc())
            logging.error(msg)
            #TODO: Add this to WMConnectionBase?
            myThread = threading.currentThread()
            myThread.transaction.rollback()
            raise Exception(msg)        
        return res
    return wrapper

class DbObject(WMConnectionBase):
    """
    Superclass of all objects that can be stored in the database.
    """

    # mapping between field names and database fields
    mapping = {}

    # default values for fields
    defaults = {}

    # non database fields
    private = {}

    # database properties
    tableName = ""
    tableIndex = []

    # exception class
    exception = Exception

    ##########################################################################
 
    def __init__(self, parameters = {}):
        """
        initialize a DbObject instance
        """

        WMConnectionBase.__init__(self, daoPackage = "WMCore.BossLite")

        # dictionary used to store information about the job
        self.data = {}
        self.existsInDataBase = False

        # set class name and exception type
        self.className = self.__class__.__name__
        self.exception = self.__class__.exception

        # get job fields
        fields = self.__class__.mapping.keys()

        # assign parameters
        for key in parameters.keys():

            # check if it is a valid parameter
            if key in fields:

                # store it using the mapped parameter
                self.data[key] = parameters[key]

            # no, signal error
            else:
                raise self.exception("Unknown field %s in %s creation" % \
                                     (key, self.className))

            fields.remove(key)

        # add default values for other parameters
        for key in fields:
            self.data[key] = self.__class__.defaults[key]

        # add private data
        self.privateData = self.__class__.private



    ##########################################################################

    def __getitem__(self, field):
        """
        return one of the fields (in a dictionary form)
        """

        # get mapped field name
        if field in self.data.keys():
            return self.data[field]

        # get private data
        if field in self.privateData.keys():
            return self.privateData[field]

        # not there
        raise self.exception("Unknown field %s in %s object" % \
                             (field, self.className))

    ##########################################################################

    def __setitem__(self, field, value):
        """
        set one of the fields (in a dictionary form)
        """

        # set mapped field name
        if field in self.data.keys():
            self.data[field] = value
            return

        # set private data
        if field in self.privateData.keys():
            self.privateData[field] = value
            return

        # not there
        raise self.exception("Unknown field %s in %s object" % \
                             (field, self.className))

    ##########################################################################

    #def update(self, parameters):
    #    """
    #    updates the fields with new values
    #    """

    #    # assign parameters
    #    for key in parameters.keys():

    #        # check if it is a valid parameter
    #        if key in self.data.keys():

    #            # store it using the mapped parameter
    #            self.data[key] = parameters[key]

    #        # no, signal error
    #        else:
    #            raise self.exception("Unknown field %s in %s update" % \
    #                                 (key, self.className))

    ##########################################################################

    def valid(self, fields):
        """
        verify that fields are set
        """

        # verify data is complete
        for key in fields:
            try:
                if self.data[key] is None:
                    return False
            except KeyError:
                raise self.exception("Unknown field %s in %s verification" %\
                                     (key, self.className))

        # return true
        return True

    ##########################################################################

    def save(self, db):
        """
        save object into database
        """
        raise NotImplementedError

    ##########################################################################

    def load(self, db, deep = True):
        """
        load object from database
        """
        raise NotImplementedError

    ##########################################################################

    def remove(self, db):
        """
        remove object from database
        """
        raise NotImplementedError

    ##########################################################################

    def update(self, db, deep = True):
        """
        update object in database
        """
        raise NotImplementedError


    ##########################################################################

    def __str__(self):
        """
        return a printed representation of the task
        """

        # get field names
        fields = self.data.keys()
        fields.sort()

        # show id first
        string = "%s instance %s\n" % (self.className, self.data['id'])
        fields.remove('id')

        # add the other fields
        for key in fields:
            string += "  %s : %s\n" % (str(key), str(self.data[key]))

        # return it
        return string

