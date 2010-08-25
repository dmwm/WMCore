#!/usr/bin/env python
"""
_DbObject_

Base class for all objects in the database
"""

__version__ = "$Id: DbObject.py,v 1.8 2010/05/10 12:44:37 spigafi Exp $"
__revision__ = "$Revision: 1.8 $"

class DbObject(object):
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
 
    def __init__(self, parameters = None):
        """
        initialize a DbObject instance
        """

        # WMConnectionBase.__init__(self, daoPackage = "WMCore.BossLite")

        # dictionary used to store information about the job
        self.data = {}
        self.existsInDataBase = False

        # set class name and exception type
        self.className = self.__class__.__name__
        self.exception = self.__class__.exception

        # get job fields
        fields = self.__class__.mapping.keys()

        if not parameters :
            parameters = {}
            
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

    def save(self, db, deep = True):
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

    def create(self, db):
        """
        update object into database
        """
        
        raise NotImplementedError

    ##########################################################################

    def exists(self, db, noDB = False):
        """
        check if the object exists inside the database
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

    
class DbObjectDBFormatter(object):
    """
    DbObjectDBFormatter
    """
    
    def __init__(self):
        """
        __init__
        """

    def preFormat(self, res):
        """
        Before save the object inside the DB, it formats the data to store
        correctly the information
        """
        
        raise NotImplementedError
    
    def postFormat(self, res):
        """
        After a DB extraction, it formats the data before save the information 
        inside the Python object
        """
        
        raise NotImplementedError