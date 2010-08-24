#!/usr/bin/env python
"""
A factory Class that is 'not thread safe' but is intended to work in threads
(no sharing). The class dynamically loads objects from files when needed and 
caches them (or not). It is a generalized factory object. If needed this class
can be made threadsafe.
"""

__revision__ = "$Id: WMFactory.py,v 1.6 2008/09/30 18:25:38 fvlingen Exp $"
__version__ = "$Revision: 1.6 $"
__author__ = "fvlingen@caltech.edu"

#FIXME: need a test for this. But this can only be done if 
# ../test/python/WMCORE is moved to ../test/python/WMCORE_t 
#: to prevent similar inclusion roots for the pythonpath.

import logging
import threading

from WMCore.WMException import WMException
from WMCore.WMExceptions import WMEXCEPTION 

class WMFactory:
    """
    A factory Class that is 'not thread safe' but is intended to work in 
    threads (no sharing). The class dynamically loads objects from files 
    when needed and caches them.
    """

    def __init__(self, name, namespace = ''):
        """
        Initializes the factory, and checks if this thread already
        has an attribute for storing registries. It uses the reserved 
        'registries' attribute in the thread.
        """
        self.namespace = namespace 
        self.objectList = {}
        msg = """
Creating factory with name: %s associated to 
namespace (package): %s """ % (name, str(namespace))
        logging.debug(msg)
        myThread = threading.currentThread()
        if not hasattr(myThread, "factory"):
            myThread.factory = {}
        myThread.factory[name] = self 

    def loadObject(self, classname, args = None, storeInCache = True, \
        getFromCache = False):
        """
        Dynamically loads the object from file.        
        For this to work the class name has to 
        be the same as the file name (minus the .py)
        
        Be default objects are loaded from cache. However if you 
        want several different instances of the same object in one
        thread, you set cache to False.
        """
        if getFromCache:
            logging.debug("Check cache")
            if self.objectList.has_key(classname):
                logging.debug("Object in cache")
                return self.objectList[classname]
            logging.debug("Not in cache")
        logging.debug("Searching class name: "+ classname)
        try:
            # check if we need to include the namespace 
            if self.namespace == '':
                module = classname
            else:
                module = "%s.%s" % (self.namespace, classname)
            logging.debug("Trying to load: "+module)
            module = __import__(module, globals(), locals(), [classname])
            obj = getattr(module, classname.split('.')[-1])
            if args == None:
                classinstance = obj()
            else:  
                classinstance = obj(args)
            if storeInCache:
                self.objectList[classname] = classinstance
            logging.debug("Created instance for class: "+classname)
            return classinstance
        except Exception,ex:
            raise WMException(WMEXCEPTION['WMCORE-4']+' '+classname +':'+ str(ex), 'WMCORE-4')
