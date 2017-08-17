#!/usr/bin/env python
"""
A factory Class that is 'not thread safe' but is intended to work in threads
(no sharing). The class dynamically loads objects from files when needed and
caches them (or not). It is a generalized factory object. If needed this class
can be made threadsafe.
"""

from builtins import object
import threading


class WMFactory(object):
    """
    A factory Class that is 'not thread safe' but is intended to work in
    threads (no sharing). The class dynamically loads objects from files
    when needed and caches them.
    """

    def __init__(self, name, namespace=''):
        """
        Initializes the factory, and checks if this thread already
        has an attribute for storing registries. It uses the reserved
        'registries' attribute in the thread.
        """
        self.namespace = namespace
        self.objectList = {}

        myThread = threading.currentThread()
        if not hasattr(myThread, "factory"):
            myThread.factory = {}
        myThread.factory[name] = self

    def loadObject(self, classname, args=None, storeInCache=True,
                   getFromCache=True, listFlag=False, alteredClassName=None):
        """
        Dynamically loads the object from file.
        For this to work the class name has to
        be the same as the file name (minus the .py)

        Be default objects are loaded from cache. However if you
        want several different instances of the same object in one
        thread, you set cache to False.
        """
        if getFromCache:
            if classname in self.objectList:
                return self.objectList[classname]

        if self.namespace == '':
            module = classname
            # FIXME: hoky way of doing this! Change this please!
            errModule = classname
        else:
            module = "%s.%s" % (self.namespace, classname)
            errModule = "%s.%s" % (self.namespace, classname)
        if alteredClassName:
            classname = alteredClassName
        module = __import__(module, globals(), locals(), [classname])
        obj = getattr(module, classname.split('.')[-1])
        if args is None:
            classinstance = obj()
        else:
            # This handles the passing of list-style arguments instead of dicts
            # Primarily for setting the schema
            # Or anywhere you need arguments of the form (a,b,c,...)
            if isinstance(args, list) and listFlag:
                classinstance = obj(*args)
            elif isinstance(args, dict):
                classinstance = obj(**args)
            else:
                # But if you actually need to pass a list, better do it the old fashioned way
                classinstance = obj(args)
        if storeInCache:
            self.objectList[classname] = classinstance

        return classinstance
