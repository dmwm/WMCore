#! /usr/bin/env python

"""
A singleton pattern implemented in python. Adapted from ActiveState Code
Recipe 52558: The Singleton Pattern implemented with Python
http://code.activestate.com/recipes/52558/
"""




class Singleton(object):
    """
    A python singleton
    """


    class SingletonImplementation:
        """
        Implementation of the singleton interface
        """

        def singletonId(self):
            """
            Test method, return singleton id
            """
            return id(self)


    # Storage for the instance reference
    __instance = None

    def __init__(self):
        """
        Create singleton instance
        """
        # Check whether we already have an instance
        if Singleton.__instance is None:
            # Create and remember instance
            Singleton.__instance = Singleton.SingletonImplementation()

        # Store instance reference as the only member in the handle
        self.__dict__['_Singleton__instance'] = Singleton.__instance


    def __getattr__(self, attr):
        """
        Delegate access to implementation
        """
        return getattr(self.__instance, attr)


    def __setattr__(self, attr, value):
        """
        Delegate access to implementation
        """
        return setattr(self.__instance, attr, value)
