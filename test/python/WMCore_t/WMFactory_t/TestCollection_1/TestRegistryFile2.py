from builtins import object
import logging

class TestRegistryFile2(object):


    def doSomething(self, parameters = {}):
        logging.debug("TestCollection_1.TestRegistryFile2 is doing something")
        logging.debug("With parameters: "+str(parameters))
        return parameters
