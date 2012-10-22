import logging

class TestRegistryFile1:


    def doSomething(self, parameters = {}):
        logging.debug("TestCollection_1.TestRegistryFile1 is doing something")
        logging.debug("With parameters: "+str(parameters))
        return parameters
