import logging

class TestRegistryFile1:
    def __init__(self, **parameters):
        logging.debug("Instantitating object with parameters:")
        logging.debug(str(parameters))

    def doSomething(self, parameters = {}):
        logging.debug("TestCollection_3.TestRegistryFile1 is doing something")
        logging.debug("With parameters: "+str(parameters))
        return parameters
