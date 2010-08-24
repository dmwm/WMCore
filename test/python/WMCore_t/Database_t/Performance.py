#!/usr/bin/env python

import logging, time

class Performance():
    """
    __Performance__

    Base class for Database Performance Tests 

    This class is abstract, serving as a superclass for all
    DB Performance Testcases


    """

    def setUp(self):
        """
        Common setUp for all Performance tests

        """
        
        #Setting up logger
        logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s',
                    datefmt='%m-%d %H:%M',
                    filename='%s.log' % __file__.replace('.py',''),
                    filemode='w')
        self.logger = logging.getLogger('BasePerformanceTest')

    def tearDown(self):
        #Base tearDown method for the DB Performance test
        pass

    def formatExecInput(self, input):
        """
        Method that format the string received as an argument
        for the execute method of each DAO class.

        """
        fmtstring = ""
        for i in input.keys():
            if type(input[i]) == type('string'):
                fmtstring = "%s %s='%s'," % (fmtstring, i, input[i])
            else:
                fmtstring = "%s %s=%s," % (fmtstring, i, input[i])
        fmtstring = fmtstring.strip()
        fmtstring = fmtstring.rstrip(",")
        return fmtstring

    def perfTest(self, dao, action, times=1, **input):
        """
        Method that executes a dao class operation and measures its
        execution time.
        
        """
        #Test each of the DAO classes of the specific WMBS class directory        
        action = dao(classname=action)
        string = self.formatExecInput(input=input)        
        string = "action.execute(%s)" % string
        self.logger.debug('the final string: %s' % string)
        diffTotal = 0

        for i in range(0,times):
            #Performance testing block START        
            startTime = time.time()               
            #Place execute method of the specific classname here
            #string = compile(string)
            eval(string)
            endTime = time.time()
            diffTime = endTime - startTime
            #Performance testing block END        
            diffTotal = diffTotal + diffTime
        if self.verbose == 'True':
            print string + " - " + str(times) +" time(s) " 
        return diffTotal
