#!/usr/bin/python

import sys
import os
import os.path
import inspect
import imp
import unittest

#This class should build a unittest out of a python module in the local directory
#It's weakness is a dependence on grep due to my inability to trace inheritance properly

class Inspect:
    """
    Class for building unittests out of python modules
    """
    def __init__(self):

        #Define random variables
        self.overwrite  = False
        self.writeSetUp = True

        
        if len(sys.argv) < 2:
            self.Usage()
        self.moduleName = sys.argv[-1]
        for i in sys.argv:
            if i == '-overwrite':
                self.overwrite = True
            if i == '-nosetup' or i == '-n':
                self.writeSetUp = False
            if i == '-h' or i == '-help' or i == '--help':
                self.Usage()
        self.GetDefaultDir()
        return

    def Usage(self, errno = 1):
        usageString = """
Correct usage for this function (-h for help)
python2.4 Inspect.py <-overwrite> <-n/-nosetup> <modulename>

Where:
\t<modulename>:  This is actually the name of the module, not the file (i.e. File instead of File.py)
\t<-overwrite>:  Allows generated unittest to overwrite current file with <modulename>_t.py name
\t<-n/-nosetup>: Does not write standard setUp, tearDown functions (which use WMBS format)
        """
        print usageString
        sys.exit(errno)

    def GetDefaultDir(self):
        """
        Grabs the default directories from the PYTHONPATH environment

        """

        dirlist = os.getenv('PYTHONPATH')
        finallist = []

        for dir in dirlist.split(':'):
            finallist.append(dir)

        return finallist

    def FindModuleFilename(self, moduleName, modulePath):
        """
        This function just checks to see if it can find the module given a name and a path
        """
        result = ()
        try:
            result = imp.find_module(moduleName, modulePath)
        except ImportError:
            return None
        return result
        

    def GetListOfMembers(self, moduleName):
        """
        This does all the work of creating a listing of all members of a class.
        Warning: It uses grep to check for non-inherited functions
        """


        modRes = None
        #First check if our module is local
        modRes = self.FindModuleFilename(moduleName, sys.path)
        if modRes == None:
            #If it's not, see if it's in PYTHONPATH
            modRes = self.FindModuleFilename(moduleName, self.GetDefaultDir())
            if modRes == None:
                #Well then it didn't work
                print "I can't find module " + moduleName + " in either the local directory or in $PYTHONPATH"
                sys.exit(100)

        #If we've gotten here, we should have the module location
        NewModule = imp.load_module(moduleName, modRes[0], modRes[1], modRes[2])


        memberList = inspect.getmembers(NewModule)

        #I'm handling this with string functions right now, and it bothers me
        classNames = []
        for member in memberList:
            memberName = member[0]
            memberDoc  = member[1]
            if inspect.isclass(getattr(NewModule, memberName)):
                if inspect.getsourcefile(getattr(NewModule, memberName)) == modRes[1]:
                    classNames.append(memberName)

#        print classNames[0]

        listOfMembers = []

        #Inheritance of functions traced by inspect.getsourcefile()
        for iter_class in classNames:
            newClass = getattr(NewModule, iter_class)
            classMembers = inspect.getmembers(newClass)
            for mem in classMembers:
                memName = mem[0]
                #We do not test functions with names involving double underscores
                #i.e., there will be no unittest for __init__ or __hash__, etc.
                if str(memName).find('__') >= 0:
                    continue
                newMem = getattr(newClass, memName)
                if inspect.ismethod(newMem):
                    if inspect.getsourcefile(newMem):
                        listOfMembers.append(self.Capital(memName))


#                if str(memName).find('__') < 0:
#                    cmd = 'grep -e \"def '+str(memName)+'\" '+str(modRes[1])
#                    out = os.popen(cmd)
#                    res = out.readlines()
#                    out.close()
#                    classRes = getattr(newClass, memName)
#                    if res != ['a'] and hasattr(classRes, '__class__'):
#                        #print inspect.getsourcelines(classRes)
#                        #print classRes.__class__
#                        #print inspect.getsource(classRes)
#                        listOfMembers.append(memName)

        return listOfMembers


    def WriteTest(self, listOfMembers):
        """
        Actually do the work of writing out a file.
        """
        
        #Check if file exists
        filename = self.GetFilename()
        if os.path.isfile(filename):
            print "WARNING: File %s already exists." %(filename)
            if not self.overwrite:
                print "ABORT: Will not overwrite files in standard mode."
                sys.exit(101)
                

        self.WriteHeader()
        self.WriteSetUpTearDown()


        listOfTestFunctions = []

        file = open(filename, 'a')

        for member in listOfMembers:
            string = """
    def test""" + str(member) + """(self):"""
            endstring = """

\treturn



            """
            docstring = '\n\t\"\"\"\n\tThis is the test class for function %s from module %s\n\t\"\"\"'%(str(member), str(self.moduleName))
            file.write(string+docstring+endstring)


        file.close()
        self.WriteClose()

        
        return


    def WriteHeader(self):

        file = open(self.GetFilename(), 'w')

        print "Writing file %s" %(self.GetFilename())

        importstring = """
#!/usr/bin/python

import unittest
import sys
import os
import logging
import threading

from WMQuality.TestInit import TestInit
from WMCore.DAOFactory import DAOFactory
from WMCore.WMFactory import WMFactory

# Framework for this code written automatically by Inspect.py




"""

        classdefString = 'class Test'+self.moduleName+"""(unittest.TestCase):
    _setup = False
    _teardown = False
    """

#        print importstring
#        print classdefString

        file.write(importstring)
        file.write(classdefString)

        file.close()



        return


    def WriteSetUpTearDown(self):
        """
        Writes the setUp and tearDown functions based on the WMBS standard (using their module)
        Can be switched on and off via options.
        """

        setUpString = """
    def setUp(self):
        \"\"\"
        _setUp_

        Setup the database and logging connection.  Try to create all of the
        WMBS tables.  Also add some dummy locations.
        \"\"\"
        if self._setup:
            return

        self.testInit = TestInit(__file__, os.getenv("DIALECT"))
        self.testInit.setLogging()
        self.testInit.setDatabaseConnection()
        self.testInit.setSchema(customModules = ["WMCore.WMBS"],
                                useDefault = False)

        myThread = threading.currentThread()
        daofactory = DAOFactory(package = "WMCore.WMBS",
                                logger = myThread.logger,
                                dbinterface = myThread.dbi)

        locationAction = daofactory(classname = "Locations.New")
        locationAction.execute(sename = "se1.cern.ch")
        locationAction.execute(sename = "se1.fnal.gov")        
        
        self._setup = True


    def tearDown(self):        
        "\"\"
        _tearDown_
        
        Drop all the WMBS tables.
        "\"\"
        myThread = threading.currentThread()
        
        if self._teardown:
            return

        if myThread.transaction == None:
            myThread.transaction = Transaction(self.dbi)
        
        myThread.transaction.begin()

        factory = WMFactory("WMBS", "WMCore.WMBS")        
        destroy = factory.loadObject(myThread.dialect + ".Destroy")
        destroyworked = destroy.execute(conn = myThread.transaction.conn)

        if not destroyworked:
            raise Exception("Could not complete WMBS tear down.")
        
        myThread.transaction.commit()    
        self._teardown = True

        """



        #print setUpString

        if self.writeSetUp:
             file = open(self.GetFilename(), 'a')
             file.write(setUpString)
             file.close()


        return

    def WriteClose(self):
        """
        Write any necessary closing statements
        """

        outstring = """
if __name__ == \"__main__\":
    unittest.main()
        """

        file = open(self.GetFilename(), 'a')
        file.write(outstring)
        file.close()

        return

    def GetFilename(self):
        """
        All this function does is get the filename for the output file so that
        it's consistent across functions
        """

        outstring = str(self.moduleName)+'_t.py'

        return outstring

    def Capital(self, instring):

        outstring  = instring[0].upper() + instring[1:]

        return outstring

                
    def runInspect(self):
        
        moduleList = self.GetListOfMembers(self.moduleName)
        self.WriteTest(moduleList)

        return






if __name__ == "__main__":
    test = Inspect()
    test.runInspect()
