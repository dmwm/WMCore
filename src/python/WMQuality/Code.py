
import commands
import os
import sys


class Code:

    def __init__(self, script, report, baseDir, packages, threshold):
        self.script = script
        self.report = report
        self.packages = packages
        self.baseDir = baseDir
        self.threshold = threshold
        # author to file/rate mappings
        self.lowQuality = {}


    def run(self):

        print 'Qaulity script: '+ self.script
        print 'Report file:    '+ self.report
        print 'Base dir:       '+ self.baseDir

        cont = raw_input('Are these values correct? Press "A" to abbort or any other key to proceed ')
        if cont == 'A':
            sys.exit(0)

        for packageDir in self.packages.keys():
            localPath = os.path.join(self.baseDir, packageDir) 
            # execute the quality script which produces a codeQuality.txt file
            command = self.script+' '+localPath
            result = commands.getstatusoutput(command)
            for entry in result:
                print(str(entry))
            # parse the code quality file for the rating:
            reportFile = open(self.report, 'r')
            repNl = reportFile.readline()
            while repNl:
                if repNl.find('Your code has been rated at') == 0:
                    relRating = repNl.split(' ')[6]     
                    absRating = float(relRating.split('/')[0])
                    if absRating < self.threshold:
                        fileRating = (str(absRating), packageDir)
                        authors = self.packages[packageDir].split(',')
                        for author in authors:
                            if not self.lowQuality.has_key(author):
                                self.lowQuality[self.packages[packageDir]] = []
                            # add the low rating
                            self.lowQuality[author].append(fileRating)
                        break
                repNl = reportFile.readline() 
            reportFile.close() 
    
    def summaryText(self):

        print('\nReport Summary:\n')
        for author in self.lowQuality.keys():
            if len(self.lowQuality[author]) > 0:
                print('Author: '+author)
                print('---------------------')
                for fileRating in self.lowQuality[author]:
                    print(fileRating[0]+' :: '+fileRating[1])    
                print('\n\n')
           

