import os
import re

class LoggingInfoParser:
    #Non capturing group: (?: )
    #Error messages can contain \n (that's why I used lookahead (?=-))
    sregExps = [
                  #Extract the reason of the last failing Transfer (##0 ##3 ##8)
                  #The regex matches all the events, but only store the reason of failing Transfer. It is stored in group 1
                  #Assumption: the "- Result = FAIL" string is immediately after the reason
                  r'(?:(?:Event: Transfer\n(?:-\sReason\s+=\s+([^\n]*)\n- Result\s+=\s+FAIL\n|(?:(?!---).)*?\n)+.---\n)+|Event: (?!Transfer).*?---\n)+',

                  #Broker Helper issue (##1 ##9)
                  r'(BrokerHelper: no compatible resources)',

                  #The last Done event. (##2 ##4 ##5 ##6)
                  r'(?:(?:Event: Done\n(?:-\sReason\s+=\s+(.*?)\n(?=-)|(?:(?!---).)*?\n)+.---\n)+|Event: (?!Done).*?---\n)+',

                  #The last Abort event. (##7)
                  r'(?:(?:Event: Abort\n(?:-\sReason\s+=\s+(.*?)\n(?=-)|(?:(?!---).)*?\n)+.---\n)+|Event: (?!Abort).*?---\n)+',
                  ]

    def __init__(self, saveLogsPath=None):
        """
        saveLogsPath:  if set the loggingInfo are saved there for backup. Assume the directory exists.
        """
        self.savelogsPath = saveLogsPath
        self.regExps = [ re.compile(regex, re.DOTALL) for regex in self.sregExps ]

    def parseFile(self, filename, jobId=None):
        """
        filename:   the logging info filename
        jobId:      the server id of the job for debugging purposes. Needed if self.saveLogsPath is set
        """
        content = open(filename).read()
        #Save the logging info file if needed
        if self.savelogsPath:
            open(os.path.join(self.savelogsPath, 'loggingInfoJob.%s.log' % jobId), 'w').write(content)
        for regex in self.regExps:
            match = regex.search(content)
            if match and match.group(1):
                #Instructions useful for debug. I assume they'll be useful if I'll have to modify the code in the future, so I keep them commented ;)
#                if match.group(0):
#                    open('/tmp/ciao','w').write(match.group(0))
                return match.group(1)

        return "Cannot extract the failure reason from the logging info file" + (". Server job Id: " + str(jobId) if jobId else "")
