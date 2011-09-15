import sys , re #DS
EXIT_CODE_RE = re.compile('^\s*-\s*[Ee]xit.[Cc]ode\s*=\s*(.*?)\s*$')

class LoggingInfoParser:
    def __init__(self) :
        self._events = ['Running', 'Done', 'Abort']
        self._errors = ['Maradona', 'Globus error',
                        'gridmanager error', 'CondorG',
                        'BrokerInfo', 'Cannot download',
                        'Cannot upload']
        self._categories = ['Resource unavailable',
                            'Grid error before job started',
                            'Grid error after job started',
                            'Aborted by user',
                            'Application error',
                            'Success',
                            'Failure while executing job wrapper' ]
        self.category = ''
        self.reason = ''

    def parse_reason(self, reason) :
        error = reason
        if reason.count('Cannot download') >= 1 :
            error = 'Cannot download ... from ...'
        if reason.count('cannot retrieve previous matches') >= 1 :
            error = 'Harmless warning'
        if reason.count('RetryCount') >= 1  :
            error = 'Harmless warning'
        if reason.count('There were some warnings') >= 1 :
            error = 'Harmless warning'
        return error;

    def parseFile(self,filename) :
        return self.decodeReason(open(filename).read())

    def decodeReason(self, input) :
        """
        extract meaningful message from logging-info
        """
        # init final variables
        final_done_code = 0
        final_abort_msg = ''
        final_done_msg  = ''
        final_category  = ''
        final_abort     = 0
        final_done      = 0
        final_running   = 0

        # init variable used in loop
        event     = ''
        reason    = ''
        exit_code = ''

        lines = input.split('\n')
        for line in lines :
            if line.count('Event:') >= 1 :
                event = line.split(':')[1].strip()
            if event == 'Abort' :
                final_abort = 1
            if event == 'Running' :
                final_running = 1
            if event == 'Done' :
                final_done = 1
            if line.count('Reason') >= 1 :
                reason = self.parse_reason(line.split('=')[1].strip())
            match = EXIT_CODE_RE.match(line) #DS
            if match:                       #DS
                exit_code = match.groups()[0] #DS
            if ( line.count('---') >= 1 or line.count('***') >= 1 ) and event != '' :
                if event in self._events :
                    if event == 'Done' :
                        final_done_code = int(exit_code)
                        if final_done_msg == '' :
                            final_done_msg = reason
                        if final_done_msg != reason :
                            final_done_msg += '. '+reason
                    elif event == 'Abort' :
                        final_abort_msg = reason

        if final_abort_msg.count('Failure while executing job wrapper') >= 1 and \
           ( final_running == 1 ) :
            final_category = self._categories[6]

        if final_abort_msg.count('no compatible resources') >= 1 :
            final_category = self._categories[0]

        if ( final_running == 0 ) and \
           ( final_abort == 1 ) and \
           ( final_abort_msg.count('no compatible resources') >= 1 ) :
            final_category = self._categories[1]

        if ( final_running == 0 ) :
            for error in self._errors :
                if final_done_msg.count(error) >= 1 :
                    final_category = self._categories[1]

        if ( final_running == 1 ) and \
           ( final_done_code != 0 ) and \
           ( final_done == 0 ) :
            final_category = self._categories[2]


        if ( final_running == 1 ) and \
           ( final_done_code != 0 ) :
            for error in self._errors :
                if final_done_msg.count(error) >= 1 :
                    final_category = self._categories[2]

        if ( final_done == 1 ) and \
           ( final_done_msg.count('Aborted by user') >= 1 ) :
            final_category = self._categories[3]

        if ( final_running == 1 ) and \
           ( final_abort == 0 ) and \
           ( final_done == 1 ) and \
           ( final_done_code != 0 ) :
            check = 0
            for error in self._errors :
                if final_done_msg.count(error) >= 1 :
                    check = 1
            if check == 0 :
                final_category = self._categories[4]

        if ( final_running == 1 ) and \
           ( final_abort == 0 ) and \
           ( final_done_code == 0 ) and \
           ( final_done_msg.count('Aborted by user') == 0 ) :
            final_category = self._categories[5]

        msg = ''
        if final_category == self._categories[0] :
            msg = 'aborted because: "'+self._categories[0]+'". Abort msg: "'+final_abort_msg+'".'
            self.reason = final_abort_msg
            self.category = self._categories[0]
        elif final_category == self._categories[1] :
            if final_done == 1 :
                msg = 'aborted with "'+self._categories[1]+'". Abort msg: "'+final_done_msg+'".'
                self.reason = final_done_msg
                self.category = self._categories[1]
            else :
                msg = 'aborted with "'+self._categories[1]+'". Abort msg: "'+final_abort_msg+'".'
                self.reason = final_abort_msg
                self.category = self._categories[1]
        elif final_category == self._categories[2] :
            msg = 'aborted with "'+self._categories[2]+'". Abort msg: "'+final_done_msg+'".'
            self.reason = final_done_msg
            self.category = self._categories[2]
        elif final_category == self._categories[3] :
            msg = 'was "'+self._categories[3]+'".'
            self.reason = self._categories[3]
            self.category = self._categories[3]
        elif final_category == self._categories[4] :
            msg = 'finished correctly but failed with error code: ', final_done_code
            self.reason = msg
            self.category = self._categories[4]
        elif final_category == self._categories[5] :
            msg = 'succeeded'
            self.reason = msg
            self.category = self._categories[5]
        elif final_category == self._categories[6] :
            msg = 'aborted with msg "'+final_abort_msg+'".'
            self.reason = msg
            self.category = self._categories[5]

        return msg
