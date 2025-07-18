import signal
import logging
import time
import inspect
import json

from collections  import namedtuple
from threading import Thread
from pprint import pformat


def _countdown(endTime):
    """
    _countdown_

    Aux function to return the remaining time before reaching the endTime
    :param endTime: The end time in seconds since the epoch.
    :return:        Remaining time in seconds or 0 if the endTime has already passed
                    No negative values are returned
    """
    remTime = endTime - time.time()
    if remTime < 0:
        remTime = 0
    return remTime

WatchdogAction = namedtuple('WatchdogAction', ['func', 'args', 'kwArgs'])

class TimerException(Exception):
    """
    __TimerException__
    Exception raised by the agent related to AgentWatchdog timers.
    """
    def __init__(self, message, errorNo=None, **data):
        self.name = str(self.__class__.__name__)
        Exception.__init__(self, self.name, message)


class Timer(Thread):
    """
    Watchdog timer class.
    All instances of this class are about to spawn a separate thread and are to
    expect a list of particular PIDs to reset the timer. If the timer reaches to the end
    of its interval, without being reset, it would call the action function provided
    at initialization time with the respective arguments.
    """
    def __init__(self,
                 name=None,
                 compName=None,
                 action=None,
                 expPids=[],
                 expSig=None,
                 path=None,
                 interval=0):
        """
        __init__
        :param expPids:  The list of expected pids allowed to reset the timer, signals from anybody else would be ignored
        :param compName: The component name this timer is associated with
        :param expSig:   The default signal to be expected for resetting the timer (Default: SIGCONT)
        :param interval: The interval for the timer
        """
        Thread.__init__(self)
        self.name = name
        self.compName = compName
        self.interval = interval
        self.endTime = 0
        self.startTime = 0
        self.expPids = expPids
        self.expSig = expSig or signal.SIGCONT
        self.path = path
        self.daemon = True
        self.action = action
        if self.action:
            # First, make sure the signature of the action defined for this timer matches the arguments provided:
            try:
                actionSignature = inspect.signature(self.action.func)
                actionSignature.bind(self.action.args, self.action.kwArgs)
            except TypeError as ex:
                msg = f"{self.name}:  The timer's action method signature does not match the set of arguments provided. Error: {str(ex)}"
                raise TimerException(msg) from None
        else:
            logging.warning(f"{self.name}: This is a timer with no action defined. This timer should be used mostly for debugging purposes.")

    def run(self):
        """
        _run_
        Thread class run() method override
        """
        self._timer()

    def write(self):
        """
        _write_
        Method to write the current timer's state on disk:
        :return: Nothing. Logs an error in case the timer was not written correctly
                 No exceptions would be raised
        """
        try:
            with open(self.path, 'w') as timerFile:
                json.dump(self.dictionary_(), timerFile , indent=4)
        except Exception as ex:
            logging.error(f"{self.name}: Failed to write timer data on disk. Timer path: {self.path}. ERROR: {str(ex)}")

    @property
    def remTime(self):
        """
        _remTime_
        Returns the current timer's remaining time (in seconds) before the timer's action is to be applied
        """
        return _countdown(self.endTime)

    @staticmethod
    def _isSerializable(obj):
        """
        __isSerializable__
        Auxiliary function to check for object serialization
        :param obj: Object of any type to be checked
        :return:    Bool - True if the object is serializable, False otherwise
        """
        try:
            json.dumps(obj)
            logging.debug(f"{obj} is serializable")
            return True
        except TypeError:
            logging.debug(f"{obj} is NOT serializable")
            return False

    def dictionary_(self):
        """
        _dictionary__
        Returns a dictionary representation of the current timer
        """
        return {attr: value for attr,value in inspect.getmembers(self) if self._isSerializable(value)}

    def _timer(self):
        """
        A simple timer method, which will be used to override the main Thread class run() method.
        It would expect one particular PID to reset itself. If the timer reaches to the end of
        its interval, without being reset, it would restart the component it is associated with.
        :return:         Nothing
        """

        self.startTime = time.time()
        self.endTime = self.startTime + self.interval

        while True:
            sigInfo = signal.sigtimedwait([self.expSig], self.remTime)
            if sigInfo:
                logging.info(f"{self.name}, pid: {self.native_id}, Received signal: {pformat(sigInfo)}")
                if sigInfo.si_pid in self.expPids:
                    # Resetting the timer starting again from the current time
                    logging.info(f"{self.name}, pid: {self.native_id}, Resetting timer")
                    self.endTime = time.time() + self.interval
                else:
                    # Continue to wait for signal from the correct origin
                    logging.info(f"{self.name}, pid: {self.native_id}, Continue to wait for signal from the correct origin. Remaining time: {self.remTime}")
                    continue
            else:
                logging.info(f"{self.name}, pid: {self.native_id}, Reached the end of timer. Applying action: {self.action}")
                try:
                    self.action.func(*self.action.args, **self.action.kwArgs)
                except Exception as ex:
                    currFrame = inspect.currentframe()
                    argsInfo = inspect.getargvalues(currFrame)
                    argVals = {arg: argsInfo.locals.get(arg) for arg in argsInfo.args}
                    msg = f"Failure while applying {self.name} timer's action: {self.action.func.__name__} "
                    msg += f"With arguments: {argVals}"
                    msg += f"Full exception string: {str(ex)}"
                    raise TimerException(msg) from None
                break
