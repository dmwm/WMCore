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
                 interval=0,
                 actionLimit=1):
        """
        __init__
        :param expPids:     The list of expected pids allowed to reset the timer, signals from anybody else would be ignored
        :param compName:    The component name this timer is associated with
        :param expSig:      The default signal to be expected for resetting the timer (Default: SIGCONT)
        :param interval:    The interval for the timer
        :param path:        The path where the timer's data is to be preserved on disk
        :param action:      The action to be taken in case a timer gets expired. An instance of WatchdogAction
        :param actionLimit: The maximum number of times an action would be executed
                            (hence, the maximum number of times the timer is allowed to expire) before the timer's thread gets destroyed.
                            Default: 1 - Meaning the timer would be allowed to expire exactly once.
                            NOTE: The action will be executed  at least once, the moment when the timer gets expired for the first time.
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
        self.actionString = action.__str__()
        self.actionCounter = 0
        self.actionLimit = actionLimit
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

    def reset(self):
        """
        _reset_
        Resets the timer by delaying its endTime with one timer's interval.
        """
        self.endTime = time.time() + self.interval

    def restart(self, *args, **kwArgs):
        """
        _restart_
        A method allowing a complete reconfiguration and restart of the current timer.
        :param *: Accepts all keyword parameters allowed at __init__
                NOTE: This method merges any newly provided kwArgs with the already
                      existing object parameters. Any non kwArgs are ignored
        """
        # First, check if the timer is still alive and did not yet reach its end of time
        if self.is_alive():
            logging.warning(f"You cannot restart a running timer. You should wait until all its action retries get exhausted and the timer's thread is stopped normally.")
            return

        # Second, reconfigure it with the new  set of parameters  and recreate its thread
        logging.info(f"Re-configuring timer: {self.name}")
        for arg in inspect.signature(self.__init__).parameters:
            if arg not in kwArgs:
                kwArgs[arg] = getattr(self, arg, None)
        self.__init__(**kwArgs)

        # Finally, rerun it
        logging.info(f"Restarting timer: {self.name}")
        self.start()

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
                    self.reset()
                else:
                    # Continue to wait for signal from the correct origin
                    logging.info(f"{self.name}, pid: {self.native_id}, Continue to wait for signal from the correct origin. Remaining time: {self.remTime}")
                    continue
            else:
                logging.info(f"{self.name}, pid: {self.native_id}, Reached the end of timer. Applying action: {self.action}")
                try:
                    self.action.func(*self.action.args, **self.action.kwArgs)
                    self.actionCounter += 1
                    self.reset()
                except Exception as ex:
                    currFrame = inspect.currentframe()
                    argsInfo = inspect.getargvalues(currFrame)
                    argVals = {arg: argsInfo.locals.get(arg) for arg in argsInfo.args}
                    msg = f"Failure while applying {self.name} timer's action: {self.action.func.__name__} "
                    msg += f"With arguments: {argVals}"
                    msg += f"Full exception string: {str(ex)}"
                    raise TimerException(msg) from None
                if self.actionCounter >= self.actionLimit:
                    break
