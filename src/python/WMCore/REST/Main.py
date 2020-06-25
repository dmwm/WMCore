""" wmc-httpd [-q] { -r | --restart } [-l LOG-FILE] CONFIG-FILE
        wmc-httpd [-q] { -v | --verify } [-l LOG-FILE] CONFIG-FILE
        wmc-httpd [-q] { -s | --status } CONFIG-FILE
        wmc-httpd [-q] { -k | --kill } CONFIG-FILE

Manages a web server application. Loads configuration and all views, starting
up an appropriately configured CherryPy instance. Views are loaded dynamically
and can be turned on/off via configuration file."""
from __future__ import print_function

from future import standard_library
standard_library.install_aliases()

import errno
import logging
import os
import os.path
import re
import signal
import socket
import sys
import _thread
import time
import traceback
from argparse import ArgumentParser
from io import BytesIO, StringIO
from glob import glob
from subprocess import Popen, PIPE
from pprint import pformat

import cherrypy
from cherrypy import Application
from cherrypy._cplogging import LogManager
from cherrypy.lib import profiler

### Tools is needed for CRABServer startup: it sets up the tools attributes
import WMCore.REST.Tools
from WMCore.Configuration import ConfigSection, loadConfigurationFile
from Utils.Utilities import lowerCmsHeaders

#: Terminal controls to switch to "OK" status message colour.
COLOR_OK = "\033[0;32m"

#: Terminal controls to switch to "warning" status message colour.
COLOR_WARN = "\033[0;31m"

#: Terminal controls to restore normal message colour.
COLOR_NORMAL = "\033[0;39m"


def sig_terminate(signum=None, frame=None):
    """Termination signal handler.

    CherryPy termination signal handling is broken, the handler does not
    take the right number of arguments.  This is our own fixed handler
    to terminate the web server gracefully; in theory it could be
    removed when CherryPy is fixed, but we attach other signals here
    and print a logging entry."""
    cherrypy.log("INFO: exiting server from termination signal %d" % signum, severity=logging.INFO)
    cherrypy.engine.exit()


def sig_reload(signum=None, frame=None):
    """SIGHUP handler to restart the server.

    This just adds some logging compared to the CherryPy signal handler."""
    cherrypy.log("INFO: restarting server from hang-up signal %d" % signum, severity=logging.INFO)
    cherrypy.engine.restart()


def sig_graceful(signum=None, frame=None):
    """SIGUSR1 handler to restart the server gracefully.

    This just adds some logging compared to the CherryPy signal handler."""
    cherrypy.log("INFO: restarting server gracefully from signal %d" % signum, severity=logging.INFO)
    cherrypy.engine.graceful()


class ProfiledApp(Application):
    """Wrapper CherryPy Application object which generates aggregated
    profiles for the component on each call. Note that there needs to
    be an instance of this for each mount point to be profiled."""

    def __init__(self, app, path):
        Application.__init__(self, app.root, app.script_name, app.config)
        self.profiler = profiler.ProfileAggregator(path)

    def __call__(self, env, handler):
        def gather(): return Application.__call__(self, env, handler)

        return self.profiler.run(gather)


class Logger(LogManager):
    """Custom logger to record information in format we prefer."""

    def __init__(self, *args, **kwargs):
        self.host = socket.gethostname()
        LogManager.__init__(self, *args, **kwargs)

    def access(self):
        """Record one client access."""
        request = cherrypy.request
        remote = request.remote
        response = cherrypy.response
        inheaders = lowerCmsHeaders(request.headers)
        outheaders = response.headers
        wfile = request.wsgi_environ.get('cherrypy.wfile', None)
        nout = (wfile and wfile.bytes_written) or outheaders.get('Content-Length', 0)
        if hasattr(request, 'start_time'):
            delta_time = (time.time() - request.start_time) * 1e6
        else:
            delta_time = 0
        msg = ('%(t)s %(H)s %(h)s "%(r)s" %(s)s'
               ' [data: %(i)s in %(b)s out %(T).0f us ]'
               ' [auth: %(AS)s "%(AU)s" "%(AC)s" ]'
               ' [ref: "%(f)s" "%(a)s" ]') % \
              {'t': self.time(),
               'H': self.host,
               'h': remote.name or remote.ip,
               'r': request.request_line,
               's': response.status,
               # request.rfile.rfile.bytes_read is a custom CMS web
               #  cherrypy patch not always available, hence the test
               'i': (getattr(request.rfile, 'rfile', None)
                     and getattr(request.rfile.rfile, "bytes_read", None)
                     and request.rfile.rfile.bytes_read) or "-",
               'b': nout or "-",
               'T': delta_time,
               'AS': inheaders.get("cms-auth-status", "-"),
               'AU': inheaders.get("cms-auth-cert", inheaders.get("cms-auth-host", "")),
               'AC': getattr(request.cookie.get("cms-auth", None), "value", ""),
               'f': inheaders.get("Referer", ""),
               'a': inheaders.get("User-Agent", "")}
        self.access_log.log(logging.INFO, msg)
        self.access_log.propagate = False  # to avoid duplicate records on the log
        self.error_log.propagate = False  # to avoid duplicate records on the log


class RESTMain(object):
    """Base class for the core CherryPy main application object.

    The :class:`~.RESTMain` implements basic functionality of a CherryPy-based
    server. Most users will want the fully functional :class:`~.RESTDaemon`
    instead; but in some cases such as tests and other single-shot jobs which
    don't require a daemon process this class is useful in its own right.

    The class implements the methods required to configure, but not run, a
    CherryPy server set up with an application configuration.

    The main application object takes the server configuration and state
    directory as parametres. It provides methods to create full CherryPy
    serer and configure the application based on configuration description."""

    def __init__(self, config, statedir):
        """Prepare the server.

        :arg config: server configuration
        :arg str statedir: server state directory."""
        self.config = config
        self.appname = config.main.application.lower()
        self.appconfig = config.section_(self.appname)
        self.srvconfig = config.section_("main")
        self.statedir = statedir
        self.hostname = socket.getfqdn().lower()
        self.silent = False
        self.extensions = {}
        self.views = {}

    def validate_config(self):
        """Check the server configuration has the required sections."""
        for key in ('admin', 'description', 'title'):
            if not hasattr(self.appconfig, key):
                raise RuntimeError("'%s' required in application configuration" % key)

    def setup_server(self):
        """Configure CherryPy server from application configuration.

        Traverses the server configuration portion and applies parameters
        known to be for CherryPy to the CherryPy server configuration.
        These are: engine, hooks, log, request, respose, server, tools,
        wsgi, checker.

        Also applies pseudo-parameters ``thread_stack_size`` (default: 128kB)
        and ``sys_check_interval`` (default: 10000). The former sets the
        default stack size to desired value, to avoid excessively large
        thread stacks -- typical operating system default is 8 MB, which
        adds up rather a lot for lots of server threads. The latter sets
        python's ``sys.setcheckinterval``; the default is to increase this
        to avoid unnecessarily frequent checks for python's GIL, global
        interpreter lock. In general we want each thread to complete as
        quickly as possible without making unnecessary checks."""
        cpconfig = cherrypy.config

        # Determine server local base.
        port = getattr(self.srvconfig, 'port', 8080)
        local_base = getattr(self.srvconfig, 'local_base', socket.gethostname())
        if local_base.find(':') == -1:
            local_base = '%s:%d' % (local_base, port)

        # Set default server configuration.
        cherrypy.log = Logger()

        cpconfig.update({'server.max_request_body_size': 0})
        cpconfig.update({'server.environment': 'production'})
        cpconfig.update({'server.socket_host': '0.0.0.0'})
        cpconfig.update({'server.socket_port': port})
        cpconfig.update({'server.socket_queue_size': 100})
        cpconfig.update({'server.thread_pool': 100})
        cpconfig.update({'tools.proxy.on': True})
        cpconfig.update({'tools.proxy.base': local_base})
        cpconfig.update({'tools.time.on': True})
        cpconfig.update({'engine.autoreload.on': False})
        cpconfig.update({'request.show_tracebacks': False})
        cpconfig.update({'request.methods_with_bodies': ("POST", "PUT", "DELETE")})
        _thread.stack_size(getattr(self.srvconfig, 'thread_stack_size', 128 * 1024))
        sys.setcheckinterval(getattr(self.srvconfig, 'sys_check_interval', 10000))
        self.silent = getattr(self.srvconfig, 'silent', False)

        # Apply any override options from app config file.
        for section in ('engine', 'hooks', 'log', 'request', 'response',
                        'server', 'tools', 'wsgi', 'checker'):
            if not hasattr(self.srvconfig, section):
                continue
            for opt, value in getattr(self.srvconfig, section).dictionary_().iteritems():
                if isinstance(value, ConfigSection):
                    for xopt, xvalue in value.dictionary_().iteritems():
                        cpconfig.update({"%s.%s.%s" % (section, opt, xopt): xvalue})
                elif isinstance(value, str) or isinstance(value, int):
                    cpconfig.update({"%s.%s" % (section, opt): value})
                else:
                    raise RuntimeError("%s.%s should be string or int, got %s"
                                       % (section, opt, type(value)))

        # Apply security customisation.
        if hasattr(self.srvconfig, 'authz_defaults'):
            defsec = self.srvconfig.authz_defaults
            cpconfig.update({'tools.cms_auth.on': True})
            cpconfig.update({'tools.cms_auth.role': defsec['role']})
            cpconfig.update({'tools.cms_auth.group': defsec['group']})
            cpconfig.update({'tools.cms_auth.site': defsec['site']})

        if hasattr(self.srvconfig, 'authz_policy'):
            cpconfig.update({'tools.cms_auth.policy': self.srvconfig.authz_policy})
        cherrypy.log("INFO: final CherryPy configuration: %s" % pformat(cpconfig))

    def install_application(self):
        """Install application and its components from the configuration."""
        index = self.srvconfig.index

        # First instantiate non-view extensions.
        if getattr(self.config, 'extensions', None):
            for ext in self.config.extensions:
                name = ext._internal_name
                if not self.silent:
                    cherrypy.log("INFO: instantiating extension %s" % name)
                module_name, class_name = ext.object.rsplit(".", 1)
                module = __import__(module_name, globals(), locals(), [class_name])
                obj = getattr(module, class_name)(self, ext)
                self.extensions[name] = obj

        # Then instantiate views and mount them to cherrypy. If the view is
        # designated as the index, create it as an application, profiled one
        # if server profiling was requested. Otherwise just mount it as a
        # normal server content object. Force tracebacks off for everything.
        for view in self.config.views:
            name = view._internal_name
            path = "/%s" % self.appname + ((name != index and "/%s" % name) or "")
            if not self.silent:
                cherrypy.log("INFO: loading %s into %s" % (name, path))
            module_name, class_name = view.object.rsplit(".", 1)
            module = __import__(module_name, globals(), locals(), [class_name])
            obj = getattr(module, class_name)(self, view, path)
            app = Application(obj, path, {"/": {"request.show_tracebacks": False}})
            if getattr(self.srvconfig, 'profile', False):
                profdir = "%s/profile" % self.statedir
                if not os.path.exists(profdir):
                    os.makedirs(profdir)
                app = ProfiledApp(app, profdir)
            cherrypy.tree.mount(app)
            self.views[name] = obj


class RESTDaemon(RESTMain):
    """Web server object.

    The `RESTDaemon` represents the web server daemon. It provides all
    services for starting, stopping and checking the status of the daemon,
    as well as running the main loop.

    The class implements all the methods required for proper unix
    daemonisation, including maintaing a PID file for the process group
    and correct progressively more aggressive signals sent to terminate
    the daemon. Starting multiple daemons in same directory is refused.

    The daemon takes the server configuration as a parametre. When the
    server is started, it creates a CherryPy server and configuration
    from the application config contents."""

    def __init__(self, config, statedir):
        """Initialise the daemon.

        :arg config: server configuration
        :arg str statedir: server state directory."""
        RESTMain.__init__(self, config, statedir)
        self.pidfile = "%s/%s.pid" % (self.statedir, self.appname)
        self.logfile = ["rotatelogs", "%s/%s-%%Y%%m%%d.log" % (self.statedir, self.appname), "86400"]

    def daemon_pid(self):
        """Check if there is a daemon running, and if so return its pid.

        Reads the pid file from the daemon work directory, if any. If a
        non-empty pid file exists, checks if a process by that PGID exists.
        If yes, reports the daemon as running, otherwise reports either a
        stale daemon no longer running or no deamon running at all.

        :returns: A tuple (running, pgid). The first value will be True if a
          running daemon was found, in which case pid will be its PGID. The
          first value will be false otherwise, and pgid will be either None
          if no pid file was found, or an integer if there was a stale file."""
        pid = None
        try:
            with open(self.pidfile) as fd:
                pid = int(fd.readline())
            os.killpg(pid, 0)
            return (True, pid)
        except:
            return (False, pid)

    def kill_daemon(self, silent=False):
        """Check if the daemon is running, and if so kill it.

        If there is no daemon running and no pid file, does nothing. If there
        is a pid file but no such process running, removes the stale pid file.
        Otherwise sends progressively more lethal signals at intervals to the
        daemon process until it quits.

        The signals are always sent to the entire process group, and signals
        will keep on getting sent as long as at least one process from the
        daemon process group is still alive. If for some reason the group
        cannot be killed otherwise, sends SIGKILL to the group in the end.

        The message about removing a stale pid file cannot be silenced. All
        other messages are squelched if `silent` is True.

        :arg bool silent: do not print any messages if True."""
        running, pid = self.daemon_pid()
        if not running:
            if pid != None:
                print("Removing stale pid file %s" % self.pidfile)
                os.remove(self.pidfile)
            else:
                if not silent:
                    print("%s not running, not killing" % self.appname)
        else:
            if not silent:
                sys.stdout.write("Killing %s pgid %d " % (self.appname, pid))
                sys.stdout.flush()

            dead = False
            for sig, grace in ((signal.SIGINT, .5), (signal.SIGINT, 1),
                               (signal.SIGINT, 3), (signal.SIGINT, 5),
                               (signal.SIGKILL, 0)):
                try:
                    if not silent:
                        sys.stdout.write(".")
                        sys.stdout.flush()
                    os.killpg(pid, sig)
                    time.sleep(grace)
                    os.killpg(pid, 0)
                except OSError as e:
                    if e.errno == errno.ESRCH:
                        dead = True
                        break
                    raise

            if not dead:
                try:
                    os.killpg(pid, signal.SIGKILL)
                except:
                    pass

            if not silent:
                sys.stdout.write("\n")

    def start_daemon(self):
        """Start the deamon."""

        # Redirect all output to the logging daemon.
        devnull = open(os.devnull, "w")
        if isinstance(self.logfile, list):
            subproc = Popen(self.logfile, stdin=PIPE, stdout=devnull, stderr=devnull,
                            bufsize=0, close_fds=True, shell=False)
            logger = subproc.stdin
        elif isinstance(self.logfile, str):
            logger = open(self.logfile, "a+", 0)
        else:
            raise TypeError("'logfile' must be a string or array")
        os.dup2(logger.fileno(), sys.stdout.fileno())
        os.dup2(logger.fileno(), sys.stderr.fileno())
        os.dup2(devnull.fileno(), sys.stdin.fileno())
        logger.close()
        devnull.close()

        # First fork. Discard the parent.
        pid = os.fork()
        if pid > 0:
            os._exit(0)

        # Establish as a daemon, set process group / session id.
        os.chdir(self.statedir)
        os.setsid()

        # Second fork. The child does the work, discard the second parent.
        pid = os.fork()
        if pid > 0:
            os._exit(0)

        # Save process group id to pid file, then run real worker.
        with open(self.pidfile, "w") as pidObj:
            pidObj.write("%d\n" % os.getpgid(0))

        error = False
        try:
            self.run()
        except Exception as e:
            error = True
            if sys.version_info[0] == 2:
                trace = BytesIO()
            else:
                trace = StringIO()
            traceback.print_exc(file=trace)
            cherrypy.log("ERROR: terminating due to error: %s" % trace.getvalue())

        # Remove pid file once we are done.
        try:
            os.remove(self.pidfile)
        except:
            pass

        # Exit
        sys.exit((error and 1) or 0)

    def run(self):
        """Run the server daemon main loop."""
        # Fork.  The child always exits the loop and executes the code below
        # to run the server proper.  The parent monitors the child, and if
        # it exits abnormally, restarts it, otherwise exits completely with
        # the child's exit code.
        cherrypy.log("WATCHDOG: starting server daemon (pid %d)" % os.getpid())
        while True:
            serverpid = os.fork()
            if not serverpid: break
            signal.signal(signal.SIGINT, signal.SIG_IGN)
            signal.signal(signal.SIGTERM, signal.SIG_IGN)
            signal.signal(signal.SIGQUIT, signal.SIG_IGN)
            (xpid, exitrc) = os.waitpid(serverpid, 0)
            (exitcode, exitsigno, exitcore) = (exitrc >> 8, exitrc & 127, exitrc & 128)
            retval = (exitsigno and ("signal %d" % exitsigno)) or str(exitcode)
            retmsg = retval + ((exitcore and " (core dumped)") or "")
            restart = (exitsigno > 0 and exitsigno not in (2, 3, 15))
            cherrypy.log("WATCHDOG: server exited with exit code %s%s"
                         % (retmsg, (restart and "... restarting") or ""))

            if not restart:
                sys.exit((exitsigno and 1) or exitcode)

            for pidfile in glob("%s/*/*pid" % self.statedir):
                if os.path.exists(pidfile):
                    with open(pidfile) as fd:
                        pid = int(fd.readline())
                    os.remove(pidfile)
                    cherrypy.log("WATCHDOG: killing slave server %d" % pid)
                    try:
                        os.kill(pid, 9)
                    except:
                        pass

        # Run. Override signal handlers after CherryPy has itself started and
        # installed its own handlers. To achieve this we need to start the
        # server in non-blocking mode, fiddle with, than ask server to block.
        self.validate_config()
        self.setup_server()
        self.install_application()
        cherrypy.log("INFO: starting server in %s" % self.statedir)
        cherrypy.config.update({'log.screen': bool(getattr(self.srvconfig, "log_screen", True))})
        cherrypy.engine.start()
        signal.signal(signal.SIGHUP, sig_reload)
        signal.signal(signal.SIGUSR1, sig_graceful)
        signal.signal(signal.SIGTERM, sig_terminate)
        signal.signal(signal.SIGQUIT, sig_terminate)
        signal.signal(signal.SIGINT, sig_terminate)
        cherrypy.engine.block()


def main():
    # Re-exec if we don't have unbuffered i/o. This is essential to get server
    # to output its logs synchronous to its operation, such that log output does
    # not remain buffered in the python server. This is particularly important
    # when infrequently accessed server redirects output to 'rotatelogs'.
    if 'PYTHONUNBUFFERED' not in os.environ:
        os.environ['PYTHONUNBUFFERED'] = "1"
        os.execvp("python", ["python"] + sys.argv)

    opt = ArgumentParser(usage=__doc__)
    opt.add_argument("-q", "--quiet", action="store_true", dest="quiet", default=False,
                     help="be quiet, don't print unnecessary output")
    opt.add_argument("-v", "--verify", action="store_true", dest="verify", default=False,
                     help="verify daemon is running, restart if not")
    opt.add_argument("-s", "--status", action="store_true", dest="status", default=False,
                     help="check if the server monitor daemon is running")
    opt.add_argument("-k", "--kill", action="store_true", dest="kill", default=False,
                     help="kill any existing already running daemon")
    opt.add_argument("-r", "--restart", action="store_true", dest="restart", default=False,
                     help="restart, kill any existing running daemon first")
    opt.add_argument("-d", "--dir", dest="statedir", metavar="DIR", default=os.getcwd(),
                     help="server state directory (default: current working directory)")
    opt.add_argument("-l", "--log", dest="logfile", metavar="DEST", default=None,
                     help="log to DEST, via pipe if DEST begins with '|', otherwise a file")
    opts, args = opt.parse_known_args()

    if len(args) != 1:
        print("%s: exactly one configuration file required" % sys.argv[0], file=sys.stderr)
        sys.exit(1)

    if not os.path.isfile(args[0]) or not os.access(args[0], os.R_OK):
        print("%s: %s: invalid configuration file" % (sys.argv[0], args[0]), file=sys.stderr)
        sys.exit(1)

    if not opts.statedir or \
            not os.path.isdir(opts.statedir) or \
            not os.access(opts.statedir, os.W_OK):
        print("%s: %s: invalid state directory" % (sys.argv[0], opts.statedir), file=sys.stderr)
        sys.exit(1)

    # Create server object.
    cfg = loadConfigurationFile(args[0])
    app = cfg.main.application.lower()
    server = RESTDaemon(cfg, opts.statedir)

    # Now actually execute the task.
    if opts.status:
        # Show status of running daemon, including exit code matching the
        # daemon status: 0 = running, 1 = not running, 2 = not running but
        # there is a stale pid file. If silent don't print out anything
        # but still return the right exit code.
        running, pid = server.daemon_pid()
        if running:
            if not opts.quiet:
                print("%s is %sRUNNING%s, PID %d" \
                      % (app, COLOR_OK, COLOR_NORMAL, pid))
            sys.exit(0)
        elif pid != None:
            if not opts.quiet:
                print("%s is %sNOT RUNNING%s, stale PID %d" \
                      % (app, COLOR_WARN, COLOR_NORMAL, pid))
            sys.exit(2)
        else:
            if not opts.quiet:
                print("%s is %sNOT RUNNING%s" \
                      % (app, COLOR_WARN, COLOR_NORMAL))
            sys.exit(1)

    elif opts.kill:
        # Stop any previously running daemon. If quiet squelch messages,
        # except removal of stale pid file cannot be silenced.
        server.kill_daemon(silent=opts.quiet)

    else:
        # We are handling a server start, in one of many possible ways:
        # normal start, restart (= kill any previous daemon), or verify
        # (= if daemon is running leave it alone, otherwise start).

        # Convert 'verify' to 'restart' if the server isn't running.
        if opts.verify:
            opts.restart = True
            if server.daemon_pid()[0]:
                sys.exit(0)

        # If restarting, kill any previous daemon, otherwise complain if
        # there is a daemon already running here. Starting overlapping
        # daemons is not supported because pid file would be overwritten
        # and we'd lose track of the previous daemon.
        if opts.restart:
            server.kill_daemon(silent=opts.quiet)
        else:
            running, pid = server.daemon_pid()
            if running:
                print("Refusing to start over an already running daemon, pid %d" % pid, file=sys.stderr)
                sys.exit(1)

        # If we are (re)starting and were given a log file option, convert
        # the logfile option to a list if it looks like a pipe request, i.e.
        # starts with "|", such as "|rotatelogs foo/bar-%Y%m%d.log".
        if opts.logfile:
            if opts.logfile.startswith("|"):
                server.logfile = re.split(r"\s+", opts.logfile[1:])
            else:
                server.logfile = opts.logfile

        # Actually start the daemon now.
        server.start_daemon()
