from distutils.core import Command
from distutils.command.build import build
from distutils.command.install import install
import os, sys, os.path, shutil
from setup_dependencies import dependencies

def get_path_to_wmcore_root():
    """
    Work out the path to the WMCore root from where the script is being run. Allows for
    calling setup.py env from sub directories and directories outside the WMCore tree.
    """
    return os.path.dirname(os.path.abspath(os.path.join(os.getcwd(), sys.argv[0])))

def walk_dep_tree(system):
    """
    Walk the dependancy tree/dictionary for the given system, and return a list of packages and
    statics that the system depends on.

    system is a dict containing:
        {'packages': ['Package'],
         'systems':['foo'],
         'statics': ['bar']}
    """
    packages = set()
    statics = set()
    modules = set()
    if 'modules' in system.keys():
        modules = set(system.get('modules', set()))
    if 'packages' in system.keys():
        packages = set(system.get('packages', set()))
    if 'statics' in system.keys():
        statics = set(system.get('statics', set()))
    if 'systems' in system.keys():
        for system in system['systems']:
            dependants = walk_dep_tree(dependencies[system])
            packages = packages | dependants.get('packages', set())
            statics = statics | dependants.get('statics', set())
            modules = modules | dependants.get('modules', set())
    return {'packages': packages, 'statics': statics, 'modules': modules}

def list_packages(package_dirs = [], recurse=True):
    """
    Take a list of directories and return a list of all packages under those directories, skipping VCS files.
    """
    packages = []
    # Skip the following files
    ignore_these = set(['CVS', '.svn', 'svn', '.git', 'DefaultConfig.py'])
    for a_dir in package_dirs:
        if recurse:
            # Recurse the sub-directories
            for dirpath, dirnames, filenames in os.walk('%s' % a_dir, topdown=True):
                pathelements = dirpath.split('/')
                # If any part of pathelements is in the ignore_these set skip the path
                if len(set(pathelements) & ignore_these) == 0:
                    rel_path = os.path.relpath(dirpath, get_path_to_wmcore_root())
                    rel_path = rel_path.split('/')[2:]
                    packages.append('.'.join(rel_path))
                else:
                    print 'Ignoring %s' % dirpath
        else:
            rel_path = os.path.relpath(a_dir, get_path_to_wmcore_root())
            rel_path = rel_path.split('/')[2:]
            packages.append('.'.join(rel_path))
    return packages

def data_files_for(dir):
    """
    Return list of data files in provided dir, do not walk through
    """
    files = []
    for ifile in os.listdir(dir):
        filename = os.path.join(dir, ifile)
        if  os.path.isfile(filename):
            if  filename[-1] == '~':
                continue
            files.append(filename)
    return files

def list_static_files(system = None):
    """
    Get a list of all the files that are classed as static (e.g. javascript, css, templates)
    """
    # Skip the following files
    ignore_these = set(['CVS', '.svn', 'svn', '.git', 'DefaultConfig.py'])
    static_files = []
    add_static = static_files.append
    if system:
        for static_dir in walk_dep_tree(system)['statics']:
            if static_dir.endswith('+'):
                static_dir = static_dir.rstrip('+')
                for dirpath, dirnames, filenames in os.walk('%s' % static_dir, topdown=True):
                    pathelements = dirpath.split('/')
                    # If any part of pathelements is in the ignore_these set skip the path
                    if len(set(pathelements) & ignore_these) == 0:
                        rel_path = os.path.relpath(dirpath, get_path_to_wmcore_root())
                        files = [os.path.join(rel_path, f) for f in filenames]
                        add_static((rel_path.replace('src/', ''), files))
                    else:
                        print 'Ignoring %s' % dirpath
            else:
                add_static((static_dir.replace('src/', ''), data_files_for(static_dir)))
    else:
        for language in ['couchapps', 'css', 'html', 'javascript', 'templates']:
            add_static((language, data_files_for('%s/src/%s' % (get_path_to_wmcore_root(), language))))
    # The contents of static_files will be copied at install time
    return static_files

def check_system(command):
    """
    Check that the system being built is known, print an error message and exit if it's not.
    """
    if command.system in dependencies.keys():
        return
    elif command.system == None:
        msg = "System not specified: -s option for %s must be specified and provide one of:\n" % command.get_command_name()
        msg += ", ".join(dependencies.keys())
        print msg
        sys.exit(1)
    else:
        msg = "Specified system [%s] is unknown:" % command.system
        msg += " -s option for %s must be specified and provide one of:\n" % command.get_command_name()
        msg += ", ".join(dependencies.keys())
        print msg
        sys.exit(1)

def things_to_build(command):
    """
    Take a build/install command and determine all the packages and modules it needs to build/install. Modules are
    explicitly listed in the dependancies but packages needs to be generated (to pick up sub directories).
    """
    # work out all the dependent packages
    dependency_tree = walk_dep_tree(dependencies[command.system])
    # and the corresponding source directories and files
    package_src_dirs = []
    module_src_files = []
    for package in dependency_tree['packages']:
        # Need to recurse packages
        recurse = package.endswith('+')
        print package, recurse
        package = package.rstrip('+')
        src_path = '%s/src/python/%s' % (get_path_to_wmcore_root(), package.replace('.','/'))
        package_src_dirs.extend(list_packages([src_path], recurse))
    return package_src_dirs, dependency_tree['modules']

def print_build_info(command):
    """
    print some helpful information about what needs to be built
    """
    if len(command.distribution.packages):
        command.announce('Installing %s requires the following packages:' % (command.system), 1)
        for package in command.distribution.packages:
            if os.path.exists('%s/build/lib/%s' % (get_path_to_wmcore_root(), package.replace('.','/'))) and not command.force:
                command.announce('\t %s \t - already built!' % package, 1)
            else:
                command.announce('\t %s' % package, 1)
    if len(command.distribution.py_modules):
        command.announce('Installing %s requires the following modules:' % (command.system), 1)
        for modules in command.distribution.py_modules:
            if os.path.exists('%s/build/lib/%s' % (get_path_to_wmcore_root(), modules.replace('.','/'))) and not command.force:
                command.announce('\t %s \t - already built!' % modules)
            else:
                command.announce('\t %s' % modules)

def force_rebuild():
    """
    When building sub-systems its a good idea to always start from a fresh build area, otherwise
    sub-systems can merge up, worst case is doing a sub-system install after a full build/install -
    you'll get the contents of all packages.

    This method forcibly removes the build area, so that all sub-system builds/installs start from
    a clean sheet.
    """
    if os.path.exists('%s/build' % get_path_to_wmcore_root()):
        shutil.rmtree('%s/build' % get_path_to_wmcore_root())

class BuildCommand(Command):
    """
    Build a specific system, including it's dependencies. Run with --force to trigger a rebuild
    """
    description = "Build a specific sub-system, including it's dependencies. Should be used with --force"
    description = "to ensure a clean build of only the specified sub-system.\n\n"
    description += "\tAvailable sub-systems: \n"
    description += "\t["
    description += ", ".join(dependencies.keys())
    description += "]\n"

    user_options = build.user_options
    user_options.append(('system=', 's', 'build the specified system'))

    def initialize_options(self):
        # and add our additional option
        self.system = None

    def finalize_options (self):
        # Check that the sub-system is valid
        check_system(self)
        # Set what to build
        self.distribution.packages, self.distribution.py_modules = things_to_build(self)
        print_build_info(self)
        # Always do a rebuild
        force_rebuild()

    def run (self):
        # Have to get the build command here and set force, as the build plugins only refer to the
        # build command, not what calls them. The following is taken from the Distribution class,
        # with the additional explicit setting of force
        command = 'build'
        if self.distribution.have_run.get(command):
           return
        cmd = self.distribution.get_command_obj(command)
        # Forcibly set force
        cmd.force = self.force
        cmd.ensure_finalized()
        cmd.run()
        self.distribution.have_run[command] = 1

class InstallCommand(install):
    """
    Install a specific system, including it's dependencies.
    """
    description = "Install a specific system, including it's dependencies. Should be used with --force"
    description = "to ensure a clean build of only the specified sub-system.\n\n"
    description += "\tAvailable sub-systems: \n"
    description += "\t["
    description += ", ".join(dependencies.keys())
    description += "]\n"

    user_options = install.user_options
    user_options.append(('system=', 's', 'install the specified system'))

    def initialize_options(self):
        # Call the base class
        install.initialize_options(self)
        # and add our additionl option
        self.system = None

    def finalize_options(self):
        # Check that the sub-system is valid
        check_system(self)
        # Set what actually gets installed
        self.distribution.packages, self.distribution.py_modules = things_to_build(self)
        self.distribution.data_files = list_static_files(dependencies[self.system])
        print_build_info(self)
        # Always do a rebuild
        force_rebuild()

        self.distribution.metadata.name = self.system
        assert self.distribution.get_name() == self.system

        install.finalize_options(self)

    def run (self):
        # Have to override the distribution to build and install only what we specify.
        for cmd_name in self.get_sub_commands():
            cmd = self.distribution.get_command_obj(cmd_name)
            cmd.distribution = self.distribution
            # We don't want data files mixed in with the python
            if cmd_name == 'install_data':
                cmd.install_dir = os.path.join(self.prefix, 'lib')
            else:
                cmd.install_dir = self.install_lib
            cmd.ensure_finalized()
            self.run_command(cmd_name)
            self.distribution.have_run[cmd_name] = 1
