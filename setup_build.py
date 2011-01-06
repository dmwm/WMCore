from distutils.core import setup, Command
from distutils.command.build import build
from distutils.command.install import install
import os, sys, os.path
from setup_dependencies import dependencies

def get_relative_path():
    return os.path.dirname(os.path.abspath(os.path.join(os.getcwd(), sys.argv[0])))

def generate_filelist(basepath=None, recurse=True, ignore=False):
    if basepath:
        walkpath = os.path.join(get_relative_path(), 'src/python', basepath)
    else:
        walkpath = os.path.join(get_relative_path(), 'src/python')

    files = []

    if walkpath.endswith('.py'):
        if ignore and walkpath.endswith(ignore):
            files.append(walkpath)
    else:
        for dirpath, dirnames, filenames in os.walk(walkpath):
            # skipping CVS directories and their contents
            pathelements = dirpath.split('/')
            result = []
            if not 'CVS' in pathelements:
                # to build up a list of file names which contain tests
                for file in filenames:
                    if file.endswith('.py'):
                        filepath = '/'.join([dirpath, file])
                        files.append(filepath)

    if len(files) == 0 and recurse:
        files = generate_filelist(basepath + '.py', not recurse)

    return files

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

def list_packages(package_dirs = []):
    """
    List all the packages in the source tree, skipping VCS files.
    """
    packages = []
    ignore_these = set(['CVS', '.svn', 'svn', '.git', 'DefaultConfig.py'])
    for a_dir in package_dirs:
        for dirpath, dirnames, filenames in os.walk('./%s' % a_dir, topdown=True):
            pathelements = dirpath.split('/')
            # If any part of pathelements is in the ignore_these set skip the path
            if len(set(pathelements) & ignore_these) == 0:
                path = pathelements[3:]
                packages.append('.'.join(path))
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
    static_files = []
    add_static = static_files.append
    if system:
        for static_dir in walk_dep_tree(system)['statics']:
            add_static((static_dir.replace('src/', ''), data_files_for(static_dir)))
    else:
        for language in ['couchapps', 'css', 'html', 'javascript', 'templates']:
            add_static((language, data_files_for('src/%s' % language)))
    # The contents of static_files will be copied at install time
    return static_files

class BuildCommand(Command):
   """
   Build a specific system, including it's dependencies. Run with --force to trigger a rebuild
   """
   description = "Build a specific system, including it's dependencies\n\n"
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
       if self.system == None:
           msg = "System not specified: -s option for install_system must be specified and provide one of:\n"
           msg += ", ".join(dependencies.keys())
           self.warn(msg)
           sys.exit(1)
       if self.system in dependencies.keys():
           # work out all the dependent packages
           required_packages = walk_dep_tree(dependencies[self.system])['packages']
           # and the corresponding source directories
           src_dirs = []
           for package in required_packages:
               src_path = 'src/python/%s' % (package.replace('.','/'))
               src_dirs.append(src_path)
           packages_to_build = list_packages(src_dirs)

           # print some helpful information
           self.announce('%s requires the following packages:' % (self.system), 1)
           for package in packages_to_build:
               if os.path.exists('./build/lib/%s' % package.replace('.','/')) and not self.force:
                   self.announce('\t %s \t - already built!' % package, 1)
               else:
                   self.announce('\t %s' % package, 1)
           # set what actually gets built
           self.distribution.packages = packages_to_build
           self.distribution.py_modules = walk_dep_tree(dependencies[self.system])['modules']
       else:
           msg = "Specified system [%s] is unknown:" % self.system
           msg += " -s option for install_system must be specified and provide one of:\n"
           msg += ", ".join(dependencies.keys())
           self.warn(msg)
           sys.exit(1)

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
    description = "Install a specific system, including it's dependencies. \n\n"
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
        if self.system == None:
            msg = "System not specified: -s option for install_system must be specified and provide one of:\n"
            msg += ", ".join(dependencies.keys())
            self.warn(msg)
            sys.exit(1)
        self.distribution.metadata.name = self.system
        assert self.distribution.get_name() == self.system
        # build the list of depndancies
        if self.system in dependencies.keys():
            # work out all the dependent packages
            required_packages = walk_dep_tree(dependencies[self.system])['packages']
            # and the corresponding source directories
            src_dirs = []
            for package in required_packages:
                src_path = 'src/python/%s' % (package.replace('.','/'))
                src_dirs.append(src_path)
            packages_to_build = list_packages(src_dirs)

            # print some helpful information
            self.announce('%s requires the following packages:' % (self.system), 1)
            for package in packages_to_build:
                if os.path.exists('./build/lib/%s' % package.replace('.','/')) and not self.force:
                    self.announce('\t %s \t - already built!' % package, 1)
                else:
                    self.announce('\t %s' % package, 1)
            # set what actually gets built
            self.distribution.packages = packages_to_build
            self.distribution.py_modules = walk_dep_tree(dependencies[self.system])['modules']
            self.distribution.data_files = list_static_files(dependencies[self.system])
        else:
            msg = "Specified system [%s] is unknown:" % self.system
            msg += " -s option for install_system must be specified and provide one of:\n"
            msg += ", ".join(dependencies.keys())
            self.warn(msg)
            sys.exit(1)

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
