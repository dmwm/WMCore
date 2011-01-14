#!/usr/bin/env python
""" Main Module for browsing and modifying requests """
import WMCore.RequestManager.RequestDB.Interface.User.Registration as Registration
import WMCore.RequestManager.RequestDB.Interface.Admin.SoftwareManagement as SoftwareAdmin
import WMCore.RequestManager.RequestDB.Interface.Admin.ProdManagement as ProdManagement
import WMCore.RequestManager.RequestDB.Interface.Admin.GroupManagement as GroupManagement
import WMCore.RequestManager.RequestDB.Interface.Admin.UserManagement as UserManagement
import WMCore.RequestManager.RequestDB.Interface.Group.Information as GroupInfo
import WMCore.RequestManager.RequestDB.Interface.User.Requests as UserRequests
import WMCore.RequestManager.RequestDB.Interface.Request.ListRequests as ListRequests
from WMCore.HTTPFrontEnd.RequestManager.ReqMgrWebTools import allSoftwareVersions
import WMCore.Lexicon
import logging
import cherrypy
import threading
from WMCore.WebTools.WebAPI import WebAPI

class Admin(WebAPI):
    """ Handles administration functions for ReqMgr """
    def __init__(self, config):
        WebAPI.__init__(self, config)
        # Take a guess
        self.templatedir = config.templates
        cherrypy.engine.subscribe('start_thread', self.initThread)

    def initThread(self, thread_index):
        """ The ReqMgr expects the DBI to be contained in the Thread  """
        myThread = threading.currentThread()
        #myThread = cherrypy.thread_data
        # Get it from the DBFormatter superclass
        myThread.dbi = self.dbi

    def validate(self, v, name=''):
        """ Checks if alphanumeric, tolerating spaces """
        if isinstance(v, list):
            for entry in v:
                 self.validate(entry)
        else:
            try:
                WMCore.Lexicon.identifier(v)
            except AssertionError:
                raise cherrypy.HTTPError(400, "Bad input %s" % name)
        return v

    @cherrypy.expose
    def index(self):
        """ Main web page """
        return """
<a href="users">Users</a>
<a href="groups">Groups</a>
<a href="teams">Teams</a>
<a href="versions">Versions</a>
         """

    @cherrypy.expose
    def user(self, userName):
        """ Web page of details about the user, and sets user priority """
        self.validate(userName)
        groups = GroupInfo.groupsForUser(userName).keys()
        requests = UserRequests.listRequests(userName).keys()
        priority = UserManagement.getPriority(userName)
        allGroups = GroupInfo.listGroups()
        self.validate(groups)
        self.validate(requests)
        self.validate(allGroups)
        return self.templatepage("User", user=userName, groups=groups, 
            allGroups=allGroups, requests=requests, priority=priority)

    @cherrypy.expose
    def handleUserPriority(self, user, userPriority):
        """ Handles setting user priority """
        self.validate(user)
        UserManagement.setPriority(user, userPriority)
        return "Updated user %s priority to %s" % (user, userPriority)

    @cherrypy.expose
    def group(self, groupName):
        """ Web page of details about the user, and sets user priority """
        self.validate(groupName)
        users = GroupInfo.usersInGroup(groupName)
        priority = GroupManagement.getPriority(groupName)
        return self.templatepage("Group", group=groupName, users=users, priority=priority)

    @cherrypy.expose
    def handleGroupPriority(self, group, groupPriority):
        """ Handles setting group priority """
        self.validate(group)
        GroupManagement.setPriority(group, groupPriority)
        return "Updated group %s priority to %s" % (group, groupPriority)

    @cherrypy.expose
    def users(self):
        """ Lists all users.  Should be paginated later """
        allUsers = Registration.listUsers()
        self.validate(allUsers)
        return self.templatepage("Users", users=allUsers)

    @cherrypy.expose
    def handleAddUser(self, user, email=None):
        """ Handles setting user priority """
        self.validate(user)
        Registration.registerUser(user, email)
        return "Added user %s" % user

    @cherrypy.expose
    def handleAddToGroup(self, user, group):
        """ Adds a user to the group """
        self.validate(user)
        self.validate(group)
        GroupManagement.addUserToGroup(user, group)
        return "Added %s to %s " % (user, group)

    @cherrypy.expose
    def groups(self):
        """ Lists all users.  Should be paginated later """
        allGroups = GroupInfo.listGroups()
        self.validate(allGroups)
        return self.templatepage("Groups", groups=allGroups)

    @cherrypy.expose
    def handleAddGroup(self, group):
        """ Handles adding a group """
        self.validate(group)
        GroupManagement.addGroup(group)
        return "Added group %s " % group

    @cherrypy.expose
    def teams(self):
        """ Lists all teams """
        return self.templatepage("Teams", teams = ProdManagement.listTeams())

    @cherrypy.expose
    def team(self, teamName):
        """ Details for a team """
        self.validate(teamName)
        assignments = ListRequests.listRequestsByTeam(teamName)
        self.validate(assignments)
        return self.templatepage("Team", team=teamName, requests=assignments.keys())

    @cherrypy.expose
    def handleAddTeam(self, team):
        """ Handles a request to add a team """
        self.validate(team)
        ProdManagement.addTeam(team)
        return "Added team %s" % team

    @cherrypy.expose
    def versions(self):
        """ Lists all versions """
        versions = SoftwareAdmin.listSoftware().keys()
        versions.sort()
        for version in versions:
            WMCore.Lexicon.cmsswversion(version)
        return self.templatepage("Versions", versions=versions)

    @cherrypy.expose
    def handleAddVersion(self, version):
        """ Registers a version """
        WMCore.Lexicon.cmsswversion(version)
        SoftwareAdmin.addSoftware(version)
        return "Added version %s" % version

    @cherrypy.expose
    def handleAllVersions(self):
        """ Registers all versions in the TC """
        currentVersions = SoftwareAdmin.listSoftware().keys()

        allVersions = allSoftwareVersions()
        result = ""
        for version in allVersions:
            if not version in currentVersions:
                WMCore.Lexicon.cmsswversion(version)
                SoftwareAdmin.addSoftware(version)
                result += "Added version %s<br>" % version
        if result == "":
            result = "Version list is up to date"
        return result

