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
import WMCore.HTTPFrontEnd.RequestManager.ReqMgrWebTools as Utilities
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
        self.htmldir  = config.html
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
    @cherrypy.tools.secmodv2()
    def index(self):
        """ Main web page """
        return Utilities.serveFile('text/html', self.htmldir, "Admin.html")

    @cherrypy.expose
    @cherrypy.tools.secmodv2()
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
    @cherrypy.tools.secmodv2(role=Utilities.security_roles(), group = Utilities.security_groups())
    def handleUserPriority(self, user, userPriority):
        """ Handles setting user priority """
        self.validate(user)
        UserManagement.setPriority(user, userPriority)
        return "Updated user %s priority to %s" % (user, userPriority)

    @cherrypy.expose
    @cherrypy.tools.secmodv2()
    def group(self, groupName):
        """ Web page of details about the user, and sets user priority """
        self.validate(groupName)
        users = GroupInfo.usersInGroup(groupName)
        priority = GroupManagement.getPriority(groupName)
        return self.templatepage("Group", group=groupName, users=users, priority=priority)

    @cherrypy.expose
    @cherrypy.tools.secmodv2(role=Utilities.security_roles(), group = Utilities.security_groups())
    def handleGroupPriority(self, group, groupPriority):
        """ Handles setting group priority """
        self.validate(group)
        GroupManagement.setPriority(group, groupPriority)
        return "Updated group %s priority to %s" % (group, groupPriority)

    @cherrypy.expose
    @cherrypy.tools.secmodv2()
    def users(self):
        """ Lists all users.  Should be paginated later """
        allUsers = Registration.listUsers()
        self.validate(allUsers)
        return self.templatepage("Users", users=allUsers)

    @cherrypy.expose
    @cherrypy.tools.secmodv2(role=Utilities.security_roles(), group = Utilities.security_groups())
    def handleAddUser(self, user, email=None):
        """ Handles setting user priority """
        self.validate(user)
        Registration.registerUser(user, email)
        return "Added user %s" % user

    @cherrypy.expose
    @cherrypy.tools.secmodv2(role=Utilities.security_roles(), group = Utilities.security_groups())
    def handleAddToGroup(self, user, group):
        """ Adds a user to the group """
        self.validate(user)
        self.validate(group)
        GroupManagement.addUserToGroup(user, group)
        return "Added %s to %s " % (user, group)

    @cherrypy.expose
    @cherrypy.tools.secmodv2()
    def groups(self):
        """ Lists all users.  Should be paginated later """
        allGroups = GroupInfo.listGroups()
        self.validate(allGroups)
        return self.templatepage("Groups", groups=allGroups)

    @cherrypy.expose
    @cherrypy.tools.secmodv2(role=Utilities.security_roles(), group = Utilities.security_groups())
    def handleAddGroup(self, group):
        """ Handles adding a group """
        self.validate(group)
        GroupManagement.addGroup(group)
        return "Added group %s " % group

    @cherrypy.expose
    @cherrypy.tools.secmodv2()
    def teams(self):
        """ Lists all teams """
        teams = ProdManagement.listTeams().keys()
        self.validate(teams)
        return self.templatepage("Teams", teams = teams)

    @cherrypy.expose
    @cherrypy.tools.secmodv2()
    def team(self, teamName):
        """ Details for a team """
        self.validate(teamName)
        assignments = ListRequests.listRequestsByTeam(teamName)
        if assignments == None:
            assignments = []
        else:
            assignments = assignments.keys()
        self.validate(assignments)
        return self.templatepage("Team", team=teamName, requests=assignments)

    @cherrypy.expose
    @cherrypy.tools.secmodv2(role=Utilities.security_roles(), group = Utilities.security_groups())
    def handleAddTeam(self, team):
        """ Handles a request to add a team """
        self.validate(team)
        ProdManagement.addTeam(team)
        return "Added team %s" % team

    @cherrypy.expose
    @cherrypy.tools.secmodv2()
    def versions(self):
        """ Lists all versions """
        archList = SoftwareAdmin.listSoftware()
        versions = []
        for versionList in archList.values():
            for version in versionList:
                if not version in versions:
                    versions.append(version)
        versions.sort()
        for version in versions:
            WMCore.Lexicon.cmsswversion(version)
        return self.templatepage("Versions", versions=versions)

    @cherrypy.expose
    @cherrypy.tools.secmodv2()
    def scramArchs(self):
        """
        _scramArchs_

        List all scramArchs in the DB
        Prelim for putting this in the template pages
        """
        return SoftwareAdmin.listSoftware().keys()

    @cherrypy.expose
    @cherrypy.tools.secmodv2(role=Utilities.security_roles(), group = Utilities.security_groups())
    def handleAddVersion(self, version):
        """ Registers a version """
        WMCore.Lexicon.cmsswversion(version)
        SoftwareAdmin.updateSoftware(version)
        return "Added version %s" % version

    @cherrypy.expose
    @cherrypy.tools.secmodv2(role=Utilities.security_roles(), group = Utilities.security_groups())
    def handleAllVersions(self):
        """ Registers all versions in the TC """
        Utilities.updateScramArchsAndCMSSWVersions()
        return "Updated versions to current standard"
