import urllib, urllib2, cookielib

class OidClient():
    def __init__(self, oidserver='http://localhost:8400', username='', password=''):
        self.oidserver = oidserver
        self.username = username
        self.password = password
        cj = cookielib.CookieJar()
        self.opener = urllib2.build_opener(urllib2.HTTPCookieProcessor(cj))
        #urllib2.install_opener(opener)

    def open(self, url, data=None, timeout=3):
        """
        Opens a url (which can be a urllib2.Request object or a string)

        data is a string specifying additional data to send to the server
        If provided, the HTTP request will be a POST (instead of GET).
        It should be a buffer in the standard application/x-www-form-urlencoded
        format (use urllib.urlencode() for that)

        timeout in seconds for blocking operations like the connection attempt
        Defaults to 3 seconds. Note that an openid request 
        may require several step (thus, several timeouts).

        returns an file-like object (same as urllib2.urlopen())
        """
        resp = self.opener.open(url,data,timeout)
        if resp.geturl().startswith(self.oidserver+'/openidserver'):
            # Means that the oidserver responded with the login form
            login_data = urllib.urlencode({'identifier' : self.username, 'password' : self.password, 'submit' : 'login'})
            self.opener.open(self.oidserver+'/loginsubmit', login_data, timeout)

            # Continues from the point it was...
            resp = self.opener.open(resp.geturl(),data,timeout)
        return resp

if __name__ == "__main__":
    cli=OidClient(oidserver='http://localhost:8400',username='simon',password='password')
    r=cli.open('http://localhost:8212/securedocumentation/')
    print r.read()
    print r.getcode()
 
