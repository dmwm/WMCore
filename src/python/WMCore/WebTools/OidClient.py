import urllib, urllib2, cookielib

class OidClient():
    def __init__(self, oidserver='http://localhost:8400', username='', password=''):
        self.oidserver = oidserver
        self.username = username
        self.password = password
        cj = cookielib.CookieJar()
        self.opener = urllib2.build_opener(urllib2.HTTPCookieProcessor(cj))
        #urllib2.install_opener(opener)

    def open(self, url):
        resp = self.opener.open(url)
        if resp.geturl().startswith(self.oidserver+'/openidserver'):
            # Means that the oidserver responded with the login form
            login_data = urllib.urlencode({'identifier' : self.username, 'password' : self.password, 'submit' : 'login'})
            self.opener.open(self.oidserver+'/loginsubmit', login_data)

            # Continues from the point it was...
            resp = self.opener.open(resp.geturl())
        return resp

if __name__ == "__main__":
    cli=OidClient(oidserver='http://localhost:8400',username='simon',password='password')
    r=cli.open('http://localhost:8212/securedocumentation/')
    print r.read()
    print r.getcode()
 
