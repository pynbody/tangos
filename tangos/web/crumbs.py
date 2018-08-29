import socket
from six.moves.urllib import parse

class BreadCrumbs(object):
    def __init__(self, uri=None, servername="TANGOS"):
        if uri:
            self.uri = uri  # invoke uri.setter
        else:
            self._uri = uri  # set _uri to none
        self.servername = servername

    @property
    def uri(self):
        return self._uri

    @uri.setter
    def uri(self, uri):
        """set the _uri, _protocol, and crumbs attributes"""
        self._protocol = 'http://'
        protocols = ['http://', 'https://', 'ftp://', 'sftp://']
        for protocol in protocols:
            if uri.startswith(protocol):
                self._protocol = protocol
                uri = uri[len(protocol):]  # remove protocol from uri

        self._uri = uri.rstrip('/')
        self.crumbs = self._uri.split('/')

    @property
    def links(self):
        links = []
        for count, crumb in enumerate(self.crumbs, start=1):
            crumb_uri = self._protocol + '/'.join(self.crumbs[0:count])
            if count==1:
                crumb = self.servername
            if count<len(self.crumbs):
                links.append('<a href="' + crumb_uri + '" class="breadcrumbs">' + parse.unquote(crumb) + '</a>')
            else:
                links.append(crumb)
        return links

def breadcrumbs(request):
    c = BreadCrumbs(request.url, servername="TANGOS on "+socket.gethostname())
    return " &rarr; ".join(c.links)