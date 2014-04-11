#    This program is free software: you can redistribute it and/or modify
#    it under the terms of the GNU General Public License as published by
#    the Free Software Foundation, either version 3 of the License, or
#    (at your option) any later version.
#
#    This program is distributed in the hope that it will be useful,
#    but WITHOUT ANY WARRANTY; without even the implied warranty of
#    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#    GNU General Public License for more details.
#
#    You should have received a copy of the GNU General Public License
#    along with this program.  If not, see <http://www.gnu.org/licenses/>.

try:
    from urllib2 import Request, urlopen, HTTPError
    from urllib import urlencode
except ImportError:
    # is Python3
    from urllib.request import Request, urlopen
    from urllib.error import HTTPError
    from urllib.parse import urlencode

import json

import core

API_ROOT = 'https://api.parse.com/1'
ACCESS_KEYS = {}

def register(app_id, rest_key, **kw):
    '''
        Register one or more sets of keys by app_id. If only one set 
        is registered, that set is used automatically.
    '''
    global ACCESS_KEYS

    if not ACCESS_KEYS:
        ACCESS_KEYS = { 
                        'default': {
                            'app_id': app_id,
                            'rest_key': rest_key
                         }
                      }
        ACCESS_KEYS['default'].update(**kw)
    
    ACCESS_KEYS[app_id] =  {
        'app_id':app_id,
        'rest_key':rest_key
    }
    ACCESS_KEYS[app_id].update(**kw)

def get_keys(app_id):
    '''
        Return keys associated with app_id, or the default set if
        app_id is None
    '''

    if not app_id:
        return ACCESS_KEYS.get('default')
    else:
        return ACCESS_KEYS.get(app_id)

"""
DOESN'T SUPPORT MULTIPLE KEY SETS
def master_key_required(func):
    '''decorator describing methods that require the master key'''
    def ret(obj, *args, **kw):
        conn = ACCESS_KEYS
        if not (conn and conn.get('master_key')):
            message = '%s requires the master key' % func.__name__
            raise core.ParseError(message)
        func(obj, *args, **kw)
    return ret
"""

class ParseBase(object):
    ENDPOINT_ROOT = API_ROOT

    @classmethod
    def execute(cls, uri, http_verb, extra_headers=None, batch=False, app_id=None,user=None,**kw):
        """
        if batch == False, execute a command with the given parameters and
        return the response JSON.
        If batch == True, return the dictionary that would be used in a batch
        command.
        """
        if batch:
            ret = {"method": http_verb,
                   "path": uri.split("parse.com")[1]}
            if kw:
                ret["body"] = kw
            return ret

        keys = get_keys(app_id)
        
        if not 'app_id' in keys or not 'rest_key' in keys:
            raise core.ParseError('Missing connection credentials')

        app_id = keys.get('app_id')
        rest_key = keys.get('rest_key')
        master_key = keys.get('master_key')

        headers = extra_headers or {}
        url = uri if uri.startswith(API_ROOT) else cls.ENDPOINT_ROOT + uri
        data = kw and json.dumps(kw) or "{}"
        if http_verb == 'GET' and data:
            url += '?%s' % urlencode(kw)
            data = None

        request = Request(url, data, headers)
        request.add_header('Content-type', 'application/json')
        request.add_header('X-Parse-Application-Id', app_id)
        request.add_header('X-Parse-REST-API-Key', rest_key)

        if user:
            if user.is_master():
                if not master_key:
                    raise core.ParseError('Missing requested master key')
                elif 'X-Parse-Session-Token' not in headers.keys():
                    request.add_header('X-Parse-Master-Key', master_key)
            else:
                if not user.is_authenticated():
                    user.authenticate()
                request.add_header('X-Parse-Session-Token',user.sessionToken)

        request.get_method = lambda: http_verb

        try:
            response = urlopen(request)
        except HTTPError as e:
            exc = {
                400: core.ResourceRequestBadRequest,
                401: core.ResourceRequestLoginRequired,
                403: core.ResourceRequestForbidden,
                404: core.ResourceRequestNotFound
                }.get(e.code, core.ParseError)
            raise exc(e.read())

        return json.loads(response.read())
        

    @classmethod
    def GET(cls, uri, **kw):
        return cls.execute(uri, 'GET', **kw)

    @classmethod
    def POST(cls, uri, **kw):
        return cls.execute(uri, 'POST', **kw)

    @classmethod
    def PUT(cls, uri, **kw):
        return cls.execute(uri, 'PUT', **kw)

    @classmethod
    def DELETE(cls, uri, **kw):
        return cls.execute(uri, 'DELETE', **kw)


class ParseBatcher(ParseBase):
    """Batch together create, update or delete operations"""
    ENDPOINT_ROOT = '/'.join((API_ROOT, 'batch'))

    def batch(self, methods,using=None,as_user=None):
        """
        Given a list of create, update or delete methods to call, call all
        of them in a single batch operation.
        """

        # It's not necessary to pass in using and as_users here since this eventually
        # calls execute() with the batch flag, which doesn't actually do a callout
        queries, callbacks = zip(*[m(batch=True) for m in methods])
        # perform all the operations in one batch
        responses = self.execute("", "POST", requests=queries,app_id=using,user=as_user)
        # perform the callbacks with the response data (updating the existing
        # objets, etc)
        for callback, response in zip(callbacks, responses):
            if response.has_key('error'):
                raise core.ParseError('Error: %s' % response['error'])
            callback(response["success"])

    def batch_save(self, objects,using=None,as_user=None):
        """save a list of objects in one operation"""
        self.batch([o.save for o in objects],using=using,as_user=as_user)

    def batch_delete(self, objects,using=None,as_user=None):
        """delete a list of objects in one operation"""
        self.batch([o.delete for o in objects],using=using,as_user=as_user)
