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
import datetime
import time
import urllib2
import grequests
import re
import collections
import math

import logging
LOGGER = logging.getLogger(__name__)

from . import core

API_ROOT = 'https://api.parse.com/1'
ACCESS_KEYS = {}

# Longest we'll wait for temp network or request errors to clear
MAX_ERROR_WAIT=60*10
# Wait period between attempts
ERROR_WAIT = 50

# Completely lame Parse, completely lame
MAX_PARSE_OFFSET = 10000

def chunks(l, n):
    """ Yield successive n-sized chunks from l.
    """
    for i in xrange(0, len(l), n):
        yield l[i:i+n]

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

class ConnectionException(Exception): pass

class Throttle(object):
    def __unicode__(self):
        return unicode(self.__class__.__name__)
    def __str__(self):
        return self.__unicode__().encode('utf-8')
    def __repr__(self):
        return self.__str__()

class NullThrottle(Throttle):
    
    def __enter__(self,*args,**kwargs):
        return
    def __exit__(self,exc_type, exc_val, exc_tb):
        return
    def calls_per(self,*args,**kwargs):
        return self

class TimeBasedThrottle(Throttle):
    def __init__(self,limit,period,calls_per_iteration=1):
        if period <= 0:
            raise ValueError('Throttle period should be greater than 0')
        if limit <= 0:
            raise ValueError('Throttle limit should be > 0')

        self.limit = limit
        self.period = period

        self.calls = collections.deque()
        # start with a full queue so we don't count the 0 second
        self.calls.extend(time.time() for x in xrange(0,limit))
        self.calls_per_iteration = calls_per_iteration

    def __unicode__(self):
        return u'<TimeBasedThrottle: Period=%s,limit=%s, remaining: %s' % (self.period,self.limit,self.max_calls)

    def __enter__(self):
        
        while self.calls_per_iteration > self.max_calls:
            time.sleep(.25)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type is None:
            self.calls.extend([time.time() for x in xrange(0,self.calls_per_iteration)])
            self.clean_calls()

    def calls_per(self,num_calls):
        """
            Return another throttle that assumes num_calls have been made per round
        """
        clone = self.__class__(limit=self.limit,period=self.period,calls_per_iteration=num_calls)
        clone.calls = self.calls
        return clone

    def clean_calls(self):
        # get rid of old, no longer relevant calls. 
        now = time.time()
        oldest_needed = now-self.period
        while True and self.calls:
            if self.calls[0] < oldest_needed:
                self.calls.popleft()
            else:
                break

    @property
    def max_calls(self):
        self.clean_calls()
        return int(math.floor(float(self.limit) - len(self.calls)))

DEFAULT_THROTTLE = NullThrottle()

class ParseBase(object):
    ENDPOINT_ROOT = API_ROOT

    @classmethod
    def execute(cls, uri, http_verb, extra_headers=None, batch=False, _app_id=None,_user=None,_throttle=None,_high_volume=False,retry_on_temp_error=True,error_wait=ERROR_WAIT,max_error_wait=MAX_ERROR_WAIT,**kw):
        """
        if batch == False, execute a command with the given parameters and
        return the response JSON.
        If batch == True, return the dictionary that would be used in a batch
        command.
        """

        if not _throttle:
            _throttle = DEFAULT_THROTTLE

        if isinstance(_throttle,NullThrottle):
            import pdb
            pdb.set_trace()

        if batch:
            ret = {"method": http_verb,
                   "path": uri.split("parse.com")[1]}
            if kw:
                ret["body"] = kw
            return ret

        keys = get_keys(_app_id)
        
        if not keys or not 'app_id' in keys or not 'rest_key' in keys:
            raise core.ParseError('Missing connection credentials')

        app_id = keys.get('app_id')
        rest_key = keys.get('rest_key')
        master_key = keys.get('master_key')

        headers = extra_headers or {}
        headers['Content-type']='application/json'
        headers['X-Parse-Application-Id']=app_id
        headers['X-Parse-REST-API-Key']=rest_key
        
        if _user:
            if _user.is_master():
                if not master_key:
                    raise core.ParseError('Missing requested master key')
                elif 'X-Parse-Session-Token' not in headers.keys():
                    headers['X-Parse-Master-Key']= master_key
            else:
                if not _user.is_authenticated():
                    _user.authenticate()
                headers['X-Parse-Session-Token']=_user.sessionToken

        url = uri if uri.startswith(API_ROOT) else cls.ENDPOINT_ROOT + uri

        data = kw and json.dumps(kw) or "{}"
        
        if http_verb == 'GET' and data:
            new_url = '%s?%s' % (url,urlencode(kw))

            # deal with parse's crappy URL length limit that throws 
            # 502s without any other helpful message. The current real limit seems
            # to be ~7800
            if len(new_url) > 5000:
                http_verb = 'POST'
                headers['X-HTTP-Method-Override']='GET'
                if 'limit' in kw:
                    # it appears that limit needs to be in the URL?!
                    url += '?%s' % urlencode({'limit':kw.get('limit')})                
            else:
                url = new_url
                data = None

        if not _high_volume:
            return cls._serial_execute(http_verb,url,data,headers,retry_on_temp_error,error_wait,max_error_wait,_throttle)
        else:
            return cls._concurrent_execute(http_verb,url,data,headers,_throttle)

    @classmethod
    def _serial_execute(cls,http_verb,url,data,headers,retry_on_temp_error,error_wait,max_error_wait,_throttle):
        request = Request(url, data, headers)
        request.get_method = lambda: http_verb

        start_time = datetime.datetime.now()
        
        while 1:
            try:
                with _throttle:
                    response = urlopen(request)
                return json.loads(response.read())
            except HTTPError as e:
                exc = {
                    400: core.ResourceRequestBadRequest,
                    401: core.ResourceRequestLoginRequired,
                    403: core.ResourceRequestForbidden,
                    404: core.ResourceRequestNotFound
                    }.get(e.code, core.ParseError)
                if exc != core.ParseError:
                    raise exc(e.read())
                else:
                    raise exc(e.code,e.read(),e)
            except urllib2.URLError as e:
                if not retry_on_temp_error:
                    raise

                now = datetime.datetime.now()
                if max_error_wait == 0 or (now - start_time).sections <= max_error_wait:
                    LOGGER.warn('Temp error during execute(). Waiting %s: %s' % (error_wait,e))
                    time.sleep(error_wait)
                else:
                    LOGGER.error('Temp errors for too long. Bailing due to: %s' % e)
                    raise

    @classmethod
    def _concurrent_execute(cls,http_verb,url,data,headers,_throttle):
        # Error handling in grequests is non-existent. We just try three times and call it a day
        reqs = []
        for offset in xrange(0,MAX_PARSE_OFFSET+1000,1000):
            if data:
                data['skip'] = offset
            else:
                if 'skip' not in url:
                    if '?' not in url:
                        url += '?'
                    url += '&skip=%s' % offset
                else:
                    url = re.sub('skip=[0-9]+','skip=%s' % offset,url)
            reqs.append( getattr(grequests,http_verb.lower())(url,data=data,headers=headers) )

        cur_reqs = reqs[:]
        res = {'results':[],'count':0}
        for i in xrange(0,3):
            with _throttle.calls_per(len(cur_reqs)):
                grequests.map(cur_reqs)
            c = cur_reqs[:]
            for i in c:
                if i.response:
                    # Not this one
                    cur_reqs.remove(i)
            if not cur_reqs:
                # they all succeeded
                for req in reqs:
                    resp = json.loads(req.response.content)
                    res['results'].extend(resp.get('results',[]))
                    res['count'] += resp.get('count',0)
                return res

            # else try again

        # 3 attempts and they didn't succeed
        raise ConnectionException('%s of %s requests failed' % (len(cur_reqs),len(reqs)) )

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

    def batch(self, methods,_using=None,_as_user=None,_throttle=None):
        """
        Given a list of create, update or delete methods to call, call all
        of them in a single batch operation.
        """

        # parse has a 50 record limit in batch mode
        for thisBatch in chunks(methods,50):
            # It's not necessary to pass in using and as_users here since this eventually
            # calls execute() with the batch flag, which doesn't actually do a callout
            queries, callbacks = zip(*[m(batch=True) for m in thisBatch])
            # perform all the operations in one batch
            responses = self.execute("", "POST", requests=queries,_app_id=_using,_user=_as_user,_throttle=_throttle)
            # perform the callbacks with the response data (updating the existing
            # objets, etc)
            for callback, response in zip(callbacks, responses):
                if response.has_key('error'):
                    raise core.ParseError('Error: %s' % response['error'])
                callback(response["success"])

    def batch_save(self, objects,_using=None,_as_user=None,_throttle=None):
        """save a list of objects in one operation"""
        self.batch([o.save for o in objects],_using=_using,_as_user=_as_user,_throttle=_throttle)

    def batch_delete(self, objects,_using=None,_as_user=None,_throttle=None):
        """delete a list of objects in one operation"""
        self.batch([o.delete for o in objects],_using=_using,_as_user=_as_user,_throttle=_throttle)


