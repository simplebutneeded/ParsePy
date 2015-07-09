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
import logging
LOGGER = logging.getLogger(__name__)

from core import ResourceRequestLoginRequired
from connection import API_ROOT
from datatypes import ParseResource, ParseType,Function
from query import QueryManager
from . import datatypes


def login_required(func):
    '''decorator describing User methods that need to be logged in'''
    def ret(obj, *args, **kw):
        if not hasattr(obj, 'sessionToken'):
            message = '%s requires a logged-in session' % func.__name__
            raise ResourceRequestLoginRequired(message)
        return func(obj, *args, **kw)
    return ret


class User(ParseResource):
    '''
    A User is like a regular Parse object (can be modified and saved) but
    it requires additional methods and functionality
    '''
    ENDPOINT_ROOT = '/'.join([API_ROOT, 'users'])
    PROTECTED_ATTRIBUTES = ParseResource.PROTECTED_ATTRIBUTES + [
        'username', 'sessionToken']

    # 
    _is_master = False
    parse_table = '_User'

    # Used when creating a user from python for use in as_user() calls
    _password = None
    username = None
    objectId = None
    sessionToken = None

    def is_authenticated(self):
        return self.is_master() or self.sessionToken is not None

    def authenticate(self, password=None, session_token=None):
        if self.is_authenticated(): return

        if password is None and self._password:
            password = self._password

        if password is not None:
            self = User.login(self.username, password)

        user = User.retrieve(self.objectId)
        if user.objectId == self.objectId and user.sessionToken == session_token:
            self.sessionToken = session_token

    def set_master(self,master):
        self._is_master = master
    def is_master(self):
        return self._is_master
        
    @login_required
    def session_header(self):
        return {'X-Parse-Session-Token': self.sessionToken}

    @login_required
    def save(self,_using=None,_as_user=None,batch=False,_throttle=None):
        extra  ={}
        if not _as_user:
            extra = {'X-Parse-Session-Token': self.sessionToken}
        url = self._absolute_url
        data = self._to_native()
        return self.__class__.PUT(url, _app_id=_using, _user=_as_user, batch=batch, _throttle=_throttle, extra_headers=extra, **data)

    @login_required
    def delete(self):
        session_header = {'X-Parse-Session-Token': self.sessionToken}
        return self.DELETE(self._absolute_url, extra_headers=session_header)

    @staticmethod
    def signup(username, password, **kw):
        response_data = User.POST('', username=username, password=password, **kw)
        response_data.update({'username': username})
        return User(**response_data)

    @staticmethod
    def login(username, passwd,app_id=None):
        login_url = '/'.join([API_ROOT, 'login'])
        return User(**User.GET(login_url, username=username, password=passwd,_app_id=app_id))

    @staticmethod
    def login_auth(auth):
        login_url = User.ENDPOINT_ROOT
        return User(**User.POST(login_url, authData=auth))

    @staticmethod
    def become(user_id,app_id=None):
        """ Parse should support this natively. Of course, parse sucks and doesn't
            To use this, you must implement sessionForUser in parse_rest/cloudcode/cloud/main.js
        """
        u = User()
        u.set_master(True)
        try:
            f = Function('sessionForUser')
            resp = f(userId=user_id,_using=app_id,_as_user=u)
        except Exception as e:
            LOGGER.error('become() received error {0}'.format(e))
            return None
        u.sessionToken = resp.get('result',{}).get('session')
        u.objectId = user_id
        if not u.sessionToken:
            LOGGER.error('become did not receive sessionToken: {0}'.format(resp))
            return None
        return u

    @staticmethod
    def request_password_reset(email):
        '''Trigger Parse\'s Password Process. Return True/False
        indicate success/failure on the request'''

        url = '/'.join([API_ROOT, 'requestPasswordReset'])
        try:
            User.POST(url, email=email)
            return True
        except:
            return False

    def _to_native(self):
        return dict([(k, ParseType.convert_to_parse(v, as_pointer=True))
                     for k, v in self._editable_attrs.items()])

    def __repr__(self):
        return '<User:%s (Id %s)>' % (self.username, self.objectId)


User.Query = QueryManager(User)

class Role(datatypes.Object):
    ENDPOINT_ROOT = '/'.join([API_ROOT, 'roles'])
    parse_table = '_Role'

Role.Query = QueryManager(Role)
