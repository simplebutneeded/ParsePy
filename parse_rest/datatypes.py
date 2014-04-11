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

import base64
import datetime
import copy
import json

from connection import API_ROOT, ParseBase
from query import QueryManager


class ParseType(object):

    @staticmethod
    def convert_from_parse(parse_data,_using=None,_as_user=None):
        is_parse_type = isinstance(parse_data, dict) and '__type' in parse_data
        if not is_parse_type:
            return parse_data

        parse_type = parse_data['__type']

        # Tag Relations and Pointers with their source app ID
        # in case they're retrieved later
        #if using:
        #    if parse_type == 'Relation' or parse_type == 'Pointer':
        #        parse_data['_using'] = using
        #        parse_data['_as_user'] = as_user

        native = {
            'Pointer': Pointer,
            'Date': Date,
            'Bytes': Binary,
            'GeoPoint': GeoPoint,
            'File': File,
            'Relation': Relation
            }.get(parse_type)

        return native and native.from_native(_using=_using,_as_user=_as_user,**parse_data) or parse_data

    @staticmethod
    def convert_to_parse(python_object, as_pointer=False):
        is_object = isinstance(python_object, Object)

        if is_object and not as_pointer:
            return dict([(k, ParseType.convert_to_parse(v, as_pointer=True))
                         for k, v in python_object._editable_attrs.items()
                         ])

        python_type = Object if is_object else type(python_object)

        # classes that need to be cast to a different type before serialization
        transformation_map = {
            datetime.datetime: Date,
            Object: Pointer
            }

        if python_type in transformation_map:
            klass = transformation_map.get(python_type)
            return klass(python_object)._to_native()

        if isinstance(python_object, ParseType):
            return python_object._to_native()

        return python_object

    @classmethod
    def from_native(cls, **kw):
        return cls(**kw)

    def _to_native(self):
        return self._value

    def serialize(self):
        return self._to_native()
        
class ForeignKey(object):
    def __init__(self,cls,name):
        self.name = name
        self.cls = cls
    def __get__(self, instance, owner=None):
        obj = getattr(instance,'_'+self.name+'_obj',None)
        if obj:
            # if we queried this, it only has the objectId
            if hasattr(obj,'_loaded') and not getattr(obj,'_loaded'):
                obj = self.cls.retrieve(oid,_using=instance._using,_as_user=instance._as_user)
                obj._loaded = True
                setattr(instance,'_'+self.name+'_obj',obj)
                setattr(instance,'_'+self.name+'_id',obj.objectId)
            return obj

        oid = getattr(instance,self.name+'_id',None)
        if not oid:
            return None
        obj = self.cls.retrieve(oid,_using=instance._using,_as_user=instance._as_user)
        setattr(instance,'_'+self.name+'_obj',obj)
        setattr(instance,'_'+self.name+'_id',obj.objectId)
        return obj
    def __set__(self, instance, value):
        #instance.__dict__[self.name] = value
        if isinstance(value,basestring):
            setattr(instance,'_'+self.name+'_id',value)
        else:
            setattr(instance,'_'+self.name+'_obj',value)

class Pointer(ParseType):

    @classmethod
    def from_native(cls, _using=None,_as_user=None,**kw):
        klass = Object.factory(kw.get('className'))
        # This would have been added during the query so we know
        # which data store it came from
        #app_id = kw.get('_app_id',None)
        #user   = kw.get('_user',None)
        o = klass(**kw)
        if set(kw.keys()) == set(['objectId','className','__type']):
            # not really loaded, just the id
            o._loaded = False
        else:
            o._loaded = True
        o._using = _using
        o._as_user = _as_user
        return o
        #return kw.get('objectId')

    def __init__(self, obj):
        self._object = obj

    def _to_native(self):
        return {
            '__type': 'Pointer',
            'className': self._object.parse_table or self._object.__class__.__name__,
            'objectId': self._object.objectId
            }


class Relation(ParseType):
    @classmethod
    def from_native(cls, **kw):
        pass


class Date(ParseType):
    FORMAT = '%Y-%m-%dT%H:%M:%S.%f%Z'

    @classmethod
    def from_native(cls, **kw):
        return cls._from_str(kw.get('iso', ''))

    @staticmethod
    def _from_str(date_str):
        """turn a ISO 8601 string into a datetime object"""
        return datetime.datetime.strptime(date_str[:-1] + 'UTC', Date.FORMAT)

    def __init__(self, date):
        """Can be initialized either with a string or a datetime"""
        if isinstance(date, datetime.datetime):
            self._date = date
        elif isinstance(date, unicode):
            self._date = Date._from_str(date)

    def _to_native(self):
        return {
            '__type': 'Date', 'iso': self._date.isoformat()
            }


class Binary(ParseType):

    @classmethod
    def from_native(cls, **kw):
        return cls(kw.get('base64', ''))

    def __init__(self, encoded_string):
        self._encoded = encoded_string
        self._decoded = str(base64.b64decode(self._encoded))

    def _to_native(self):
        return {'__type': 'Bytes', 'base64': self._encoded}


class GeoPoint(ParseType):

    @classmethod
    def from_native(cls, **kw):
        return cls(kw.get('latitude'), kw.get('longitude'))

    def __init__(self, latitude, longitude):
        self.latitude = latitude
        self.longitude = longitude

    def _to_native(self):
        return {
            '__type': 'GeoPoint',
            'latitude': self.latitude,
            'longitude': self.longitude
            }


class File(ParseType):

    @classmethod
    def from_native(cls, **kw):
        return cls(**kw)

    def __init__(self, **kw):
        name = kw.get('name')
        self._name = name
        self._api_url = '/'.join([API_ROOT, 'files', name])
        self._file_url = kw.get('url')

    def _to_native(self):
        return {
            '__type': 'File',
            'name': self._name
            }

    url = property(lambda self: self._file_url)
    name = property(lambda self: self._name)
    _absolute_url = property(lambda self: self._api_url)


class Function(ParseBase):
    ENDPOINT_ROOT = '/'.join((API_ROOT, 'functions'))

    def __init__(self, name):
        self.name = name

    def __call__(self, _using=None,_as_user=None,**kwargs):
        return self.POST('/' + self.name, _app_id=_using,_as_user=_as_user,**kwargs)


class ParseResource(ParseBase, Pointer):

    PROTECTED_ATTRIBUTES = ['objectId', 'createdAt', 'updatedAt']

    # __USER__ will either be deleted or 
    # replaced with the user ID from as_user (if it exists)
    DEFAULT_ACL = {'__USER__':{"write":True,"read":True}}

    @classmethod
    def retrieve(cls, resource_id,_using=None,_as_user=None):
        return cls(**dict(cls.GET('/' + resource_id,_app_id=_using,_user=_as_user),_using=_using,_as_user=_as_user) )

    @property
    def _editable_attrs(self):
        protected_attrs = self.__class__.PROTECTED_ATTRIBUTES
        allowed = lambda a: a not in protected_attrs and not a.startswith('_')
        return dict([(k, v) for k, v in self.__dict__.items() if allowed(k)])

    def __init__(self, _using=None,_as_user=None,**kw):
        for key, value in kw.items():
            a = ParseType.convert_from_parse(value,_using=_using,_as_user=_as_user)
            setattr(self, key, a)
        self._using = _using
        self._as_user = _as_user



    def _to_native(self):
        if not isinstance(self,Pointer):
            return ParseType.convert_to_parse(self)
        else:
            if hasattr(self,'_loaded') and not getattr(self,'_loaded',None):
                return ParseType.convert_to_parse(self,as_pointer=True)
            else:
                return ParseType.convert_to_parse(self)   

    def _get_object_id(self):
        return self.__dict__.get('_object_id')

    def _set_object_id(self, value):
        if '_object_id' in self.__dict__ and value != self._object_id:
            raise ValueError('Can not re-set object id')
        self._object_id = value

    def _get_updated_datetime(self):
        return self.__dict__.get('_updated_at') and self._updated_at._date

    def _set_updated_datetime(self, value):
        self._updated_at = Date(value)

    def _get_created_datetime(self):
        return self.__dict__.get('_created_at') and self._created_at._date

    def _set_created_datetime(self, value):
        self._created_at = Date(value)

    def save(self, batch=False,_using=None,_as_user=None):
        using = _using or getattr(self,'_using',None)
        as_user = _as_user or getattr(self,'_as_user',None)
        if self.objectId:
            return self._update(batch=batch,_using=using,_as_user=as_user)
        else:
            return self._create(batch=batch,_using=using,_as_user=as_user)

    def _create(self, batch=False,_using=None,_as_user=None):
        uri = self.__class__.ENDPOINT_ROOT
        response = self.__class__.POST(uri, batch=batch, _app_id=_using,_user=_as_user,**self._to_native())

        if not hasattr(self,'ACL') or self.ACL is None:
            self.ACL = copy.copy(self.DEFAULT_ACL)
            if _as_user:
                if self.ACL.has_key('__USER__'):
                    if not _as_user.is_authenticated():
                        _as_user.authenticate()
                    self.ACL[_as_user.id]=self.ACL['__USER__']
                    del self.ACL['__USER__']
            else:
                del self.ACL['__USER__']

        def call_back(response_dict):
            self.createdAt = self.updatedAt = response_dict['createdAt']
            self.objectId = response_dict['objectId']
            self.id = self.objectId

        if batch:
            return response, call_back
        else:
            call_back(response)

    def _update(self, batch=False,_using=None,_as_user=None):
        response = self.__class__.PUT(self._absolute_url, batch=batch,
                                      _app_id=_using,_user=_as_user,**self._to_native())

        def call_back(response_dict):
            self.updatedAt = response_dict['updatedAt']

        if batch:
            return response, call_back
        else:
            call_back(response)

    def delete(self, batch=False,_using=None,_as_user=None):
        response = self.__class__.DELETE(self._absolute_url, batch=batch,_app_id=_using,_user=_as_user)
        def call_back(response_dict):
            self.__dict__ = {}

        if batch:
            return response, call_back
        else:
            call_back(response)

    _absolute_url = property(
        lambda self: '/'.join([self.__class__.ENDPOINT_ROOT, self.objectId])
        )

    objectId = property(_get_object_id, _set_object_id)
    createdAt = property(_get_created_datetime, _set_created_datetime)
    updatedAt = property(_get_updated_datetime, _set_updated_datetime)

    def __repr__(self):
        return '<%s:%s>' % (unicode(self.parse_table or self.__class__.__name__), self.objectId)   

class ObjectMetaclass(type):
    def __new__(cls, name, bases, dct):
        cls = super(ObjectMetaclass, cls).__new__(cls, name, bases, dct)
        cls.set_endpoint_root()
        cls.Query = QueryManager(cls)
        return cls


class Object(ParseResource):
    __metaclass__ = ObjectMetaclass
    parse_table = None
    ENDPOINT_ROOT = '/'.join([API_ROOT, 'classes'])

    @classmethod
    def factory(cls, class_name):
        class DerivedClass(cls):
            pass
        DerivedClass.__name__ = str(class_name)
        DerivedClass.set_endpoint_root()
        return DerivedClass

    @classmethod
    def set_endpoint_root(cls):
        root = '/'.join([API_ROOT, 'classes', cls.parse_table or cls.__name__])
        if cls.ENDPOINT_ROOT != root:
            cls.ENDPOINT_ROOT = root
        return cls.ENDPOINT_ROOT

    @property
    def _absolute_url(self):
        if not self.objectId: return None
        return '/'.join([self.__class__.ENDPOINT_ROOT, self.objectId])

    @property
    def as_pointer(self):
        return Pointer(**{
                'className': self.__class__.__name__,
                'objectId': self.objectId
                })

    def serialize(self):
        vals = {'pk':getattr(self,'objectId',None),
                '__type':self.parse_table or self.__class__.__name__,
                'objectId':self.objectId,
                'createdAt':self.createdAt,
                'updatedAt':self.updatedAt}
        for key,val in self.__dict__.items():
            if key.startswith('_'):
                continue
            if isinstance(val,ParseResource):
                oid = getattr(self,key+'_id',None)
                vals[key] = {'pk':oid,'__type':val.parse_table or val.cls.__name__,'objectId':oid}
            elif isinstance(val,Object) or hasattr(val,'serialize'):
                vals[key] = val.serialize()
            else:
                vals[key] = val
        return vals


    def increment(self, key, amount=1,_using=None,_as_user=None):
        """
        Increment one value in the object. Note that this happens immediately:
        it does not wait for save() to be called
        """
        payload = {
            key: {
                '__op': 'Increment',
                'amount': amount
                }
            }
        self.__class__.PUT(self._absolute_url, _app_id=_using,_user=_as_user,**payload)
        self.__dict__[key] += amount
