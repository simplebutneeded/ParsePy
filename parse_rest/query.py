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

import json
import collections
import copy

try:
    unicode = unicode
except NameError:
    unicode = str

class QueryResourceDoesNotExist(Exception):
    '''Query returndtBCnOCz4bed no results'''
    pass


class QueryResourceMultipleResultsReturned(Exception):
    '''Query was supposed to return unique result, returned more than one'''
    pass

class BadQueryParametersException(Exception):
    ''' Bad query args '''
    pass

class QueryManager(object):

    def __init__(self, model_class):
        self.model_class = model_class

    def _fetch(self, **kw):
        using = None
        if kw.has_key('_using'):
            using = kw.get('_using')
            del kw['_using']
        as_user = None
        if kw.has_key('_as_user'):
            as_user = kw.get('_as_user')
            del kw['_as_user']
        high_volume = False
        if kw.has_key('_high_volume'):
            high_volume = kw.get('_high_volume')
            del kw['_high_volume']
        values_list = kw.get('_values_list')
        if values_list:
            del kw['_values_list']
        values = kw.get('_values')
        if values:
            del kw['_values']
        throttle = None
        if kw.has_key('_throttle'):
            throttle = kw.get('_throttle')
            del kw['_throttle']

        klass = self.model_class
        uri = self.model_class.ENDPOINT_ROOT

        # This is to compensate for Parse's 1k query limit
        done = False
        limit = kw.get('limit',1000)
        kw['limit'] = limit
        offset = kw.get('skip',0)
        kw['skip'] = offset
        results = []
        if not high_volume:
            while not done:
                if not (values_list or values):
                    new_res = [klass(_using=using,_as_user=as_user,_throttle=throttle,**it) for it in klass.GET(uri, _app_id=using,_user=as_user,_throttle=throttle,**kw).get('results')]
                elif values_list:
                    new_res = [[it[y] for y in values_list] for it in klass.GET(uri, _app_id=using,_user=as_user,**kw).get('results')]
                elif values:
                    new_res = klass.GET(uri, _app_id=using,_user=as_user,_throttle=throttle,**kw).get('results')
                    
                results.extend(new_res)
                if len(new_res) < limit or limit < 1000:
                    done = True
                else:
                    offset += 1000
                    kw['skip'] = offset

                if offset > 10000:
                    # parse can't handle offsets > 10k without a serious hack (order_by)
                    done = True
            return results
        else:
            # high_volume will cause 11 requests to be send concurently
            if not (values_list or values):
                return [klass(_using=using,_as_user=as_user,_throttle=throttle,**it) for it in klass.GET(uri, _app_id=using,_user=as_user,_throttle=throttle,_high_volume=high_volume,**kw).get('results')]
            elif values_list:
                return [[it[y] for y in values_list] for it in klass.GET(uri, _app_id=using,_user=as_user,_throttle=throttle,_high_volume=high_volume,**kw).get('results')]
            elif values:
                return klass.GET(uri, _app_id=using,_user=as_user,_throttle=throttle,_high_volume=high_volume,**kw).get('results')

    def _count(self, **kw):
        using = None
        if kw.has_key('_using'):
            using = kw.get('_using')
            del kw['_using']
        as_user = None
        if kw.has_key('_as_user'):
            as_user = kw.get('_as_user')
            del kw['_as_user']
        throttle = None
        if kw.has_key('_throttle'):
            throttle = kw.get('_throttle')
            del kw['_throttle']
        
        kw.update({"count": 1, "limit": 0})
        return self.model_class.GET(self.model_class.ENDPOINT_ROOT,_app_id=using,_user=as_user,_throttle=throttle,
                                        **kw).get('count')
    def using(self,using):
        return Queryset(self,_using=using)

    def as_user(self,as_user):
        return Queryset(self,_as_user=as_user)

    def high_volume(self,val):
        return Queryset(self,_high_volume=val)

    def throttle(self,val):
        return Queryset(self,_throttle=val)
        
    def include(self,val):
        return self.all().include(val)

    def keys(self, val):
        return self.all().keys(val)

    def all(self):
        return Queryset(self)

    def filter(self, **kw):
        return self.all().filter(**kw)

    def fetch(self):
        return self.all().fetch()

    def get(self, **kw):
        return self.filter(**kw).get()

    def values_list(self,*args):
        return self.all().values_list(*args)

    def values(self,*args):
        return self.all().values(*args)

    def limit(self,val):
        return self.all().limit(val)

    def offset(self,val):
        return self.all().offset(val)
    
    def matchesQuery(self, fieldName, subquery):
        return self.all().matchesQuery(fieldName, subquery)
    
    def doesNotMatchQuery(self, fieldName, subquery):
        return self.all().doesNotMatchQuery(fieldName, subquery)


class QuerysetMetaclass(type):
    """metaclass to add the dynamically generated comparison functions"""
    def __new__(cls, name, bases, dct):
        cls = super(QuerysetMetaclass, cls).__new__(cls, name, bases, dct)

        for fname in ['limit', 'skip']:
            def func(self, value, fname=fname):
                s = copy.deepcopy(self)
                s._options[fname] = int(value)
                return s
            setattr(cls, fname, func)

        return cls


class Queryset(object):
    __metaclass__ = QuerysetMetaclass

    OPERATORS = [
        'lt', 'lte', 'gt', 'gte', 'ne', 'in', 'nin', 'exists', 'select', 'dontSelect', 'all'
        ]

    @staticmethod
    def convert_to_parse(value):
        from datatypes import ParseType
        return ParseType.convert_to_parse(value, as_pointer=True)

    @classmethod
    def extract_filter_operator(cls, parameter):
        for op in cls.OPERATORS:
            underscored = '__%s' % op
            if parameter.endswith(underscored):
                return parameter[:-len(underscored)], op
        return parameter, None

    def __init__(self, manager,_using=None,_as_user=None,_throttle=None,_high_volume=False,_values_list=None,_values=None):
        self._manager = manager
        self._where = collections.defaultdict(dict)

        self._options = {}
        self._using = _using
        self._as_user = _as_user
        self._throttle = _throttle
        self._high_volume = _high_volume
        self._values_list = _values_list
        self._values=_values

    def __iter__(self):
        return iter(self._fetch())

    def __len__(self):
        return len(self._fetch())

    def __getitem__(self,key):
        options = copy.deepcopy(self._options)  # make a local copy
        options['skip']=int(key)
        options['limit']=1
        return self._manager._fetch(**options)

    def serialize(self):
        return [x.serialize() for x in self]

    def _fetch(self, count=False):
        """
        Return a list of objects matching query, or if count == True return
        only the number of objects matching.
        """
        options = copy.deepcopy(self._options)  # make a local copy
        if self._using:
            options['_using'] = self._using
        if self._as_user:
            options['_as_user'] = self._as_user
        if self._throttle:
            options['_throttle'] = self._throttle
        if self._high_volume:
            options['_high_volume'] = self._high_volume
        
        if self._values_list:
            options['_values_list'] = self._values_list
        if self._values:
            options['_values'] = self._values

        if self._where:
            # JSON encode WHERE values
            where = json.dumps(self._where)
            options.update({'where': where})
        
        if count:
            return self._manager._count(**options)

        return self._manager._fetch(**options)

    def _clone(self):
        clone = Queryset(manager=self._manager,_using=self._using,_as_user=self._as_user,_throttle=self._throttle,_high_volume=self._high_volume,
                         _values_list=self._values_list,_values=self._values)
        clone._options = copy.deepcopy(self._options)
        clone._where = copy.deepcopy(self._where)
        return clone

    def using(self,using):
        clone = self._clone()
        clone._using = using
        return clone

    def as_user(self,user):
        clone = self._clone()
        clone._as_user = user
        return clone

    def throttle(self,throttle):
        clone = self._clone()
        clone._throttle = throttle
        return clone 

    def high_volume(self,val):
        clone = self._clone()
        clone._high_volume = val
        return clone

    def include(self,val):
        clone = self._clone()
        clone._options['include'] = val
        return clone
    
    def keys(self, keyList):
        if isinstance(keyList, basestring):
            val = keyList
        else:
            val = ','.join(keyList)
            
        clone = self._clone()
        clone._options['keys'] = val
        return clone  

    def all(self):
        return self._clone()

    def filter(self, **kw):
        clone = self._clone()
        for name, value in kw.items():
            parse_value = Queryset.convert_to_parse(value)
            attr, operator = Queryset.extract_filter_operator(name)
            if operator is None:
                clone._where[attr] = parse_value
            else:
                clone._where[attr]['$' + operator] = parse_value
        return clone

    def order_by(self, order, descending=False):
        clone = self._clone()
        # add a minus sign before the order value if descending == True
        if order:
            clone._options['order'] = descending and ('-' + order) or order
        return clone

    def count(self):
        return self._fetch(count=True)

    def exists(self):
        results = self._fetch(count=True)
        return bool(results)

    def limit(self,val):
        val = int(val)
        if val >= 1000:
            raise BadQueryParametersException('limit must be less than 1000')
        clone = self._clone()
        clone._options['limit'] = val
        return clone

    def offset(self,val):
        val = int(val)
        if val >= 10000:
            raise BadQueryParametersException('offset must be less than or equal to 10000')
        clone = self._clone()
        clone._options['skip'] = val
        return clone


    def get(self,**kw):
        results = self.filter(**kw)._fetch()
        if len(results) == 0:
            raise QueryResourceDoesNotExist
        if len(results) >= 2:
            raise QueryResourceMultipleResultsReturned
        return results[0]

    def values_list(self,*args):
        clone = self._clone()
        clone._values_list = args
        return clone

    def values(self,*args):
        clone = self._clone()
        clone._values = args
        return clone
    
    # too lazy to put this in filter and it's a distinct enough operation
    def matchesQuery(self, fieldName, subquery):
        clone = self._clone()
        clone._where[fieldName] = {'$inQuery':{'className': subquery._manager.model_class.parse_table or subquery._manager.model_class.__class__.__name__,
                                               'where': subquery._where
                                               }} 
        return clone

    def doesNotMatchQuery(self, fieldName, subquery):
        clone = self._clone()
        clone._where[fieldName] = {'$notInQuery':{'className': subquery._manager.model_class.parse_table or subquery._manager.model_class.__class__.__name__,
                                               'where': subquery._where
                                               }}
        return clone
     
    def __repr__(self):
        return unicode(self._fetch())
