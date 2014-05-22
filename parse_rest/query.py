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
    '''Query returned no results'''
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
                if not kw.get('values_list'):
                    new_res = [klass(_using=using,_as_user=as_user,**it) for it in klass.GET(uri, _app_id=using,_user=as_user,**kw).get('results')]
                else:
                    new_res = [[it[y] for y in kw['values_list']] for it in klass.GET(uri, _app_id=using,_user=as_user,**kw).get('results')]
                    
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
            if not kw.get('values_list'):
                return [klass(_using=using,_as_user=as_user,**it) for it in klass.GET(uri, _app_id=using,_user=as_user,_high_volume=high_volume,**kw).get('results')]
            else:
                return [[it[y] for y in kw['values_list']] for it in klass.GET(uri, _app_id=using,_user=as_user,_high_volume=high_volume,**kw).get('results')]

    def _count(self, **kw):
        using = kw.get('_using')
        as_user = kw.get('_as_user')
        kw.update({"count": 1, "limit": 0})
        return self.model_class.GET(self.model_class.ENDPOINT_ROOT,_app_id=using,_user=as_user,
                                        **kw).get('count')
    def using(self,using):
        return Queryset(self,_using=using)

    def as_user(self,as_user):
        return Queryset(self,_as_user=as_user)

    def high_volume(self,val):
        return Queryst(self,_high_volume=val)

    def include(self,val):
        return Queryset(self,_include=val)

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

    def limit(self,val):
        return self.all().limit(val)

    def offset(self,val):
        return self.all().offset(val)


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

    def __init__(self, manager,_using=None,_as_user=None,_high_volume=False,_include=None,values_list=None):
        self._manager = manager
        self._where = collections.defaultdict(dict)
        self._options = {}
        self._using = _using
        self._as_user = _as_user
        self._high_volume = _high_volume
        self._include = _include
        self._values_list = None

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
        if self._high_volume:
            options['_high_volume'] = self._high_volume
        # notice the lack of _. This goes as a top level parameter
        if self._include:
            options['include'] = self._include
        if self._values_list:
            options['values_list'] = self._values_list
        if self._where:
            # JSON encode WHERE values
            where = json.dumps(self._where)
            options.update({'where': where})
        if count:
            return self._manager._count(**options)

        return self._manager._fetch(**options)

    def _clone(self):
        clone = Queryset(manager=self._manager,_using=self._using,_as_user=self._as_user,_high_volume=self._high_volume,_include=self._include,
                         values_list=self._values_list)
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

    def high_volume(self,val):
        clone = self._clone()
        clone._high_volume = val
        return clone

    def include(self,val):
        clone = self._clone()
        clone._include = val
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

    def __repr__(self):
        return unicode(self._fetch())
