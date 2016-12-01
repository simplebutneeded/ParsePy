#!/usr/bin/env python
#-*- coding: utf-8 -*-

"""
Contains unit tests for the Python Parse REST API wrapper
"""

import os
import sys
import subprocess
import unittest
import datetime
import random
import time


from core import ResourceRequestNotFound
from connection import register, get_keys,ParseBatcher,TimeBasedThrottle
from datatypes import GeoPoint, Object, Function
from user import User, Role
import query

try:
    import settings_local
except ImportError:
    sys.exit('You must create a settings_local.py file of this format:\n\nKEYS = {\n\t\t' \
             "'myappid': {\n\t\t\t"\
             "'REST_API_KEY':'myrestkey',\n\t\t\t"\
             "'API_ROOT':'optional root',\n\t\t\t"\
             "'MASTER_KEY':'mymasterkey'\n\t\t"\
             "},"
             "'myotherappid': {\n\t\t\t"\
             "'REST_API_KEY':'myotherrestkey',\n\t\t\t"\
             "'MASTER_KEY':'myothermasterkey'\n\t\t"\
             "}\n\t}"
            )

try:
    unicode = unicode
except NameError:
    # is python3
    unicode = str

for app_id in settings_local.KEYS:
    register(
        app_id,
        settings_local.KEYS.get(app_id).get('REST_API_KEY'),
        master_key = settings_local.KEYS.get(app_id).get('MASTER_KEY'),
        api_root = settings_local.KEYS.get(app_id).get('API_ROOT'),
        )

GLOBAL_JSON_TEXT = """{
    "applications": {
        "_default": {
            "link": "parseapi"
        },
        "parseapi": {
            "applicationId": "%s",
            "masterKey": "%s"
        }
    },
    "global": {
        "parseVersion": "1.1.16"
    }
}
"""


class Game(Object):
    ACL={"*":{'read':True,'write':True}}


class GameScore(Object):
    ACL={"*":{'read':True,'write':True}}


class City(Object):
    ACL={"*":{'read':True,'write':True}}


class Review(Object):
    ACL={"*":{'read':True,'write':True}}


class CollectedItem(Object):
    ACL={"*":{'read':True,'write':True}}


class TestObject(object):
    USING = None
    
    def tearDown(self):
        city_name = getattr(self.city, 'name', None)
        game_score = getattr(self.score, 'score', None)
        if city_name:
            old = [x for x in City.Query.using(self.USING).all()]
            if old:
                ParseBatcher().batch_delete(old,_using=self.USING)
        if game_score:
            old = [x for x in GameScore.Query.using(self.USING).all()]
            if old:
                ParseBatcher().batch_delete(old,_using=self.USING)
        
    def testCanInitialize(self):
        self.assert_(self.score.score == self.SCORE_SCORE, 'Could not set score')

    def testCanInstantiateParseType(self):
        self.assert_(self.city.location.latitude == self.CITY_LAT)

    def testCanSaveDates(self):
        now = datetime.datetime.now()
        self.score.last_played = now
        self.score.save(_using=self.USING)
        self.assert_(self.score.last_played == now, 'Could not save date')

    def testCanCreateNewObject(self):
        self.score.save(_using=self.USING)
        object_id = self.score.objectId

        self.assert_(object_id is not None, 'Can not create object')
        self.assert_(type(object_id) == unicode)
        self.assert_(type(self.score.createdAt) == datetime.datetime)
        self.assert_(GameScore.Query.using(self.USING).filter(objectId=object_id).exists(),
                     'Can not create object')

    def testCanUpdateExistingObject(self):
        self.city.save(_using=self.USING)
        self.city.country = self.CITY_COUNTRY
        self.city.save(_using=self.USING)
        self.assert_(type(self.city.updatedAt) == datetime.datetime)

        city = City.Query.using(self.USING).get(name=self.CITY_NAME)
        self.assert_(city.country == self.CITY_COUNTRY, 'Could not update object')

    def testCanDeleteExistingObject(self):
        self.score.save(_using=self.USING)
        object_id = self.score.objectId
        self.score.delete(_using=self.USING)
        self.assert_(not GameScore.Query.using(self.USING).filter(objectId=object_id).exists(),
                     'Failed to delete object %s on Parse ' % self.score)

    def testCanIncrementField(self):
        previous_score = self.score.score
        self.score.save(_using=self.USING)
        self.score.increment('score',_using=self.USING)
        self.assert_(GameScore.Query.using(self.USING).filter(score=previous_score + 1).exists(),
                     'Failed to increment score on backend')

    def testAssociatedObject(self):
        """test saving and associating a different object"""
        collectedItem = CollectedItem(type="Sword", isAwesome=True)
        collectedItem.save(_using=self.USING)

        self.score.item = collectedItem
        self.score.save(_using=self.USING)

        # get the object, see if it has saved

        qs = GameScore.Query.using(self.USING).include('item').get(objectId=self.score.objectId)
        self.assert_(isinstance(qs.item, Object),
                     "Associated CollectedItem is not an object")
        self.assert_(qs.item.type == "Sword",
                   "Associated CollectedItem does not have correct attributes")
    
    def testBatch(self):
        """test saving, updating and deleting objects in batches"""
        scores = [GameScore(score=s, player_name=self.SCORE_NAME, cheat_mode=False)
                    for s in range(5)]
        batcher = ParseBatcher()
        batcher.batch_save(scores,_using=self.USING)
        
        self.assert_(GameScore.Query.using(self.USING).filter(player_name=self.SCORE_NAME).count() == 5,
                     "batch_save didn't create objects")
        self.assert_(all(s.objectId is not None for s in scores),
                     "batch_save didn't record object IDs")

        # test updating
        for s in scores:
            s.score += 10
        batcher.batch_save(scores,_using=self.USING)

        updated_scores = GameScore.Query.using(self.USING).filter(player_name=self.SCORE_NAME)
        
        self.assertEqual(sorted([s.score for s in updated_scores]),
                         range(10, 15), msg="batch_save didn't update objects")

        # test deletion
        batcher.batch_delete(scores,_using=self.USING)
        self.assert_(GameScore.Query.using(self.USING).filter(player_name=self.SCORE_NAME).count() == 0,
                     "batch_delete didn't delete objects")


class TestStandardObject(TestObject,unittest.TestCase):
    def setUp(self):
        self.SCORE_SCORE = 1338
        self.SCORE_NAME = 'John'
        self.score = GameScore(
                score=self.SCORE_SCORE, player_name=self.SCORE_NAME+' Doe', cheat_mode=False
                )
        self.CITY_LAT = -23.5
        self.CITY_COUNTRY = 'Brazil'
        self.CITY_NAME = 'São Paulo'
        self.city = City(
                name=self.CITY_NAME, location=GeoPoint(self.CITY_LAT, -46.6167)
                )

class TestObjectUsing(TestObject,unittest.TestCase):
    def setUp(self):
        self.SCORE_SCORE = 1337
        self.SCORE_NAME = 'Jane'
        self.score = GameScore(
                score=self.SCORE_SCORE, player_name=self.SCORE_NAME+' Doe', cheat_mode=False
                )
        self.CITY_LAT = 37.791
        self.CITY_COUNTRY = 'USA'
        self.CITY_NAME = 'San Francisco'
        self.city = City(
                name=self.CITY_NAME, location=GeoPoint(self.CITY_LAT,-122.395)
                )
        self.USING = settings_local.KEYS.keys()[1]


class TestTypes(unittest.TestCase):
    
    def setUp(self):
        self.now = datetime.datetime.now()
        self.score = GameScore(
            score=1337, player_name='John Doe', cheat_mode=False,
            date_of_birth=self.now
            )
        self.sao_paulo = City(
            name='São Paulo', location=GeoPoint(-23.5, -46.6167)
            )

    def testCanConvertToNative(self):
        native_data = self.sao_paulo._to_native()
        self.assert_(type(native_data) is dict, 'Can not convert object to dict')

    def testCanConvertNestedLocation(self):
        native_sao_paulo = self.sao_paulo._to_native()
        location_dict = native_sao_paulo.get('location')

        self.assert_(type(location_dict) is dict,
                     'Expected dict after conversion. Got %s' % location_dict)
        self.assert_(location_dict.get('latitude') == -23.5,
                     'Can not serialize geopoint data')

    def testCanConvertDate(self):
        native_date = self.score._to_native().get('date_of_birth')
        self.assert_(type(native_date) is dict,
                     'Could not serialize date into dict')
        iso_date = native_date.get('iso')
        self.assert_(iso_date == self.now.isoformat(),
                     'Expected %s. Got %s' % (self.now.isoformat(), iso_date))


class TestQuery(object):
    """Tests of an object's Queryset"""

    USING = None

    def setUp(self):
        """save a bunch of GameScore objects with varying scores"""
        # first delete any that exist
        old = [x for x in GameScore.Query.using(self.USING).all()]
        if old:
            ParseBatcher().batch_delete(old,_using=self.USING)
        old = [x for x in Game.Query.all()]
        if old:
            ParseBatcher().batch_delete(old,_using=self.USING)

        self.game = Game(title="Candyland")
        self.game.save(_using=self.USING)

        self.scores = [
            GameScore(score=s, player_name='John Doe', game=self.game)
                        for s in range(1, 6)]
        for s in self.scores:
            ParseBatcher().batch_save(self.scores,_using=self.USING)

    def testValuesList(self):
        res = [x for x in GameScore.Query.using(self.USING).values_list('score','player_name')]
        
        expected = [[s.score,s.player_name] for s in self.scores]

        self.assertEqual(sorted(res,key=lambda x:x[0]), sorted(expected,key=lambda x:x[0]))

        res = [x for x in GameScore.Query.using(self.USING).all().values_list('score','player_name')]
        self.assertEqual(sorted(res,key=lambda x:x[0]), sorted(expected,key=lambda x:x[0]))
    
    def testKeys(self):
        res = [x for x in GameScore.Query.using(self.USING).keys(['score',])]
        self.assertEqual(None, getattr(res[0], 'player_name', None))
        self.assertNotEqual(None, getattr(res[0], 'objectId', None))
        self.assertNotEqual(None, getattr(res[0], 'score', None))


    def testExists(self):
        """test the Queryset.exists() method"""
        for s in range(1, 6):
            self.assert_(GameScore.Query.using(self.USING).filter(score=s).exists(),
                         "exists giving false negative")
        self.assert_(not GameScore.Query.using(self.USING).filter(score=10).exists(),
                     "exists giving false positive")

    def testCanFilter(self):
        """test the Queryset.filter() method"""
        for s in self.scores:
            qobj = GameScore.Query.using(self.USING).filter(objectId=s.objectId).get()
            self.assert_(qobj.objectId == s.objectId,
                         "Getting object with .filter() failed")
            self.assert_(qobj.score == s.score,
                         "Getting object with .filter() failed")

        # test relational query with other Objects
        num_scores = GameScore.Query.using(self.USING).filter(game=self.game).count()
        self.assert_(num_scores == len(self.scores),
                        "Relational query with .filter() failed")

    def testGetExceptions(self):
        """test possible exceptions raised by Queryset.get() method"""
        self.assertRaises(query.QueryResourceDoesNotExist,
                          GameScore.Query.using(self.USING).filter(score__gt=20).get)
        self.assertRaises(query.QueryResourceMultipleResultsReturned,
                          GameScore.Query.using(self.USING).filter(score__gt=3).get)

    def testCanQueryDates(self):
        last_week = datetime.datetime.now() - datetime.timedelta(days=7)
        score = GameScore(name='test', last_played=last_week)
        score.save(_using=self.USING)
        self.assert_(GameScore.Query.using(self.USING).filter(last_played=last_week).exists(),
                     'Could not run query with dates')

    def testComparisons(self):
        """test comparison operators- gt, gte, lt, lte, ne"""
        scores_gt_3 = list(GameScore.Query.using(self.USING).filter(score__gt=3))
        self.assertEqual(len(scores_gt_3), 2)
        self.assert_(all([s.score > 3 for s in scores_gt_3]))

        scores_gte_3 = list(GameScore.Query.using(self.USING).filter(score__gte=3))
        self.assertEqual(len(scores_gte_3), 3)
        self.assert_(all([s.score >= 3 for s in scores_gt_3]))

        scores_lt_4 = list(GameScore.Query.using(self.USING).filter(score__lt=4))
        self.assertEqual(len(scores_lt_4), 3)
        self.assert_(all([s.score < 4 for s in scores_lt_4]))

        scores_lte_4 = list(GameScore.Query.using(self.USING).filter(score__lte=4))
        self.assertEqual(len(scores_lte_4), 4)
        self.assert_(all([s.score <= 4 for s in scores_lte_4]))

        scores_ne_2 = list(GameScore.Query.using(self.USING).filter(score__ne=2))
        self.assertEqual(len(scores_ne_2), 4)
        self.assert_(all([s.score != 2 for s in scores_ne_2]))

        # test chaining
        lt_4_gt_2 = list(GameScore.Query.using(self.USING).filter(score__lt=4).filter(score__gt=2))
        self.assert_(len(lt_4_gt_2) == 1, 'chained lt+gt not working')
        self.assert_(lt_4_gt_2[0].score == 3, 'chained lt+gt not working')
        q = GameScore.Query.using(self.USING).filter(score__gt=3, score__lt=3)
        self.assert_(not q.exists(), "chained lt+gt not working")

    def testOptions(self):
        """test three options- order, limit, and skip"""
        scores_ordered = list(GameScore.Query.all().using(self.USING).order_by("score"))
        self.assertEqual([s.score for s in scores_ordered],
                         [1, 2, 3, 4, 5])

        scores_ordered_desc = list(GameScore.Query.all().using(self.USING).order_by("score", descending=True))
        self.assertEqual([s.score for s in scores_ordered_desc],
                         [5, 4, 3, 2, 1])

        scores_limit_3 = list(GameScore.Query.using(self.USING).all().limit(3))
        self.assert_(len(scores_limit_3) == 3, "Limit did not return 3 items")

        scores_skip_3 = list(GameScore.Query.using(self.USING).all().skip(3))
        self.assert_(len(scores_skip_3) == 2, "Skip did not return 2 items")

    def testCanCompareDateInequality(self):
        today = datetime.datetime.today()
        tomorrow = today + datetime.timedelta(days=1)
        self.assert_(GameScore.Query.using(self.USING).filter(createdAt__lte=tomorrow).count() == 5,
                     'Could not make inequality comparison with dates')

    def tearDown(self):
        """delete all GameScore and Game objects"""
        old = [x for x in GameScore.Query.using(self.USING).all()]
        if old:
            ParseBatcher().batch_delete(old,_using=self.USING)
        self.game.delete(_using=self.USING)

class TestStandardQuery(TestQuery,unittest.TestCase):
    pass
class TestQueryUsing(TestQuery,unittest.TestCase):
    USING = settings_local.KEYS.keys()[1]

class TestFunction(unittest.TestCase):
    def setUp(self):
        """create and deploy cloud functions"""
        original_dir = os.getcwd()

        cloud_function_dir = os.path.join(os.path.split(__file__)[0], 'cloudcode')
        os.chdir(cloud_function_dir)
        # write the config file
        with open("config/global.json", "w") as outf:
            outf.write(GLOBAL_JSON_TEXT % (get_keys(None).get('APPLICATION_ID'),
                                           get_keys(None).get('MASTER_KEY') )
                      )
        try:
            subprocess.call(["parse", "deploy"])
        except OSError as why:
            print("parse command line tool must be installed " \
                "(see https://www.parse.com/docs/cloud_code_guide)")
            self.skipTest(why)
        os.chdir(original_dir)

    def tearDown(self):
        for review in Review.Query.all():
            review.delete()

    def test_simple_functions(self):
        """test hello world and averageStars functions"""
        # test the hello function- takes no arguments

        hello_world_func = Function("hello")
        ret = hello_world_func()
        self.assertEqual(ret["result"], u"Hello world!")

        # Test the averageStars function- takes simple argument
        r1 = Review(movie="The Matrix", stars=5,
                    comment="Too bad they never made any sequels.")
        r1.save()
        r2 = Review(movie="The Matrix", stars=4, comment="It's OK.")
        r2.save()

        star_func = Function("averageStars")
        ret = star_func(movie="The Matrix")
        self.assertAlmostEqual(ret["result"], 4.5)




class TestUser(unittest.TestCase):
    USERNAME = "dhelmet%s@spaceballs.com" % random.randint(0,10000000)
    PASSWORD = "12345"
    game = None

    def _get_user(self):
        try:
            user = User.signup(self.username, self.password)
        except:
            user = User.Query.get(username=self.username)
        return user

    def _destroy_user(self):
        user = self._get_logged_user()
        try:
            user and user.delete()
        except:
            pass

    def _get_logged_user(self):
        if User.Query.filter(username=self.username).exists():
            return User.login(self.username, self.password)
        else:
            return self._get_user()

    def setUp(self):
        self.username = TestUser.USERNAME
        self.password = TestUser.PASSWORD

        try:
            u = User.login(self.USERNAME, self.PASSWORD)
        except ResourceRequestNotFound:
            # if the user doesn't exist, that's fine
            return
        u.delete()

    def tearDown(self):
        if self.game:
            try:
                self._get_user()
                user = User.login(self.username,self.password)
                self.game.delete(_as_user=user)
            except:
                pass
        self._destroy_user()
    
    def testCanSignUp(self):
        self._destroy_user()
        user = User.signup(self.username, self.password)
        self.assert_(user is not None)
        self.assert_(user.username == self.username)

    def testCanLogin(self):
        self._get_user()  # User should be created here.
        user = User.login(self.username, self.password)
        self.assert_(user.is_authenticated(), 'Login failed')

    def testCanUpdate(self):
        user = self._get_logged_user()
        phone_number = '555-5555'

        # add phone number and save
        user.phone = phone_number
        user.save()

        self.assert_(User.Query.filter(phone=phone_number).exists(),
                     'Failed to update user data. New info not on Parse')
    
    def testCanCreateRecord(self):
        self._get_user()
        user = User.login(self.username,self.password)

        self.game = Game(title="Candyland")
        self.game.ACL = {user.objectId:{'read':True}}
        self.game.save(_as_user=user)

        
        self.assert_(Game.Query.filter(title="Candyland").exists() == False)
        self.assert_(Game.Query.as_user(user).filter(title="Candyland").exists() == True)
        self.assert_(Game.Query.filter(title="Candyland").as_user(user).exists() == True)

    def testBecome(self):
        u = self._get_user()
        res = User.become(user_id=u.objectId)
        self.assertTrue(res)
        self.assertEqual(res.sessionToken, u.sessionToken)
        self.assertEqual(res.objectId, u.objectId)

    def testBecomeBad(self):
        u = self._get_user()
        res = User.become(user_id='asfd')
        self.assertEqual(None,res)

class TestRole(unittest.TestCase):
    USING = None

    def setUp(self):
        self.username = TestUser.USERNAME
        self.password = TestUser.PASSWORD

        self.master_user = User()
        self.master_user.set_master(True)

        try:
            self.user = User.login(self.username, self.password)
        except ResourceRequestNotFound:
            # if the user doesn't exist, that's fine
            self.user = User.signup(self.username, self.password)
        existing = Role.Query.as_user(self.master_user).using(self.USING).all()
        if existing:
            ParseBatcher().batch_delete(existing,_using=self.USING,_as_user=self.master_user)

    def tearDown(self):
        if self.user:
            self.user.delete()
        existing = Role.Query.as_user(self.master_user).using(self.USING).all()
        if existing:
            ParseBatcher().batch_delete(existing,_using=self.USING,_as_user=self.master_user)

    def testNoRoles(self):
        self.assertEqual(0,len(Role.Query.as_user(self.user).using(self.USING).all()))

    def testAddRole(self):
        admin = Role()
        admin.name='testrole-%s' % random.randint(0,10000000)
        admin.ACL={'*':{'read':True}}
        admin.save(_as_user=self.master_user)

        admin.addRelation('users',self.user,_as_user=self.master_user)

        self.myRoles = [x for x in Role.Query.as_user(self.user).using(self.USING).filter(users=self.user)]
        self.assertEqual(1,len(self.myRoles))

    def testRemoveRole(self):
        self.testAddRole()

        self.myRoles[0].removeRelation('users',self.user,_as_user=self.master_user)

        myRolesNow = Role.Query.as_user(self.user).using(self.USING).filter(users=self.user)
        self.assertEqual(0,len(myRolesNow))

class TimeBasedThrottleTest(unittest.TestCase):
    USING = None
    def tearDown(self):
        recs =  [x for x in Game.Query.using(self.USING).all()]
        if recs:
            ParseBatcher().batch_delete(recs,_using=self.USING)
    
    def testLimits(self):
        t = TimeBasedThrottle(limit=4,period=1)
        count = 0
        start = time.time()
        for i in xrange(0,12):
            with t:
                count += 1
        end = time.time()
        self.assertGreater(end-start, 3)
        self.assertLess(end-start, 4)
        self.assertEqual(count,12)
    
    def testLimitsAndMultiIterations(self):
        t = TimeBasedThrottle(limit=4,period=1,calls_per_iteration=2)
        count = 0
        start = time.time()
        for i in xrange(0,8):
            with t:
                count += 2
        end = time.time()
        self.assertGreater(end-start, 4)
        self.assertLess(end-start, 5)
        self.assertEqual(count,16)

    
            

    def testSave(self):
        t = TimeBasedThrottle(limit=2,period=1)
        self.scores = [
            Game(name='John Doe%s' % s) for s in xrange(0, 6)]

        start = time.time()
        for i in self.scores:
            i.save(_throttle=t, _using=self.USING)
        end = time.time()
        self.assertTrue( (end-start) >= 3)

    def testDelete(self):
        t = TimeBasedThrottle(limit=2,period=1)
        self.scores = [
            Game(name='John Doe%s' % s) for s in xrange(0, 6)]
        ParseBatcher().batch_save(self.scores,_using=self.USING)
            
        start = time.time()
        for i in self.scores:
            i.delete(_throttle=t, _using=self.USING)
        end = time.time()
        self.assertTrue( (end-start) >= 3)

    def _quickGet(self,query,objectId):
        try:
            return query.get(objectId=objectId)
        except:
            return None

    def testQueries(self):
        t = TimeBasedThrottle(limit=2,period=1)
        start = time.time()
        q = Game.Query.using(self.USING).throttle(t)
        for i in xrange(0,6):
            self._quickGet(q,'123')
        end = time.time()
        self.assertTrue( (end-start) >= 3)
    
    def testBatchSave(self):
        t = TimeBasedThrottle(limit=2,period=1)
        g = Game(name='John Doe')
        g.save(_using=self.USING)
        g.name='joe2'
        
        count = 0
        start = time.time()

        for i in xrange(0,6):
            ParseBatcher().batch_save([g],_using=self.USING,_throttle=t)
        end = time.time()
        self.assertGreater(end-start,3)
    
    def testBatchSaveAtOnce(self):
        t = TimeBasedThrottle(limit=2,period=1)
        g = Game(name='John Doe')
        g.save(_using=self.USING)
        g.name='joe2'
        
        count = 0
        start = time.time()

        ParseBatcher().batch_save([g]*6,_using=self.USING,_throttle=t)
        end = time.time()
        self.assertGreater(end-start,3)


    def testBatchDelete(self):
        t = TimeBasedThrottle(limit=2,period=1)
        self.scores = [
            Game(name='John Doe%s' % s) for s in xrange(0, 6)]
        ParseBatcher().batch_save(self.scores,_using=self.USING)
            
        start = time.time()
        for i in self.scores:
            ParseBatcher().batch_delete([i],_throttle=t, _using=self.USING)
        end = time.time()
        self.assertTrue( (end-start) >= 3)
    
    


if __name__ == "__main__":
    # command line
    unittest.main()
