# -*- coding: utf-8 -*-
'''
DESCRIPTION:
    Unit test system for flask


USAGE:
    @unit.observe
    @files.route('/path')
    def path_handler():
        ...

    @path_handler.test
    def test():
        r = unit.client.get('/path', query_string={"a":"hello","b":"handler"})
        assert len(r.d("#body > li")) == 1, "Error"

    @test.fail
    def fallback(e):
        # Run fallbacks
        print "FAILED"

    @test.success
    def fallback():
        # Run success action
        print "OK"



'''
import os
import functools
import inspect
import traceback
import threading
from flask import current_app, Response
from flask.testing import FlaskClient
from xml.dom.minidom import parseString as dom_parse

from pyquery import PyQuery

from foofind.utils import u
from foofind.utils import logging

class TestResponse(Response):
    _d = None

    @property
    def d(self):
        if self._d is None:
            self._d = PyQuery(u(self.data))
        return self._d

class UnitTestPassed(object):
    def __init__(self, fnc, test_fnc):
        self.fnc = fnc
        self.test_fnc = test_fnc

class UnitTestFailed(UnitTestPassed, Exception):
    def __init__(self, fnc, test, exception):
        Exception.__init__(self, exception)
        UnitTestPassed.__init__(self, fnc, test)
        self.exception = exception

class NotObserved(Exception):
    def __init__(self, fnc):
        Exception.__init__(self)
        self.function = fnc

class NotDeclaredTest(Exception):
    def __init__(self, fnc, test):
        Exception.__init__(self)
        self.function = fnc
        self.test = test

class UnitTester(object):
    TestResponse = TestResponse
    UnitTestPassed = UnitTestPassed
    UnitTestFailed = UnitTestFailed
    NotObserved = NotObserved
    NotDeclaredTest = NotDeclaredTest

    def __init__(self):
        self.tests = {}
        self.logger = logging

    _app = None
    @property
    def app(self):
        return self._app or current_app

    _test_client = None
    @property
    def client(self, use_cookies=True):
        '''
        Argumentos de open/get/post/put/...:
            path='/'
            base_url=None
            query_string=None
            method='GET'
            input_stream=None
            content_type=None
            content_length=None
            errors_stream=None
            multithread=False
            multiprocess=False
            run_once=False
            headers=None
            data=None
            environ_base=None
            environ_overrides=None
            charset='utf-8'

            follow_redirects=False
        '''
        return FlaskClient(self.app, TestResponse, use_cookies=use_cookies)

    def addtest(self, obj, test_fnc):
        '''
        Añade test
        '''
        self.tests[obj].append(test_fnc)
        return test_fnc


    def observe(self, obj):
        '''
        Decorador para funciones bajo test
        '''
        if not hasattr(obj, "test"):
            self.tests[obj] = []
            setattr(obj, "test", lambda fnc: self.addtest(obj, fnc))
        return obj

    def runtest(self, observed_fnc, test_fnc=None):
        if not observed_fnc in self.tests: raise NotObserved(observed_fnc)
        if test_fnc is None: tests = self.tests[observed_fnc]
        elif test_fnc in self.tests[observed_fnc]: tests = (test_fnc,)
        else: raise NotDeclaredTest(observed_fnc, test_fnc)
        for test in tests:
            try:
                test()
                self.logger.debug(UnitTestPassed(observed_fnc, test))
            except BaseException as e:
                self.logger.exception(UnitTestFailed(observed_fnc, test, e))
                continue

    def run_tests(self):
        '''
        Ejecuta los tests
        '''
        for fnc in self.tests:
            #print fnc, fnc.name if hasattr(fnc, "name") else fnc.__name__
            self.runtest(fnc)

    def init_app(self, app):
        self._app = app

