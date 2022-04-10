#! /usr/bin/env python
# -*- coding: utf-8 -*-
# file: startrpcserver.py
# author: fan
# date: 2020.10.12 11:49:40

from rpcserver import RPCServer
import json


def add(a, b):
    return a+b


s = RPCServer()
s.register_function(add)  # resister method
s.loop(5000)
