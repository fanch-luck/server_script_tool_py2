#! /usr/bin/env python
# -*- coding: utf-8 -*-
# file: startsrm.py
# author: fan
# date: 2020.10.12 11:25:10

from rpcclient import RPCClient


c = RPCClient()
c.connect("127.0.0.1", 5000)  # connect rpc server
res = c.add(5, 6)
print(res)
