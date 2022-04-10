#!/usr/bin/env python
# coding=utf-8
"""
for linux python2.7
@file: gz_zip_parser.py
@author: Administrator
@time: 2021.05.19
@description: 1.获取多个zip包内bcp文件内容 2.判断查询词是否在该bcp内，若是打印查询词所在的zip包名、bcp文件名、行号，并打印整行内容
"""

import os
import sys
import re
import zipfile


def func(pattern, targetdir):
    file = targetdir
    with open(file, 'r') as f:
        cont = f.readlines()[0]
    pattern = pattern.decode("string_escape")
    cont = cont
    print type(pattern), pattern
    print type(cont), cont
    if re.search(pattern, cont):
        # 匹配命中
        print "yes ok"

if __name__ == "__main__":
    p, t= sys.argv[1:]
    func(p,t)