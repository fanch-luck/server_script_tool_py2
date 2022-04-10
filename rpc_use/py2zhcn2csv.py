#! /usr/bin/env python
# -*- coding: utf-8 -*-
# file: py2zhcn2csv.py
# author: fan
# date: 2020.10.14 09:59:52

import csv

row1 = [
    u"服务器列表",
    u"CPU使用率(最大100%)",
    u"物理内存使用率(最大100%)",
    u"磁盘空间使用率(最大100%)"
]
row2 = [s.encode("gbk") for s in row1]
with open("p2csv.csv", "w") as f:
    f_csv = csv.writer(f)
    f_csv.writerow(row2)


if __name__ == "__main__":
    pass
