#!usr/bin/python
# -*- coding:utf-8 -*-
# -----------------------------------------------------------
# File Name: sys_infos
# Author:    fan20200225
# Date:      2022/3/27 0027
# -----------------------------------------------------------
import sys
import os
import re
from collections import OrderedDict


def ver_info():
    os.system("cat /proc/version > /tmp/verinfotmplog")
    with open("/tmp/verinfotmplog", "r") as f:
        lines = f.read()
        if "Red Hat" in lines:
            os.system("cat /etc/redhat-release")
        elif "Ubuntu" in lines:
            os.system("cat /etc/issue")
        else:
            os.system("cat /etc/issue")
    os.system("rm /tmp/verinfotmplog -f")


def cpu_information():
    cpuinfo = OrderedDict()
    procinfo = OrderedDict()
    nprocs = 0
    # physicals = 0  # 物理CPU数
    # cores = 0  # 每cpu内核数
    with open("/proc/cpuinfo", "r") as f:
        for line in f:
            if line != '\n':
                # 非空行
                l = line.split(":")
                if len(l) == 2:
                    procinfo[l[0].strip()] = l[1].strip()
                else:
                    procinfo[l[0].strip()] = ''
            else:
                # 到空行，代表一个processor信息结束
                cpuinfo["proc{}".format(nprocs)] = procinfo
                nprocs += 1
                procinfo = OrderedDict()
    return cpuinfo


def mem_info():
    meminfo = OrderedDict()
    os.system("free -h > /tmp/freeinfotmplog")
    with open("/tmp/freeinfotmplog", "r") as f:
        ll = []
        for line in f:
            if line != '\n':
                # 非空行
                ll.append(line.split())
    meminfo[ll[0][0]] = ll[1][1]
    meminfo[ll[0][1]] = ll[1][2]
    os.system("rm /tmp/freeinfotmplog -f")
    return meminfo


def disk_info():
    diskinfo = OrderedDict()
    os.system("lsblk > /tmp/lsblkinfotmplog")
    with open("/tmp/lsblkinfotmplog", "r") as f:
        ll = []
        for line in f:
            if line != '\n':
                # 非空行
                ll.append(line.split())
    diskinfo[ll[0][0]] = ll[0][3]
    for line in ll:
        matched = re.match("^sd[a-z]$", line[0], flags=0)
        if matched:
            diskinfo[line[0]] = line[3]
    os.system("rm /tmp/lsbinfotmplog -f")
    os.system("df -h > /tmp/dfinfotmplog")
    with open("/tmp/dfinfotmplog", "r") as f:
        ll = []
        for line in f:
            if line != '\n':
                # 非空行
                ll.append(line.split())
    for line in ll:
        matched = re.match("^\/dev\/sd[a-z]\d+", line[0], flags=0)
        if matched:
            diskinfo[line[0]] = "容量{} 已用{} 可用{} 已用{} 挂载点{}".format(*line[1:6])
    os.system("rm /tmp/dfinfotmplog -f")
    return diskinfo


if __name__ == "__main__":
    ver_info()
    params = sys.argv[1:]
    if params[0] == "cpu_info" or "sys_info":
        print("CPU")
        cpu_info = cpu_information()
        for proc in cpu_info.keys():
            print("CPU[{}]={}\n"
                  "physicalid(CPU编号)={} cores(内核数)={} siblings(逻辑核数)={}".format(
                proc, cpu_info[proc]['model name'],
                cpu_info[proc]['physical id'], cpu_info[proc]['cpu cores'], cpu_info[proc]['siblings']))
        print("")
    if params[0] == 'mem_info' or "sys_info":
        print("Memory（Total & Used）")
        mem_info = mem_info()
        for k in mem_info:
            print(k, mem_info[k])
        print("")
    if params[0] == 'disk_info' or 'sys_info':
        print("Disk & Parts")
        disk_info = disk_info()
        for k in disk_info:
            print(k, disk_info[k])
        print('\n')

