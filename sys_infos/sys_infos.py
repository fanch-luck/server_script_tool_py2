#!usr/bin/python
# -*- coding:utf-8 -*-
# -----------------------------------------------------------
# File Name: sys_infos
# Author:    fan20200225
# Date:      2022/3/27 0027
# -----------------------------------------------------------
import sys
from collections import OrderedDict


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


if __name__ == "__main__":
    params = sys.argv[1:]
    if params[0] == "cpu_info":
        cpu_info = cpu_information()
        for proc in cpu_info.keys():
            print("CPU[{}]={}\n"
                  "physicalid(CPU编号)={} cores(内核数)={} siblings(逻辑核数)={}".format(
                proc, cpu_info[proc]['model name'],
                cpu_info[proc]['physical id'], cpu_info[proc]['cpu cores'], cpu_info[proc]['siblings']))
