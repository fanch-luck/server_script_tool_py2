#!/usr/bin/env python
# -*- coding:utf-8 -*-
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


class Parser(object):
    def __init__(self, filepat=".zip$", txtpat=".bcp$"):
        """
        程序初始化，导入参数，默认匹配.zip结尾压缩包、.bcp结尾的文件
        :param contentpattern: 欲查找的字符串或正则表达式
        :param tartgetdir: 查找路径，包含zip文件，遍历模式
        :param filepat: 默认匹配.zip结尾的数据包
        :param txtpat: 默认匹配.bcp结尾的数据文件
        """
        # self.contentpattern = contentpattern
        # self.tartgetdir = tartgetdir
        self.filepat = filepat
        self.txtpat = txtpat
        self.filesres = None
        self.contentres = None
        self.matchres = None

    def get_files(self, tardir, pattern):
        """
        获取zip列表
        :param tardir: zip所在目录
        :param pattern: zip文件名匹配的正则表达式，为空则表示匹配任何文件
        :return: None
        """
        res = {"result": "false", "info": "OK", "pattern": pattern, "filelist": []}
        filelist = []  # 文件绝对路径构成列表
        if os.path.exists(tardir):
            for rt, dirs, filenames in os.walk(tardir):
                for name in filenames:
                    # 匹配包名
                    if re.search(pattern, name):
                        res["result"] = "true"
                        pth = os.path.join(rt, name)
                        filelist.append(pth)
                    else:
                        continue
            if filelist == []:
                res["info"] = "ERROR: none of hit files in target dir, stop parsing."
        else:
            res["info"] = "ERROR: target dir not exists. stop parsing."
        res["filelist"] = filelist
        self.filesres = res

    def get_txts_and_contents(self, zipfilepath, pattern):
        """
        从zip包提取bcp文件，读取文件所有内容
        :param zipfilepath: zip包文件绝对路径
        :param pattern: 匹配包内文件名称的正则表达式，为空则表示匹配任何文件
        :return:
        """
        # 数据结构 contents 若存在符合要求文本文件，则contents中，key为文本文件名称，value为文本内容（str）
        res = {"result": "false", "info": "OK", "pattern": pattern, "zippath": zipfilepath, "contents": {}}
        if os.path.exists(zipfilepath):
            try:
                with zipfile.ZipFile(zipfilepath, 'r') as bag:
                    for name in bag.namelist():
                        if re.search(pattern, name):
                            res["contents"][name] = ""  # 文本文件名占位一个值为空的key
                            with bag.open(name, 'r') as txt:
                                res["contents"][name] = txt.read()
                                res["result"] = "true"
                        else:
                            continue
                    if res["contents"] == {}:
                        res["result"] = "false"
                        res["info"] = "ERROR, this zip({}) don\'t contain any {} files".format(zipfilepath, pattern)
            except Exception as e:
                res["info"] = "ERROR, while opening zip file({}). error detail({})".format(zipfilepath, e)
        else:
            res["info"] = "ERROR, this zip file not exists ({})".format(zipfilepath)
        self.contentres = res

    def get_match_result(self, txtcontent, pattern):
        """
        在文件内容中对条件正则表达式进行逐行匹配
        :param txtcontent: 文本内容 fileopen.read()读取的全文字符串
        :param pattern: 匹配目标字符串的正则表达式
        :return:
        """

        # 数据结构，result命中结果true or false; hitrows命中行信息，为空表示未命中，若命中，则hitrows中，key为行号，value为行内容
        res = {"result": "false", "info": "OK", "pattern": pattern, "hitrows": {}}
        if txtcontent:
            if sys.platform == "win32":
                # 在Windows命令行执行时须加此条件，先对文本进行转码
                pattern = pattern.decode("string_escape")
                txtcontent = txtcontent.decode("utf-8").encode("gbk")
            contlist = txtcontent.split("\n")
            for i in range(len(contlist)):
                # 逐行匹配正则表达式
                if contlist[i]:
                    if re.search(pattern, contlist[i]):
                        # 匹配命中
                        res["result"] = "true"
                        res["hitrows"][i+1] = contlist[i]  # i+1表示真实行号从1开始而不是0
                    else:
                        # 未命中，正常
                        continue
                else:
                    # 存在空行，正常
                    continue
            if res["hitrows"] == {}:
                res["result"] = "true"
                res["info"] = "INFO: pattern not hit in txt content."
        else:
            res["info"] = "WARN: text content is empty!"
        self.matchres = res

    def pretty_output(self, filepath, txtname, contents):
        """
        正确的输出信息
        :return: None
        """
        print """************************* hit info *************************
hit {}
in file {}({})
witch lines:""".format(
            contents["pattern"],
            txtname,
            filepath
        )
        for irow in sorted(contents["hitrows"].keys()):
            print "line {}: {}".format(irow, contents["hitrows"][irow])
        print "************************* hit info end *************************\n"

def start_parse():

    if len(sys.argv) != 3:
        print "the number of parameters is not correct. please type 'python [scriptsname].py [content_pattern] [target_dir]' to retry."
    else:
        content_pattern, target_dir = sys.argv[1:]
        # content_pattern, target_dir = "腾讯首页", "R:\\桌面\\桌面文件\\bao\\hotel_user_22021_20210406\\hotel_user_22021\\123-631422656-350125-350100-1617678672-03686"
        # 路径中含中文应用u"XXX"表示，查询关键词不要使用u标头
        par = Parser()
        par.get_files(target_dir, par.filepat)
        if par.filesres["result"] == "true":
            if par.filesres["filelist"]:
                # 已匹配到符合要求的zip文件
                for fil in par.filesres["filelist"]:
                    par.get_txts_and_contents(fil, par.txtpat)
                    if par.contentres["result"] == "true":
                        if par.contentres["contents"]:
                            # 匹配到符合要求的文本文件
                            for txt in par.contentres["contents"].keys():  # key是文件名，value是文件内容
                                par.get_match_result(par.contentres["contents"][txt], content_pattern)
                                if par.matchres["result"] == "true":
                                    if par.matchres["hitrows"]:
                                        # 命中行
                                        par.pretty_output(fil, txt, par.matchres)
                                    else:
                                        if par.matchres["info"] == "OK":
                                            continue
                                else:
                                    print par.matchres["info"]
                        else:
                            continue
                    else:
                        print par.contentres["info"]
            else:
                print par.filesres["info"]
        else:
            # 出错或未匹配到zip,输出信息，结束解析
            print par.filesres["info"]


if __name__ == "__main__":
    # par = Parser()
    # tarpth = u"R:\\桌面\\桌面文件\\bao\hotel_user_22021_20210513\\hotel_user_22021"
    # par.get_files(tarpth, pattern=".zip$")
    # print len(par.filesres), par.filesres
    #
    # filepth = u"R:\\桌面\\桌面文件\\bao\\120-791229046-350900-350900-1617263342-85230.zip"
    # par.get_txts_and_contents(filepth, pattern=".bcp$")
    # print len(par.contentres["contents"]), par.contentres
    #
    # txtpath = u"R:\\桌面\桌面文件\\bao\hotel_user_22021_20210406\\hotel_user_22021\\123-631422656-350125-350100-1617678654-04009\\123-631422656-350125-350100-1617678654-04009\\123-350125-1617678651-03915-WA_BASIC_0022-0.bcp"
    # with open(txtpath, 'r') as t:
    #     txtcon = t.read()
    # par.get_match_result(txtcon, "35012521000112741f4a5b36951617678562")
    # print par.matchres
    start_parse()
    print 'parsing finished'
