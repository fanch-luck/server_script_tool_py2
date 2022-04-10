#!/usr/bin/env python
# -*- coding=utf-8 -*-
"""
@file: hdfs_reader.py
@author: Administrator
@time: 2021.06.08
@description: ...
"""

from hdfs import InsecureClient
from pyhive import hive
import zipfile
import StringIO
import re
import sys
from lxml import etree


if sys.getdefaultencoding() != 'utf-8':
    reload(sys)
    sys.setdefaultencoding('utf-8')

CONF = {
    "url": 'http://192.168.0.33:50070',
    "user": 'hadoop',
    "dpath": '/data/zip_data',  # 数据目录
    "date_str": '20210618',
    "protopat": '.xml$',  # 解析协议名称
    "fieldspat": '.xml$',  # 解析字段列表
    "recordspat": '.bcp$',  # 解析数据文件
    "hive_host": "192.168.0.33",
    "hive_port": "10000",
    "hive_username": "hadoop",
    "hive_dbname": "default"
}

"""hdfs 数据目录结构
/
  ...
  /data/
    ...
    zip_data/
      ...
      20210615/
        ...
        120-705420347-350100-350100-1623089700-88326.zip"""


class Job(object):
    def __init__(self):
        self.hdfs_client = None
        self.dpath = None  # data path zip 总数据目录
        self.connected = False
        self.zip_content = None
        
        self.data_set_code = None # 数据集代码（作为表名） 
        self.field_info = None
        self.bcp_content = None
        self.hive_conn = None

    def start_job(self):
        """
        :return:
        """
        pass

    def create_hdfs_client(self, url, user, timeout=5):
        """
        创建到hdfs的"终端"
        :param url:
        :param user:
        :return:
        """
        if self.connected:
            print 'connection existed. exit creating anothor connection.'
        else:
            try:
                client = InsecureClient(url, user=user, timeout=timeout)
                sta = client.status('/')
                self.connected = True
                self.hdfs_client = client
                print 'connection created successfully!'
            except Exception as e:
                self.connected = False
                print 'fail to create connection! {} \n exited'.format(e)
                exit()

    def get_zip_from_hdfs(self, dpath, tdate):
        """
        读取目标文件
        :param fpath:
        :return:
        """
        zip_files = []
        zip_dir = dpath + '/' + tdate
        assert self.connected
        try:
            zip_names = self.hdfs_client.list(zip_dir)
            if len(zip_names) > 0:
                tmp = dict().copy()
                for name in zip_names:
                    if name.endswith('.zip'):
                        k = name[28:44]
                        tmp[k] = name  # 文件名为键，时间戳为值
                for k in sorted(tmp.keys()):
                    zip_files.append(zip_dir + '/' + tmp[k])
                print 'get zip files path successfully!'
        except Exception as e:
            print 'fail to get zip files! {} \n exited'.format(e)
            exit()
        return zip_files

    def read_file(self, fpath):
        """
        获取并解压zip文件，获取xml、bcp文件
        :param zpath:
        :return:
        """
        assert self.connected
        self.zip_content = None
        try:
            with self.hdfs_client.read(fpath) as reader:
                content = reader.read()
        except Exception as e:
            content = None
            print 'fail to read file ({}). {}'.format(fpath, e)
        if content:
            content = StringIO.StringIO(content)  # 使用此方法，可以使对象能够作为文件对象被正常open)
        self.zip_content = content
        return content

    def parse(self, zfile):
        """
        解析xml,获取协议名称
        :param sxml:
        :return:
        """
        assert self.zip_content
        self.field_info = None
        self.bcp_content = None
        xmlstr = None
        bcps = []
        with zipfile.ZipFile(zfile, 'r') as bag:
            for name in bag.namelist():
                if re.search('.xml$', name):
                    with bag.open(name) as _xml:
                        xmlstr = _xml.read()  # 考虑只有一个xml文件
                if re.search('.bcp$', name):
                    with bag.open(name) as _bcp:
                        bcps.append(_bcp.read())  # 考虑一个或多个bcp文件
        # print xmlstr
        # print bcps

        tree = etree.XML(xmlstr)
        data_file_index_info_protocol = tree.xpath('/MESSAGE/DATASET')[0].get('name')  # 数据文件索引信息 协议名称
        bcp_file_format_info_protocol = tree.xpath('/MESSAGE/DATASET/DATA/DATASET')[0].get('name')  # bcp文件格式信息 协议名称
        bcp_formats = []  # bcp文件格式
        items_bcp_format = tree.xpath('/MESSAGE/DATASET/DATA/DATASET/DATA/ITEM')
        for item in items_bcp_format:
            bcp_formats.append({
                'key': item.get('key'),
                'val': item.get('val'),
                'rmk': item.get('rmk')
            })
            if item.get('key') == 'A010004':                
                self.data_set_code = item.get('val')
        
        items_bcp_data = tree.xpath('/MESSAGE/DATASET/DATA/DATASET/DATA/DATASET')
        items_bcp_file = []
        items_bcp_field = []
        for item in items_bcp_data:
            if item.get('name') == 'WA_COMMON_010014':
                items_bcp_file = item.xpath('DATA')
            elif item.get('name') == 'WA_COMMON_010015':
                items_bcp_field = item.xpath('DATA')
            else:
                continue
        bcp_files = {'fileCount': 0, 'files': []}  # bcp文件信息
        bcp_fields = {'fieldCount': 0, 'fields': []}  # bcp文件字段信息
        for item in items_bcp_file:
            bcp_files['fileCount'] += 1
            bcp_files['files'].append([])
            for citem in item.xpath('ITEM'):
                bcp_files['files'][-1].append({
                    'key': citem.get('key'),
                    'val': citem.get('val'),
                    'rmk': citem.get('rmk')
                })
        item = items_bcp_field[0]
        bcp_fields["fieldCount"] = item.get('fieldCount')
        for citem in item.xpath('ITEM'):
            bcp_fields['fields'].append({
                'key': citem.get('key'),
                'eng': citem.get('eng'),
                'chn': citem.get('chn'),
                'Rmrmkk': citem.get('Rmrmkk')
            })
        self.field_info = bcp_fields

        bcp_content = ''
        if len(bcps) > 0:
            for bcp in bcps:
                bcp_content += bcp
        self.bcp_content = bcp_content

    def connect_hive(self):
        pass

    def modeling(self, hhost, husername):
        """
        解析xml，获取字段列表
        :return:
        """
        assert self.bcp_content
        assert self.field_info
        assert self.data_set_code
        assert int(self.field_info['fieldCount']) == len(self.field_info['fields'])
        
        db_name = 'default'
        table_name = self.data_set_code
     
        qline = ','.join(['{} string COMMENT \'{}\''.format(i['eng'], i['chn'].encode('utf8')) for i in self.field_info['fields']])
        #qline = ','.join(['{} string COMMENT \'{}\''.format(i['eng'], 'thisiscomment') for i in self.field_info['fields']])
        print qline
        create_table_qline = "create table IF NOT EXISTS {}.{} ({}) COMMENT '{}' ROW FORMAT DELIMITED FIELDS TERMINATED BY \',\' STORED AS TEXTFILE".format(
            db_name,
            table_name,
            qline,
            "table comment"            
        )
        print create_table_qline
        
        # build qline for data inserting
        fields = [i['eng'].lower() for i in self.field_info['fields']]
        fields = '(' + ','.join(fields) + ')'
        print fields
        records = []
        for line in self.bcp_content.split('\n'):
            if line:
                line = ','.join('\'{}\''.format(i)for i in line.split('\t')[:-1])  # remove non-use word on the end of line
                record = '(' + line + ')'
                records.append(record)
        records = ','.join(records)
       
        load_data_qline = 'INSERT INTO {}.{} {} VALUES {}'.format(
            db_name,
            table_name,
            fields,
            records
        )
        print load_data_qline
        
        try:
            hcur = hive.connect(host='192.168.0.33',username='hadoop').cursor()  # create cursor to hive
            hcur.execute(create_table_qline)  # execute hql words for creating table
            print hcur.fetchall()  # print execution returns
            print "create table successfully!"
        except Exception as e:
            print 'fail to connect hiveserver2 or execute create_table_qline! {}'.format(e)
        
        try:
            hcur.execute(load_data_qline)  # execute hql words for loading data from string
            print hcur.fetchall()  # print execution returns
            print "insert data successfully!"
        except Exception as e:
            print 'fail to execute load_data_qline! {}'.format(e)
             

if __name__ == '__main__':
   
    url = CONF['url']
    user = CONF['user']
    data_path = CONF['dpath']
    date_str = CONF['date_str']

    hive_host = CONF['hive_host']
    hive_username = CONF['hive_username']

    job = Job()
    job.create_hdfs_client(url, user)
    zipfiles = job.get_zip_from_hdfs(data_path, date_str)
    print zipfiles
    job.read_file(zipfiles[0])  # here only chose 1 zip file for testing.
    job.parse(job.zip_content)
    print job.field_info
#    print job.bcp_content

    job.modeling(hive_host, hive_username)

