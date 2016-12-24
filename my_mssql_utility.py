# -*- coding: utf-8 -*-
# author: ylkjick532428@gmail.com
import os
import pypyodbc
import re
import sys
import platform

plat_str = platform.system()
#=========================================================================
# MsSqlUtil : mssql数据库的工具类
#=========================================================================
class MsSqlUtil(object):

    def __init__(self, db):
        self.db = db
        self.db_conn = None
        self.db_cursor = None
        self.get_db_conn()
        self.get_db_cursor()

    #=========================================================================
    # set_db : 修改数据库配置
    #=========================================================================
    def set_db(self, db):
        self.db = db

    #=========================================================================
    # get_db_conn : 获得数据库连接
    #=========================================================================
    def get_db_conn(self):
        db = self.db
        plat_str = platform.system()
        if plat_str == "Windows":
            connection_string ='Driver={SQL Server Native Client 10.0};Server=%s;Database=%s;Uid=%s;Pwd=%s;PORT=%s;' % (self.db["HOST"], self.db["NAME"], self.db["USER"], self.db["PASSWORD"], self.db["PORT"])
            self.db_conn = pypyodbc.connect(connection_string)
        else:
            connection_string ='DSN=MySQLServerDatabase;Server=%s;Database=%s;Uid=%s;Pwd=%s;PORT=%s;' % (self.db["HOST"], self.db["NAME"], self.db["USER"], self.db["PASSWORD"], self.db["PORT"])
            self.db_conn = pypyodbc.connect(connection_string)
        return self.db_conn

    #=========================================================================
    # get_db_cursor : 获得数据库光标
    #=========================================================================
    def get_db_cursor(self):
        try:
            if self.db_conn:
                self.db_cursor = self.db_conn.cursor()
        except:
            self.db_cursor = self.get_db_conn().cursor()

        return self.db_cursor

    #=========================================================================
    # query : 执行查询语句, fields为查询自动的数组
    #=========================================================================
    def query(self, comm, parameters=[], fileds=[]):
        if plat_str != "Windows":
            return self.query_fileds(comm, parameters, fileds)
        results = []
        self.db_cursor.execute(comm, parameters)
        query_results = self.db_cursor.fetchmany(10)
        columns = [d[0].lower() for d in self.db_cursor.description]
        while query_results:
            result = [dict(zip(columns, record)) for record in query_results]
            results.extend(result)
            query_results = self.db_cursor.fetchmany(10)
        return results
    
    #===========================================================================
    # match_fileds
    #===========================================================================
    def match_fileds(self, text):
        values = []
        for tmp in text.split(","):
            tmp_col = tmp.split(" as ")
            if len(tmp_col) == 2:
                values.append(tmp_col[1].strip())
            else:
                values.append(tmp_col[0].strip())
        return values
    
    #=========================================================================
    # query : 执行查询语句, fields为查询自动的数组
    #=========================================================================
    def query_fileds(self, comm, parameters=[], fileds=[]):
        comm = comm.lower()
        select_pattern = re.compile("select (.*) from")
        match = select_pattern.search(comm)
        if match:
            text = str(match.group(1))
            fileds = self.match_fileds(text)
            
        results = []
        self.db_cursor.execute(comm, parameters)
        query_results = self.db_cursor.fetchmany(10)
        while query_results:
            for record in query_results:
                tmp = {}
                for i in range(0, len(fileds)):
                    tmp_key = fileds[i]
                    tmp[tmp_key] = record[i]
                results.append(tmp)
            query_results = self.db_cursor.fetchmany(10)
        return results

    #=========================================================================
    # update : 执行update语句
    #=========================================================================
    def update(self, comm, parameters=[]):
        self.db_cursor.execute(comm, parameters)
        self.db_conn.commit()

    #=========================================================================
    # insert_many:
    # sql_str:"insert into python_modules(module_name, file_path) values(:1, :2)"
    # records: [(1, 1), (2,2)]
    #=========================================================================
    def insert_many(self, sql_str, records):
        # sql_str:"insert into python_modules(module_name, file_path) values(:1, :2)"
        # records: [(1, 1), (2,2)]
        self.db_cursor.prepare(sql_str)
        self.db_cursor.executemany(None, records)
        self.db_conn.commit()

    #=========================================================================
    # escape_string :
    #=========================================================================
    def escape_string(self, str):
        str = str.replace("'", "")
        str = str.replace('"', "")

    #=========================================================================
    # close_db_connection : 关闭数据库连接
    #=========================================================================
    def close_db_conn(self):
        try:
            self.db_conn.close()
            print('close db connection success')
        except:
            print('close db connection failure')

    #=========================================================================
    # close : 关闭数据库连接
    #=========================================================================
    def close(self):
        self.close_db_conn()

    #=========================================================================
    # test : 数据库测试
    #=========================================================================
    def test(self):
        sql = 'select id, name, type, poi_x,poi_y from pps_poi_data'
        self.db_cursor.execute(sql)
        result = self.db_cursor.fetchall()
        for i in result:
            print(i[1])

if __name__ == "__main__":
	db_config_local={'USER': 'USER', 'PASSWORD': 'PASSWORD', 'NAME': 'NAME', 'HOST': 'HOST', 'PORT': 50000}
	sqlserver = MsSqlUtil(self.db_config_local)