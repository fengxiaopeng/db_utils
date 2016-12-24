# -*- coding: utf-8 -*-
# author: ylkjick532428@gmail.com
import os
import pymysql
import re

#=========================================================================
# MySqlUtil : mysql数据库的工具类
#=========================================================================
class MySqlUtil(object):

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
        self.db_conn = pymysql.connect(host=db['HOST'], port=db["PORT"], user=db["USER"], passwd=db["PASSWORD"], db=db["NAME"], charset="utf8")
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
    def query(self, comm, parameters=[]):
        results = []
        self.db_cursor.execute(comm, parameters)
        query_results = self.db_cursor.fetchmany(10)
        columns = [d[0].lower() for d in self.db_cursor.description]
        while query_results:
            result = [dict(zip(columns, record)) for record in query_results]
            results.extend(result)
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
    # print_sql : 
    #=========================================================================
    def print_sql(self, sql, params):
        re_pattern = re.compile("(::::\d+)")
        matchs = re_pattern.findall(sql)
        for i in range(len(matchs) -1, -1, -1):
            match = matchs[i]
            value = params[i]
            if isinstance(params[i], int):
                sql = sql.replace(str(match), str(value))
            elif isinstance(params[i], str):
                value = "'%s'" % value
                sql = sql.replace(match, value)
            else:
                value = "'%s'" % value
                sql = sql.replace(match, value)
        return sql
    
    #=========================================================================
    # escape_string :
    #=========================================================================
    def escape_string(self, str_str):
        str_str = str_str.replace("'", "")
        str_str = str_str.replace('"', "")
        return str_str
		
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
	db_config_local={'NAME': 'baidu',
            'USER': 'baidu',
            'PASSWORD': 'baidu',
            'HOST': 'localhost',
            'PORT': 3306}
			
	MySqlUtil(self.db_config_local)
    
    