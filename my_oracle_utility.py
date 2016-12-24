# -*- coding: utf-8 -*-
# author: ylkjick532428@gmail.com
import cx_Oracle
import os
from sqlalchemy.util.langhelpers import NoneType
os.environ['NLS_LANG'] = 'SIMPLIFIED CHINESE_CHINA.UTF8'

#=========================================================================
# MyOracleUtil : oracle数据库的工具类
#=========================================================================

class MyOracleUtil(object):

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
        if "SID" in self.db:
            if self.db["SID"]:
                tns = cx_Oracle.makedsn(
                self.db['HOST'], int(self.db["PORT"]), self.db["NAME"])
            else:
                tns='%s:%s/%s' % (self.db['HOST'], self.db["PORT"], self.db["NAME"])
        else:
            tns='%s:%s/%s' % (self.db['HOST'], self.db["PORT"], self.db["NAME"])
            
        self.db_conn = cx_Oracle.connect(
            self.db["USER"], self.db["PASSWORD"], tns)
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
        try:
            self.db_cursor.execute(comm, parameters)
        except cx_Oracle.OperationalError:
            self.close()
            self.get_db_conn()
            self.get_db_cursor()
            self.db_cursor.execute(comm, parameters)
        query_results = self.db_cursor.fetchmany(10)
        columns = [d[0].lower() for d in self.db_cursor.description]
        while query_results:
            result = [dict(zip(columns, record)) for record in query_results]
            results.extend(result)
            query_results = self.db_cursor.fetchmany(10)
        return results
    
    #=========================================================================
    # table_fileds : 获得表字段数组
    #=========================================================================
    def table_fileds(self, table_name):
        comm = "select * from %s  where ROWNUM< 1" % table_name
        results = []
        self.db_cursor.execute(comm)
        query_results = self.db_cursor.fetchmany(10)
        columns = [d[0].lower() for d in self.db_cursor.description]
        return columns
    
    #=========================================================================
    # table_fileds : 获得表字段注释
    #=========================================================================
    def table_fileds_type(self, table_name, fileds):
        tmp_fileds = [tmp.upper() for tmp in fileds]
        comm = "select column_name, data_type, data_length from user_tab_columns \
        where table_name='%s' and column_name in ('%s')" \
        % (table_name, "','".join(tmp_fileds))
        return self.query(comm)
    
    #===========================================================================
    # tab_comments : 表注释
    #===========================================================================
    def tab_comments(self):
        sql_str = "select * from user_tab_comments"
        rows = self.query(sql_str)
        result = {}
        for row in rows:
            table_name = row["table_name"]
            result[table_name] = row["comments"]
        return result
    
    
    #===========================================================================
    # col_comments :列注释
    #===========================================================================
    def col_comments(self):
        sql_str = "select * from user_col_comments"
        rows = self.query(sql_str)
        result = {}
        for row in rows:
            table_name = row["table_name"]
            if table_name not in result:
                result[table_name] = {}
            column_name = row["column_name"]
            result[table_name][column_name] = row["comments"] 
        return result
        
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

    #===============================================================================
    # get_query_str
    #===============================================================================
    def get_query_str(self, query_oracle, table_name1, need_fileds, time_split_filed, start_time, end_time):
        fileds_str = ",".join(need_fileds)
        sql_str = "select * from %s t1 where t1.%s >= '%s' and t1.%s < '%s' " % (table_name1, time_split_filed, start_time, time_split_filed, end_time)
        print (sql_str)
        result = []
        for row in query_oracle.query(sql_str):
            tmp = {}
            for key, value in row.items():
                if key.upper() in need_fileds:
                    tmp[key] = value
            if tmp:
                result.append(tmp)
        return result

    #===============================================================================
    # get_update_str
    #===============================================================================
    def get_update_str(self, tmp_table_name, tmp_data, where_fileds, tmp_fileds=[]):
        if not tmp_fileds:
            tmp_fileds = sorted(tmp_data.keys())
        
        value_str = ""
        where_str = ""
        for tmp_filed in tmp_fileds:
            tmp_value = tmp_data[tmp_filed]
            value_type = type(tmp_value)
            if tmp_filed in where_fileds:
                if value_type == int:
                    where_str = "%s and %s=%s" % (where_str, tmp_filed, tmp_value)
                elif value_type == str:
                    where_str = "%s and %s='%s'" % (where_str, tmp_filed, tmp_value.strip())
                elif value_type == float:
                    where_str = "%s and %s=%s" % (where_str, tmp_filed, tmp_value)
                elif value_type == NoneType:
                    continue
                else:
                    where_str = "%s and %s='%s'" % (where_str, tmp_filed, tmp_value.strip())
            else:
                if value_type == int:
                    value_str = "%s,%s=%s" % (value_str, tmp_filed, tmp_value)
                elif value_type == str:
                    value_str = "%s,%s='%s'" % (value_str, tmp_filed, tmp_value.strip())
                elif value_type == float:
                    value_str = "%s,%s=%s" % (value_str, tmp_filed, tmp_value)
                elif value_type == NoneType:
                    continue
                else:
                    value_str = "%s,%s='%s'" % (value_str, tmp_filed, tmp_value.strip())
        value_str = value_str[1:]
        where_str = where_str[5:]
        text = "update %s set %s where %s" % (tmp_table_name, value_str, where_str)
        return text
    
    #===============================================================================
    # get_insert_str
    #===============================================================================
    def get_insert_str(self, tmp_table_name, tmp_data, tmp_fileds=[], uuid=""):
        if not tmp_fileds:
            tmp_fileds = sorted(tmp_data.keys())
        
        value_str = ""
        have_fileds = []
        for tmp_filed in tmp_fileds:
            tmp_value = tmp_data[tmp_filed]
            value_type = type(tmp_value)
            if value_type == int:
                value_str = "%s,%s" % (value_str, tmp_value)
            elif value_type == str:
                value_str = "%s,'%s'" % (value_str, tmp_value.strip())
            elif value_type == float:
                value_str = "%s,%s" % (value_str, tmp_value)
            elif value_type == NoneType:
                continue
            else:
                value_str = "%s,'%s'" % (value_str, tmp_value.strip())
            have_fileds.append(tmp_filed)
        value_str = value_str[1:]
        if not uuid:
            text = "insert into %s(\"%s\") values (%s)" % (tmp_table_name, '","'.join(have_fileds).upper(), value_str)
        else:
            text = "insert into %s(\"%s\",\"%s\") values (sys_guid(),%s)" % (tmp_table_name, uuid.upper(),'","'.join(have_fileds).upper(), value_str)
        return text

    #===============================================================================
    # check_is_exist
    #===============================================================================
    def check_is_exist_key(self, query_oracle, tmp_table_name, tmp_filed, tmp_uninue_id):
        sql_str = "select count(*) as count from %s where %s='%s'" % (tmp_table_name, tmp_filed, tmp_uninue_id)
        print (sql_str)
        tmp_count = query_oracle.query(sql_str)[0]["count"]
        return tmp_count
    
    #===============================================================================
    # check_is_exist
    #===============================================================================
    def check_is_exist(self,query_oracle,  tmp_table_name, where_fileds, tmp_data):
        where_str = ""
        for tmp_filed in where_fileds:
            tmp_value = tmp_data[tmp_filed]
            value_type = type(tmp_value)
            if value_type == int:
                where_str = "%s and %s=%s" % (where_str, tmp_filed, tmp_value)
            elif value_type == str:
                where_str = "%s and %s='%s'" % (where_str, tmp_filed, tmp_value.strip())
            elif value_type == float:
                where_str = "%s and %s=%s" % (where_str, tmp_filed, tmp_value)
            elif value_type == NoneType:
                continue
            else:
                where_str = "%s and %s='%s'" % (where_str, tmp_filed, tmp_value.strip())
    
        sql_str = "select count(*) as count from %s where %s" % (tmp_table_name, where_str[4:])
        tmp_count = query_oracle.query(sql_str)[0]["count"]
        return tmp_count

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
	db_config_local = {'PORT': 'PORT', 'SID': False, 'PASSWORD': 'PASSWORD', 'USER': 'USER', 'HOST': 'USER', 'NAME': 'NAME'}
	oracle = MyOracleUtil(db_config_local)
    sql_str = "select period_id, interval_upper, interval_lower from PD_PATROL_PERIOD where pd_id=:pd_id order by period_id"
    print(sql_str)
    rows = oracle.query(sql_str, [str(176)])
	for row in rows:
	    print (row)
