import json
from datetime import datetime
import pymysql
from sqlalchemy import create_engine


class P_DB_CONN:
    def __init__(self):
        with open("./conf/db_connect.conf", 'r', encoding='utf-8') as f:
            conn_str = json.load(f)

            # conn_Str = json.loads("".join(conn_str))
            self.mysqlConn = pymysql.connect(**conn_str)
            conn_str1 = 'mysql+pymysql://%(user)s:%(password)s@%(host)s:%(port)d/%(db)s?charset=utf8' % conn_str
            print(conn_str1)
            self.engine = create_engine(conn_str1, encoding='utf-8')

    def getInterfaceInfo(self, table_name):
        sql = f"""
        select interface_table_name,interface_url,params,token,share_token,datakey,incre_type,incre_field,headers from 
        `help`.sync_bigdata_interface_help where interface_table_name = '{table_name}'"""
        # print(sql)
        cursor = self.mysqlConn.cursor()
        cursor.execute(sql)
        res = cursor.fetchall()
        # for interface in res:
        now_time = datetime.now()
        interface = res[0]
        interface_info = {
            "interface_table_name": interface[0],
            "interface_url": interface[1],
            "params": interface[2],
            "token": interface[3],
            "share_token": interface[4],
            "datakey": interface[5],
            "incre_type": interface[6],
            "incre_field": interface[7],
            "now_time": now_time.strftime("%Y-%m-%d %H:%M:%S"),
            "last_time": now_time.strftime("%Y-%m-%d 00:00:00"),
            "pageSize": 1000,
            "pageNo": 0,
            "headers": interface[8]
        }
        print(interface_info)
        return interface_info

    def saveRespInfo(self, tablename, now_time, datakey, file_path, insert_func):
        cursor = self.mysqlConn.cursor()
        now_time = now_time.strftime("%Y-%m-%d %H:%M:%S")
        sql = f"""
            INSERT INTO `help`.`sync_bigdata_interface_result` (
                `table_name`,`insert_time`,`data_field`,`target_directory`,`save_mode`,`request`,`response`,`err_msg`,`update_time`
            )VALUES(
                '{tablename}','{now_time}','{datakey}','{file_path}','{insert_func}','N','N',NULL,'{now_time}'
            )
        """
        cursor.execute(sql)

    def db_query(self, table_name=None, col=None, sql=None):
        """
            封装执行sql语句接口
        """
        if sql is None:
            sql = f"""
                select {col} from {table_name}
            """
        cursor = self.mysqlConn.cursor()
        cursor.execute(sql)
        res = cursor.fetchall()
        return res

    def closeConn(self):
        self.mysqlConn.close()

    def getDataFileInfo(self, table_name):
        cursor = self.mysqlConn.cursor()
        sql = f"""
                select data_field,target_directory from 
                `help`.sync_bigdata_interface_result where table_name = 'ads.{table_name}'"""
        cursor = self.mysqlConn.cursor()
        cursor.execute(sql)
        select_list = cursor.fetchall()
        res_list = []
        for params in select_list:
            tmp = {
                "data_field": params[0],
                "target_directory": params[1]
            }
            res_list.append(dict.copy(tmp))
        return res_list
