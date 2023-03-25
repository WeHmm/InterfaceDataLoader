import os

import pandas as pd
import json
import utils.mysqlUtils as mysql_utils
import utils.jsonUtils as json_utils

def ReadJsonFile(file):
    with open(file, encoding='utf-8') as f:
        txt = json.load(f)
    return txt


def GetFileList(path):
    fileList = os.listdir(path)
    return [i for i in fileList if i.split('.')[-1] == 'json']


class data2Ads:
    def __init__(self, table_name):
        self.myUtils = mysql_utils.P_DB_CONN()
        self.table_name = table_name
        self.dataFileInfo_list = self.myUtils.getDataFileInfo(table_name=table_name)
        print(self.dataFileInfo_list)

    def flow(self):
        jUtils = json_utils.jsonUtils()
        # 获取表名当天所有同步的记录集

        df_list = []
        for dataFileInfo in self.dataFileInfo_list:
            file_list = GetFileList(dataFileInfo['target_directory'])
            for file in file_list:
                # 读取json文件，返回一个json
                default = ReadJsonFile(dataFileInfo['target_directory'] + file)
                # 中文字段处理为拼音，同时查找到指定位置的数据，返回json_list
                res = jUtils.extract_data(jUtils.recursive_cn2py(default), dataFileInfo['data_field'])
                df = pd.DataFrame(res)
                if len(df) == 0:
                    continue
                df_list.append(df.copy())
        if len(df_list) == 0:
            return
        # pandas 去重
        merge_df = pd.concat(df_list, ignore_index=True)
        print(merge_df)
        res_df = merge_df.drop_duplicates()
        print(res_df)
        res_df.to_sql(
            name=self.table_name,
            index=False,
            schema='ads',
            con=self.myUtils.engine,
            if_exists='replace'
        )
