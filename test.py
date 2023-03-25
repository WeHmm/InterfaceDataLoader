import sys

from model.interface2dataFile import getData_proc
from model.dataFile2Ads import data2Ads
import utils.mysqlUtils as mutils

if __name__ == '__main__':
    mu = mutils.P_DB_CONN()
    sql = """
        SELECT interface_table_name FROM `help`.`sync_bigdata_interface_help`
    """
    res_list = mu.db_query(sql=sql)
    for res in res_list:
        # print(res[0])
        try:
            getData_proc.start_main(table_name=res[0])
            d2a = data2Ads(table_name=res[0])
            d2a.flow()
        except:
            print(res[0]+"出现异常")
