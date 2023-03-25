import sys

from model.interface2dataFile import getData_proc
from model.dataFile2Ads import data2Ads

if __name__ == '__main__':
    table_name=sys.argv[1]
    getData_proc.start_main(table_name=table_name)
    data2Ads = data2Ads(table_name=table_name)
    data2Ads.flow()