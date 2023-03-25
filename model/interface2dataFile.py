import datetime
import json
import os
import utils.mysqlUtils as mysql_utils
import utils.httpUtils as http_utils
import utils.loopUtils as loop_utils


class getData_proc:
    @staticmethod
    def start_main(table_name):
        myUtils = mysql_utils.P_DB_CONN()
        interface_info = myUtils.getInterfaceInfo(table_name)
        if interface_info == {}:
            return
        httpUtils = http_utils.httpUtils()
        params = httpUtils.params_process(interface_info=interface_info)
        headers = httpUtils.setHeaders(interface_info)
        response = httpUtils.send_request(url=interface_info['interface_url'],
                                          kw=params,
                                          headers=headers)

        file_path = './req/' + interface_info['interface_table_name'] + '/' + datetime.datetime.now().strftime(
            '%Y%m%d%H') + '/'

        if not os.path.isdir(file_path):
            os.makedirs(file_path)
        res_json = json.loads(response)
        loopUtils = loop_utils.loopUtils(kw=params,
                                         url=interface_info['interface_url'],
                                         headers=headers,
                                         res_json=res_json,
                                         file_path=file_path,
                                         incre_param=interface_info['incre_field'],
                                         datakey=interface_info['datakey'])
        if 'count' not in res_json or res_json['count'] == res_json['total']:
            httpUtils.saveResponse(file_path=file_path,
                                   response=response)
        elif interface_info['incre_type'] == 'pageNo':
            loopUtils.loopByPageNo()
        else:
            loopUtils.loopByDateTime()
        myUtils.saveRespInfo(
            tablename='ads.' + interface_info["interface_table_name"],
            now_time=datetime.datetime.now(),
            datakey=interface_info["datakey"],
            file_path=file_path,
            # 此处使用overwrite会出现问题，由于后续进行的都是truncate insert的操作，前置库中存在的数据最多只会保留当日数据（某些全量表不受影响）
            insert_func='overwrite'
        )
