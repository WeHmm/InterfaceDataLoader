import datetime
import json
import math

from utils.httpUtils import httpUtils
import utils.jsonUtils as json_utils


class loopUtils:
    def __init__(self, kw, url, headers, res_json, file_path, incre_param, datakey=''):
        self.res_json = res_json
        self.file_path = file_path
        self.kw = kw
        self.url = url
        self.headers = headers
        self.datakey = datakey
        self.incre_param = incre_param

    def loopByPageNo(self, datetime_idx=''):
        loop_sum = math.ceil(self.res_json['total'] / self.res_json['count'])
        if datetime_idx != '':
            datetime_idx = datetime_idx + '_'
        for i in range(0, loop_sum):
            idx = i + 1
            self.kw[self.incre_param] = idx
            response = httpUtils.send_request(url=self.url,
                                              kw=self.kw,
                                              headers=self.headers)
            httpUtils.saveResponse(file_path=self.file_path,
                                   response=response,
                                   index='_' + datetime_idx + str(idx))

    def loopByDateTime(self):
        jUtils = json_utils.jsonUtils()
        idx = 0
        response = self.res_json
        while True:
            # 获取记录集
            # records = response[self.datakey]
            records = jUtils.extract_data(response, self.datakey)
            if len(records) == 0:
                return
            if idx == 24:
                return
            for record in records:
                # 获取时间最大的一条记录作为查询参数
                record_time = datetime.datetime.strptime(record[self.kw['incre_field']], "%Y-%m-%d %H:%M:%S")
                last_time = datetime.datetime.strptime(self.kw['last_time'], "%Y-%m-%d %H:%M:%S")
                if record_time > last_time:
                    self.kw[self.incre_param] = record_time
                response = httpUtils.send_request(url=self.url, kw=self.kw, headers=self.headers)
                httpUtils.saveResponse(self.file_path, response, index='_' + str(idx))
                idx += 1
                res_json = json.loads(response)
                # 判断当前参数下数据是否已全量获取
                if res_json['count'] >= res_json['total']:
                    return
