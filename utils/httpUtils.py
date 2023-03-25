import json
import datetime

import requests


class httpUtils:

    def __init__(self):
        pass

    """
    构造请求参数，根据params字段入参
    """

    def params_process(self, interface_info) -> dict:
        params = interface_info["params"]
        if params is None:
            return {}
        params_list = json.loads("".join(params))
        make_params = []
        for p in params_list:
            if 'func' not in p:
                print("exception: func 未指定")
                raise Exception("exception: func 未指定")
            if p['func'] == "static" or p['func'] == "固定参数":
                make_params.append(self.setStaticParam(p))
            elif p['func'] == "Authentication" or p['func'] == "认证":
                make_params.append(self.getTokenFromAuthentication(p))
            elif p['func'] == "last_time" or p['func'] == '上一条数据的时间':
                del p['func']
                for key in p.keys():
                    p[key] = interface_info['last_time']
                make_params.append(p)
            elif p['func'] == "now_time" or p['func'] == "当前时间":
                del p['func']
                for key in p.keys():
                    p[key] = interface_info['now_time']
                make_params.append(p)
            else:
                print("exception:" + p['func'] + "不支持的方法")
                raise Exception("exception:" + p['func'] + "不支持的方法")
        res_dict = {k: v for x in make_params for k, v in x.items()}
        print(res_dict)
        return res_dict

    @staticmethod
    def setStaticParam(param) -> dict:
        del param['func']
        return param

    def getTokenFromAuthentication(self, param) -> dict:
        url = param['url']
        del param['func']
        del param['url']
        response = json.loads(self.send_request(url, param))
        return response

    @staticmethod
    def send_request(url, kw, headers={}):
        response = requests.get(url=url, params=kw, headers=headers)
        return response.text

    @staticmethod
    def setHeaders(interface_info) -> dict:
        return {} if interface_info['headers'] == '' or interface_info['headers'] is None else interface_info['headers']

    @staticmethod
    def saveResponse(file_path, response, index=''):
        now_time = datetime.datetime.now()
        with open(file_path + str(int(now_time.timestamp())) + index + '.json', 'w', encoding='utf-8') as f:
            f.write(response)
            f.close()
