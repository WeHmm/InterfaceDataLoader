from pypinyin import pinyin, Style
from jsonpath_ng import parse


# 定义一个函数，用于将中文字符串转换为拼音字符串
def cn2py(cn):
    return ''.join([i[0] for i in pinyin(cn, style=Style.NORMAL)])


class jsonUtils:
    # 定义一个递归函数，用于遍历json中的所有key，将中文key转换为拼音key
    def recursive_cn2py(self, data):
        if isinstance(data, dict):
            new_data = {}
            for key, value in data.items():
                if isinstance(key, str) and any('\u4e00' <= c <= '\u9fa5' for c in key):  # 判断是否为中文字符
                    new_key = cn2py(key)
                else:
                    new_key = key
                new_data[new_key] = self.recursive_cn2py(value)
            return new_data
        elif isinstance(data, list):
            return [self.recursive_cn2py(item) for item in data]
        else:
            return data



    @staticmethod
    def extract_data(json_string, path):
        jsonpath_expr = parse(path)
        matches = jsonpath_expr.find(json_string)
        match_list = [match.value for match in matches]
        return match_list
