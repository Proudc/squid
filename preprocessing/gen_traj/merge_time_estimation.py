import re
import os
import numpy as np
def get_time_estimation_result(path):
    data = open(path, 'r')
    text = data.read()
    split = re.split("\n", text)
    result = {}
    for each in split:
        record = re.split(",", each)
        if len(record) == 0:
            continue
        else:
            if record[0] == '':
                continue
            key = int(record[0])
            value = []
            for i in range(1, len(record)):
                if record[i] != '':
                    value.append(int(record[i]))
            result[key] = value
    return result

def write_result_to_path(path, result):
    f = open(path, 'a')
    for key in result:
        value = result[key]
        mean =  np.mean(value)
        f.write(str(key) + "," + str(value) + "\n")
    f.close()

if __name__ == '__main__':
    total_traj_num = 26000
    root_read_path = "2015-04-01/mm_result_time_eatimation"
    result_path = "2015-04-01/merge.txt"
    result = {}
    for i in range(1, total_traj_num):
        read_path = root_read_path + str(i) + ".txt"
        if not os.path.exists(read_path):
            continue
        tem_result = get_time_estimation_result(read_path)
        for key in tem_result:
            value = tem_result[key]
            if key in result:
                result[key].extend(value)
            else:
                result[key] = value
    final_result = {}
    for key in result:
        value = result[key]
        mean_value = np.mean(value)
        final_result[key] = mean_value
    write_result_to_path(result_path, final_result)