import re
import sys
import numpy as np
import pandas as pd
import time, datetime

def get_raw_traj_point(read_path):
    edge_data = open(read_path, 'r')
    edge_text = edge_data.read()
    edge_split = re.split("\n", edge_text)
    traj_list = []
    for each_edge in edge_split:
        each_record = re.split(",", each_edge)
        if len(each_record) < 3:
            continue
        curr_point = [int(each_record[0][5:]), each_record[2], each_record[1], each_record[4], each_record[5]]
        traj_list.append(curr_point)
    return traj_list

def get_node(node_path):
    print("开始读取node")
    node_data = open(node_path, 'r')
    node_text = node_data.read()
    node_split = re.split("\n", node_text)
    node = {}
    pos = 0
    for each_node in node_split:
        if pos == 0:
            pos += 1
            continue 
        each_record = re.split(" |\t|,", each_node)
        if len(each_record) < 3:
            continue
        curr_node = [each_record[1], each_record[2]]
        node[int(each_record[0])] = curr_node
        pos += 1
    print("读取node结束")
    return node


def get_edge(edge_path):
    print("开始读取edge")
    edge_data = open(edge_path, 'r')
    edge_text = edge_data.read()
    edge_split = re.split("\n", edge_text)
    edge = {}
    pos = 0
    for each_node in edge_split:
        if pos == 0:
            pos += 1
            continue 
        each_record = re.split(" |\t|,", each_node)
        if len(each_record) < 3:
            continue
        curr_node = [each_record[1], each_record[2]]
        edge[int(each_record[0])] = curr_node
        pos += 1
    print("读取edge结束")
    return edge


def get_mmedge(path, traj_id):
    mm_result = pd.read_csv(path)
    tem = (mm_result["TRAJ_ID"] == traj_id)
    record = mm_result[tem].head()
    mm_edge_record = record["MATCHED_EDGE"]
    tem_mm = mm_edge_record.tolist()
    mm_edge_str = tem_mm[0]
    mm_edge_str = mm_edge_str[1:len(mm_edge_str) - 1]
    edge_split = re.split(",", mm_edge_str)
    edge = []
    for each in edge_split:
        if each != "":
            edge.append(int(each))
    return edge


def write_time_span_to_path(total_trans_probility, path):
    f = open(path, 'a')
    for key in total_trans_probility:
        f.write(str(key) + ",")
        value = total_trans_probility[key]
        f.write(str(value) + "\n")
    f.close()


if __name__ == '__main__':
    node_path = "node.txt"
    edge_path = "edge.txt"
    mm_edge_root_path = "2015-04-01/mm_result"
    raw_traj_root_path = "2015-04-01/"
    result_write_path = mm_edge_root_path + "_trans_probility.txt"
    start_num = 1
    stop_num = 26000
    node = get_node(node_path)
    edge = get_edge(edge_path)
    total_trans_probility = {}
    for edge_id in edge_path:
        total_trans_probility[edge_id] = 0
    for i in range(start_num, stop_num):
        if i == 0:
            continue
        pos = int(i / 500)
        mm_edge_path = mm_edge_root_path + str(pos) + ".csv"
        print(mm_edge_path)
        mm_edge = get_mmedge(mm_edge_path, i)
        for each_edge_id in mm_edge:
            if each_edge_id in total_trans_probility:
                total_trans_probility[each_edge_id] += 1
            else:
                total_trans_probility[each_edge_id] = 0
    write_time_span_to_path(total_trans_probility, result_write_path)

    