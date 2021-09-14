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
        edge.append(int(each))
    return edge
    

def get_distance_from_point_to_line(x, y, x1, y1, x2, y2):
    A = y2 - y1
    B = x1 - x2
    C = (y1 - y2) * x1 + (x2 - x1) * y1
    distance = np.abs(A * x + B * y + C) / (np.sqrt(A**2 + B**2))
    return distance

def change_time_to_seconds(date, t_time):
    curr_time = date + " " + t_time
    timeArray = time.strptime(curr_time, "%Y-%m-%d %H:%M:%S")
    timeStamp = int(time.mktime(timeArray))
    return timeStamp


def time_estimation(traj_list, mm_edge, node, edge):
    last_edge_id = -1
    in_time = []
    out_time = []
    pos = 0
    total_time_span = {}
    print("traj length is: ", len(traj_list))
    for point in traj_list:
        lon = float(point[1])
        lat = float(point[2])
        date = point[3]
        time = point[4]
        min_distance = float('inf')
        min_edge_id = 0
        for test_edge_id in mm_edge:
            fir_node_id = int(edge[test_edge_id][0])
            sec_node_id = int(edge[test_edge_id][1])
            lon1 = float(node[fir_node_id][0])
            lat1 = float(node[fir_node_id][1])
            lon2 = float(node[sec_node_id][0])
            lat2 = float(node[sec_node_id][1])
            distance = get_distance_from_point_to_line(lon, lat, lon1, lat1, lon2, lat2)
            if distance < min_distance:
                min_distance = distance
                min_edge_id = test_edge_id
        if pos == 0:
            pos += 1
            last_edge_id = min_edge_id
            in_time = [date, time]
            out_time = [date, time]
        else:
            pos += 1
            if min_edge_id == last_edge_id:
                out_time = [date, time]
            else:
                tem_in_time = change_time_to_seconds(in_time[0], in_time[1])
                tem_out_time = change_time_to_seconds(out_time[0], out_time[1])
                time_span = tem_out_time - tem_in_time
                if last_edge_id in total_time_span:
                    if time_span != 0:
                        total_time_span[last_edge_id].append(time_span)
                else:
                    if time_span != 0:
                        total_time_span[last_edge_id] = [time_span]
                last_edge_id = min_edge_id
                in_time = [date, time]
                out_time = [date, time]
    print("total_time_span is: ")
    print(total_time_span)
    return total_time_span


def write_time_span_to_path(total_time_span, path):
    f = open(path, 'a')
    for key in total_time_span:
        f.write(str(key) + ",")
        list = total_time_span[key]
        for value in list:
            if value != 0:
                f.write(str(value) + ",")
        f.write("\n")
    f.close()


if __name__ == '__main__':
    node_path = "node.txt"
    edge_path = "edge.txt"
    mm_edge_root_path = "2015-04-01/mm_result"
    raw_traj_root_path = "2015-04-01/"
    start_num = int(sys.argv[1])
    stop_num = int(sys.argv[2])
    node = get_node(node_path)
    edge = get_edge(edge_path)
    for i in range(start_num, stop_num):
        if i == 0:
            continue
        raw_traj_path = raw_traj_root_path + str(i) + ".txt"
        pos = int(i / 500)
        mm_edge_path = mm_edge_root_path + str(pos) + ".csv"
        result_write_path = mm_edge_root_path + "_time_eatimation" + str(i) + ".txt"
        traj_list = get_raw_traj_point(raw_traj_path)
        mm_edge = get_mmedge(mm_edge_path, i)
        total_time_span = time_estimation(traj_list, mm_edge, node, edge)
        write_time_span_to_path(total_time_span, result_write_path)

    