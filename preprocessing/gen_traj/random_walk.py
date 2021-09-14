import re
import sys
import math
import random
import time
import numpy as np

def random_walk(start_node, graph_adj_node, sample_point_limit):
    print("start_node", start_node)
    sample_rate = 5
    gen_path = []
    path_point = 0
    curr_node = start_node
    start_time = time.time()
    last_edge = -1
    while path_point < sample_point_limit:
        stop_time = time.time()
        if stop_time - start_time >= 1:
            return gen_path, 0
        # print(gen_path)
        if curr_node not in graph_adj_node:
            return gen_path, 0
        adj_node = graph_adj_node[curr_node][0]
        adj_edge = graph_adj_node[curr_node][1]
        adj_weight = graph_adj_node[curr_node][2]
        edge_length = graph_adj_node[curr_node][3]
        edge_time = graph_adj_node[curr_node][4]
        
        adj_node2 = []
        adj_edge2 = []
        adj_weight2 = []
        edge_time2 = []
        if last_edge == -1:
            adj_node2 = adj_node
            adj_edge2 = adj_edge
            adj_weight2 = adj_weight
            edge_time2 = edge_time
        else:
            for i in range(len(adj_edge)):
                if adj_edge[i] != last_edge:
                    adj_node2.append(adj_node[i])
                    adj_edge2.append(adj_edge[i])
                    adj_weight2.append(adj_weight[i])
                    edge_time2.append(edge_time[i])

        
        total_weight = np.sum(adj_weight2)
        # print("curr_node", curr_node2)
        # print("adj_node", adj_node2)
        # print("adj_edge", adj_edge2)
        # print("adj_weigth", adj_weight2)
        # print("edge_time", edge_time2)
        random_probability = random.random()
        pos = -1
        if total_weight != 0:
            for tem_pos in range(len(adj_weight2)):
                curr_probability = np.sum(adj_weight2[:tem_pos + 1]) / total_weight
                if random_probability < curr_probability:
                    pos = tem_pos
                    break
        if pos == -1:
            return gen_path, 0
        gen_path.append([adj_edge2[pos], edge_time2[pos]])
        path_point += get_sample_point_num(edge_time2[pos], sample_rate)
        curr_node = adj_node2[pos]
        last_edge = adj_edge2[pos]
    return gen_path, 1

def get_sample_point_num(time, sample_rate):
    return int(time / sample_rate)

def sample_path(path, start_node, end_node, auxiliary_array, total_node, total_edge):
    help_fir_node = start_node
    help_sec_node = 0
    sample_point = []
    for i in range(len(path)):
        help_edge_id = path[i]
        tem_fir_id = total_edge[help_edge_id][0]
        tem_sec_id = total_edge[help_edge_id][1]
        if help_fir_node == tem_fir_id:
            help_fir_node = tem_fir_id
            help_sec_node = tem_sec_id
        else:
            help_fir_node = tem_sec_id
            help_sec_node = tem_fir_id

        # ???
        total_time = auxiliary_array[i][0]
        total_length = auxiliary_array[i][1]
        section_length = total_length / total_time

        lon1 = total_node[help_fir_node][0]
        lat1 = total_node[help_fir_node][1]
        lon2 = total_node[help_sec_node][0]
        lat2 = total_node[help_sec_node][1]
        lon_length = (lon2 - lon1) * section_length / total_length
        lat_length = (lat2 - lat1) * section_length / total_length
        lon = lon1
        lat = lat1
        while(real_distance((lon - lon1), (lat - lat1), lat) < total_length):
            sample_point.append([lon, lat])
            lon += lon_length
            lat += lat_length
        help_fir_node = help_sec_node


def real_distance(lon_diff, lat_diff, lat):
    lon_change_ratio = math.cos(lat)
    lon_distance = lon_diff * 111111 * lon_change_ratio
    lat_distance = lat_diff * 111111
    return math.sqrt(lon_distance * lon_distance, lat_distance * lat_distance)


def get_edge(edge_path):
    print("开始读取edge")
    edge_data = open(edge_path, 'r')
    edge_text = edge_data.read()
    edge_split = re.split("\n", edge_text)
    edge = {}
    total_length = {}
    pos = 0
    for each_node in edge_split:
        if pos == 0:
            pos += 1
            continue 
        each_record = re.split(" |\t|,", each_node)
        if len(each_record) < 2:
            continue
        curr_node = [int(each_record[1]), int(each_record[2])]
        edge[int(each_record[0])] = curr_node
        total_length[int(each_record[0])] = float(each_record[3])
        pos += 1
    print("读取edge结束")
    return edge, total_length

def get_weight(edge_path):
    edge_data = open(edge_path, 'r')
    edge_text = edge_data.read()
    edge_split = re.split("\n", edge_text)
    total_weight = {}
    pos = 0
    for each_node in edge_split:
        each_record = re.split(",|\t|,", each_node)
        if len(each_record) < 2:
            continue
        total_weight[int(each_record[0])] = int(each_record[1])
        pos += 1
    return total_weight

def get_time(edge_path):
    edge_data = open(edge_path, 'r')
    edge_text = edge_data.read()
    edge_split = re.split("\n", edge_text)
    total_time = {}
    pos = 0
    for each_node in edge_split:
        each_record = re.split(" |\t|,", each_node)
        if len(each_record) < 2:
            continue
        total_time[int(each_record[0])] = float(each_record[1])
        pos += 1
    return total_time

def get_graph(edge_path, weight_path, time_path):
    total_edge, total_length   = get_edge(edge_path) # a dict
    total_weight = get_weight(weight_path)
    total_time   = get_time(time_path)
    # print(total_weight)
    graph = {}
    for each_edge in total_edge:
        edge_id = each_edge
        fir_node_id = total_edge[each_edge][0]
        sec_node_id = total_edge[each_edge][1]
        if fir_node_id in graph:
            graph[fir_node_id][0].append(sec_node_id)
            graph[fir_node_id][1].append(edge_id)
            if edge_id in total_weight:
                graph[fir_node_id][2].append(total_weight[edge_id])
            else:
                graph[fir_node_id][2].append(random.randint(0, 5000))
            graph[fir_node_id][3].append(total_length[edge_id])
            if edge_id in total_time:
                graph[fir_node_id][4].append(total_time[edge_id])
            else:
                graph[fir_node_id][4].append(total_length[edge_id] / 16)
        else:
            graph[fir_node_id] = []
            graph[fir_node_id].append([sec_node_id])
            graph[fir_node_id].append([edge_id])
            if edge_id in total_weight:
                graph[fir_node_id].append([total_weight[edge_id]])
            else:
                graph[fir_node_id].append([random.randint(0, 5000)])
            graph[fir_node_id].append([total_length[edge_id]])
            if edge_id in total_time:
                graph[fir_node_id].append([total_time[edge_id]])
            else:
                graph[fir_node_id].append([total_length[edge_id] / 16])

        
        if sec_node_id in graph:
            graph[sec_node_id][0].append(fir_node_id)
            graph[sec_node_id][1].append(edge_id)
            if edge_id in total_weight:
                graph[sec_node_id][2].append(total_weight[edge_id])
            else:
                graph[sec_node_id][2].append(random.randint(0, 5000))
            graph[sec_node_id][3].append(total_length[edge_id])
            if edge_id in total_time:
                graph[sec_node_id][4].append(total_time[edge_id])
            else:
                graph[sec_node_id][4].append(total_length[edge_id] / 16)
        else:
            graph[sec_node_id] = []
            graph[sec_node_id].append([fir_node_id])
            graph[sec_node_id].append([edge_id])
            if edge_id in total_weight:
                graph[sec_node_id].append([total_weight[edge_id]])
            else:
                graph[sec_node_id].append([random.randint(0, 5000)])
            graph[sec_node_id].append([total_length[edge_id]])
            if edge_id in total_time:
                graph[sec_node_id].append([total_time[edge_id]])
            else:
                graph[sec_node_id].append([total_length[edge_id] / 16])
    return graph



def write_traj_to_file(traj, file_path):
    f = open(file_path, 'a')
    for each_edge in traj:
        f.write(str(each_edge[0]) + "," + str(each_edge[1]) + "\n")
    f.close()


if __name__ == '__main__':
    start_num = int(sys.argv[1])
    stop_num = int(sys.argv[2])
    edge_path = "network_properties.txt"
    weight_path = "2015-04-01/mm_result_trans_probility.txt"
    time_path = "2015-04-01/merge.txt"
    graph = get_graph(edge_path, weight_path, time_path)
    root_write_path = "/"
    sample_point_limit = 20000
    pos = start_num
    while(pos < stop_num):
        start_node = random.randint(0, 20800)
        traj, status = random_walk(start_node, graph, sample_point_limit)
        file_path = root_write_path + str(pos) + ".txt"
        print(file_path)
        if status == 0:
            continue
        else:
            write_traj_to_file(traj, file_path)
            pos += 1

        
