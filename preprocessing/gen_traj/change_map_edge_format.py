import re

def get_node(node_path):
    print("开始读取node")
    node_data = open(node_path, 'r')
    node_text = node_data.read()
    node_split = re.split("\n", node_text)
    node = []
    id_list = []
    coor_list = []
    for each_node in node_split:
        each_record = re.split(" |\t", each_node)
        if len(each_record) < 3:
            continue
        curr_node = [int(each_record[0]), each_record[1], each_record[2]]
        node.append(curr_node)
        id_list.append(int(each_record[0]))
        coor_list.append(each_record[1] + "-" + each_record[2])
    print("读取node结束")
    return id_list, coor_list, node


def get_node2(node_path):
    print("开始读取node")
    node_data = open(node_path, 'r')
    node_text = node_data.read()
    node_split = re.split("\n", node_text)
    node = {}
    for each_node in node_split:
        each_record = re.split(" |\t", each_node)
        if len(each_record) < 3:
            continue
        curr_node = [each_record[2], each_record[1]]
        node[int(each_record[0])] = curr_node
    print("读取node结束")
    return node


def get_edge(edge_path):
    print("开始读取edge")
    edge_data = open(edge_path, 'r')
    edge_text = edge_data.read()
    edge_split = re.split("\n", edge_text)
    edge = []
    for each_edge in edge_split:
        each_record = re.split(" |\t", each_edge)
        if len(each_record) < 3:
            continue
        curr_edge = [[each_record[3], each_record[4]], [each_record[5], each_record[6]]]
        edge.append(curr_edge)
    print("读取edge结束")
    return edge


def get_edge2(edge_path):
    print("开始读取edge")
    edge_data = open(edge_path, 'r')
    edge_text = edge_data.read()
    edge_split = re.split("\n", edge_text)
    edge = []
    for each_edge in edge_split:
        each_record = re.split(" |\t", each_edge)
        if len(each_record) < 3:
            continue
        curr_edge = [int(each_record[0]), int(each_record[1]), int(each_record[2])]
        edge.append(curr_edge)
    print("读取edge结束")
    return edge


def change_map_edge_format(id_list, coor_list, edge_list, edge_path):
    f = open(edge_path, 'a')
    after_change_format = []
    pos = 0
    for edge in edge_list:
        fir_node = edge[0]
        sec_node = edge[1]
        fir_id = -1
        sec_id = -1
        fir_index = coor_list.index(fir_node[0] + "-" + fir_node[1])
        sec_index = coor_list.index(sec_node[0] + "-" + sec_node[1])
        fir_id = id_list[fir_index]
        sec_id = id_list[sec_index]
        after_change_format.append([pos, fir_id, sec_id, fir_node[0], fir_node[1], sec_node[0], sec_node[1]])
        f.write(str(pos) + "," + str(fir_id) + "," + str(sec_id) + "," + str(fir_node[0]) + "," + str(fir_node[1]) + ","
                    + str(sec_node[0]) + "," + str(sec_node[1]) + "," + str((float(fir_node[0]) + float(sec_node[0])) / 2) + ","
                    + str((float(fir_node[1]) + float(sec_node[1])) / 2) + "\n")
        pos += 1
    f.close()
    return after_change_format
    
def change_map_edge_format2(node_list, edge_list, edge_path):
    f = open(edge_path, 'a')
    f.write("edge,s_node,e_node,s_lng,s_lat,e_lng,e_lat,c_lng,c_lat\n")
    for edge in edge_list:
        edge_id = edge[0]
        fir_id = edge[1]
        sec_id = edge[2]
        lon1 = node_list[fir_id][0]
        lat1 = node_list[fir_id][1]
        lon2 = node_list[sec_id][0]
        lat2 = node_list[sec_id][1]
        f.write(str(edge_id) + "," + str(fir_id) + "," + str(sec_id) + "," + lon1 + "," + lat1 + ","
                    + lon2 + "," + lat2 + "," + str((float(lon1) + float(lon2)) / 2) + ","
                    + str((float(lat1) + float(lat2)) / 2) + "\n")
    f.close()



def write_edge_to_path(edge_list, edge_path):
    f = open(edge_path, 'a')
    for edge in edge_list:
        f.write(str(edge[0]) + "," + str(edge[1]) + "," + str(edge[2]) + "," + str(edge[3]) + "," + str(edge[4]) + ","
                    + str(edge[5]) + "," + str(edge[6]) + "," + str((edge[3] + edge[5]) / 2) + "," + str((edge[4] + edge[7]) / 2) + "\n")
    f.close()

def write_node_to_path(node_list, node_path):
    f = open(node_path, 'a')
    for node in node_list:
        f.write(str(node[0]) + "," + str(node[1]) + "," + str(node[2]) + "\n")
    f.close()


def write_node_to_path2(node_list, node_path):
    f = open(node_path, 'a')
    f.write("node,lng,lat\n")
    for node in node_list:
        f.write(str(node) + "," + node_list[node][0] + "," + node_list[node][1] + "\n")
    f.close()




if __name__ == '__main__':

    before_node_path = "nodeOSM.txt"
    before_edge_path = "edgeOSM.txt"
    after_node_path = "node.txt"
    after_edge_path = "edge.txt"
    node = get_node2(before_node_path)
    edge = get_edge2(before_edge_path)
    change_map_edge_format2(node, edge, after_edge_path)
    write_node_to_path2(node, after_node_path)



