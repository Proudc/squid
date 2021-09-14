import re
from geopy.distance import geodesic


def get_network_properties(edge_read_path, edge_write_path):
    print("开始读取edge")
    edge_data = open(edge_read_path, 'r')
    edge_text = edge_data.read()
    edge_split = re.split("\n", edge_text)
    f = open(edge_write_path, 'a')
    f.write("section_id,s_node,e_node,length\n")
    pos = 0
    for each_edge in edge_split:
        if pos == 0:
            pos += 1
            continue
        each_record = re.split(" |\t|,", each_edge)
        if len(each_record) < 3:
            continue
        lon1 = float(each_record[3])
        lat1 = float(each_record[4])
        lon2 = float(each_record[5])
        lat2 = float(each_record[6])
        point1 = (lat1, lon1)
        point2 = (lat2, lon2)
        distance = geodesic(point1, point2).m
        f.write(each_record[0] + "," + each_record[1] + "," + each_record[2] + "," + str(distance) + "\n")
        pos += 1
    f.close()
    print("读取edge结束")

if __name__ == '__main__':
    before_edge_path = "edge.txt"
    after_edge_path = "network_properties.txt"
    get_network_properties(before_edge_path, after_edge_path)