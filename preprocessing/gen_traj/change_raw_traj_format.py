import re
def change_raw_traj_format(read_path, write_path, pos, change_traj_num):
    edge_data = open(read_path, 'r')
    edge_text = edge_data.read()
    edge_split = re.split("\n", edge_text)
    f = open(write_path, 'a')
    if pos == 1 or (i % change_traj_num) == 0:
        f.write("TRAJ_ID,LON,LAT\n")

    for each_edge in edge_split:
        each_record = re.split(" |\t|,", each_edge)
        if len(each_record) < 3:
            continue
        f.write(str(int(each_record[0][5:])) + "," + each_record[2] + "," + each_record[1] + "\n")
    f.close()


if __name__ == '__main__':
    total_traj_num = 26001
    change_traj_num = 500
    root_read_path = "2015-04-01/"
    root_write_path = "2015-04-01/total_raw_traj"
    total_write_path = "2015-04-01/total_raw_traj.txt"
    for i in range(1, total_traj_num):
        pos = int(i / change_traj_num)
        read_path = root_read_path + str(i) + ".txt"
        write_path = root_write_path + str(pos) + ".txt"
        print(read_path)
        # change_raw_traj_format(read_path, write_path)
        change_raw_traj_format(read_path, write_path, i, change_traj_num)