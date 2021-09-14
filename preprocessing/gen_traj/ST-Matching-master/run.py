from STmatching_distribution_ver import *
from multiprocessing import cpu_count, Pool


if __name__ == '__main__':
    total_num = 52
    root_read_path = "total_raw_traj"
    root_write_path = "mm_result"
    for i in range(total_num):
        read_path = root_read_path + str(i) + ".txt"
        write_path = root_write_path + str(i) + ".csv"
        trajectory = pd.read_csv(read_path)
        trajectory = data_convert(trajectory)
        pool = Pool()
        match_results = pool.map(trajectory_matching, trajectory)
        pool.close()
        match_results = pd.concat(match_results, ignore_index=True)
        match_results.to_csv(write_path)
        