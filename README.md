# SQUID: Subtrajectory Query in Trillion-Scale GPS Database

The code implements SQUID algorithm for subtrajectory query and join.



## Supported query modes

1. subtrajectory query
2. subtrajectory join
3. dtj-mr-join
4. dtj-mr-c-join



## Trajectory generation

1. **Map Matching. **Use preprocessing/gen_traj/ST_Matching-master/run.ipynb to run the Map Matching algorithm. For detailed usage, please refer to preprocessing/gen_traj/ST_Matching-master/README.md.
2. **Time estimation.** Run preprocessing/gen_traj/time_estimation.py and get the time estimation result.
3. **Merge time estimation.** Run preprocessing/gen_traj/merge_time_estimation.py to merge the time estimation result.
4. **Transfer probability.** Run preprocessing/gen_traj/merge_trans_probility.py and get the transfer probability result.
5. **Random walk.** Run preprocessing/random_walk.py and get the final result.
6. **Trajectory sampling.** Use preprocessing/sampleTraj/trajGen.sh to sample the trajectories.



## SQUID & DTJ-MR-C data preprocessing

#### Text file

Initial file format:

```.txt
trajectoryID	time_stamp	longitude	latitude
```

Example:

```.txt
0	0	103.6	1.22
0	1	103.6	1.22
0	2	103.6	1.22
...
```

#### Text to binary and GZIP

You have to do perform binary conversion on text data and use gzip algorithm to compress the data.

#### Time partition file

Use preprocessing/TimePartition.java for time partition processing

#### Space partition file

Use preprocessing/SpacePartition.java for space partition processing

#### Trajic file

Use preprocessing/trajic-float/Trajic.java for zip processing



## DTJ-MR data preprocessing

#### Binary file

Each record format:

```.txt
trajID[4 Bytes]	timeStamp[2 Bytes]	longitude[4 Bytes]	latitude[4 Bytes]
```

Example:

```.txt
0	0	103.6	1.22
0	1	103.6	1.22
...
```

#### Time partition file

Use preprocessing/TimePartition.java for time partition processing



## Usage

#### SQUID-Query

```
squid/start_squid_query.sh
```

#### SQUID-Join

```
squid/start_squid_join.sh
```

#### DTJ-MR

```
squid/start_dtjmr.sh
```

#### DTJ-MR-C

```
squid/start_dtj_mr_c.sh
```



## Contributors

- Zhihao Chang: changzhihao@zju.edu.cn



## Reference
If you want to use this code, please cite:
```
@article{DBLP:journals/vldb/ZhangCYLTCC23,
  author       = {Dongxiang Zhang and
                  Zhihao Chang and
                  Dingyu Yang and
                  Dongsheng Li and
                  Kian{-}Lee Tan and
                  Ke Chen and
                  Gang Chen},
  title        = {{SQUID:} subtrajectory query in trillion-scale {GPS} database},
  journal      = {{VLDB} J.},
  volume       = {32},
  number       = {4},
  pages        = {887--904},
  year         = {2023},
}
```
