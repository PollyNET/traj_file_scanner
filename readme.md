# Trajectory files Scanner for Picasso

## Database structure

|ID|filename|category|pollynet_station|gdas1_station|path|start_time|stop_time|upload_time|insert_time|
|:-:|:-----:|:---:|:---:|:----:|:----|:---|:---:|:---:|:---:|
|1|20190913_tel-aviv_multi-geonames-abs-region-ens-below2.0km.png|1|tel-aviv|tel-aviv|/cygdrive/c/Users/zhenping/Desktop/trajectory_scanner|2019-09-13 00:00:00|2019-09-13 23:59:59|2019-09-15 04:00:00|2019-09-15 06:00:00|

### ID

auto-increment from 1
INT

### filename

filename of the figure
varchar

### category

product type
|value|            type                 |
|:---:|:--------------------------------|
|1    |geonames-abs-regions-ens-below2.0|
|2    |geonames-abs-regions-ens-below5.0|
|3    |geonames-abs-regions-ens-below8.0|
|4    |geonames-abs-regions-ens-belowmd |
|5    |land-use-occ-ens-below2.0        |
|6    |land-use-occ-ens-below5.0        |
|7    |land-use-occ-ens-below8.0        |
|8    |land-use-occ-ens-belowmd         |
|9    |trajectories_map                 |
|10   |trajectories_prof                |

INT

### pollynet_station

pollynet station name
varchar

### gdas1_station

gdas1 station name
varchar

Note: this name is unique and it can be projected to multiple pollynet station name. The lookup table can be found in [name_lookup_table](config\station_name_lookup_table.toml).

### path

root dir of the figures
varchar

### start_time

start time of the figure
datetime

### stop_time

stop time of the figure
datetime

### upload_time

upload time of the figure
datetime

### insert_time

insert time of the entry
datetime