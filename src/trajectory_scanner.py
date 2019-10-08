import os
import sys
import datetime
import toml
import sqlite3
import MySQLdb
import glob
import re
from sqlite3 import Error
from logger_init import logger_init


SCANNER_CONFIG_FILE = 'scanner_config.toml'
PROJECTDIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

###############################################################################
#                                 load config                                 #
###############################################################################
configFullPath = os.path.join(PROJECTDIR, 'config', SCANNER_CONFIG_FILE)
with open(configFullPath, 'r', encoding='utf-8') as fh:
    config = toml.loads(fh.read())

# define the logger for producing the logs
logger = logger_init(
    datetime.datetime.now(),
    os.path.join(PROJECTDIR, 'log'),
    force=True,
    mode=config['LOGMODE']
)


class TrajScanner(object):
    """
    Trajectory results scanner to scan the folder and add the results to
    sqlite3 database.
    """

    def __init__(self):
        """
        initialize the sqlite3 database.
        """

        # load configuration
        configFile = os.path.join(
            PROJECTDIR, 'config', config['DB_CONFIG_FILE'])
        self.db_config = toml.load(configFile)

        lookupTFile = os.path.join(
            PROJECTDIR, 'config', config['STATION_NAME_FILE'])
        with open(lookupTFile, 'r', encoding='utf-8') as fh:
            self.station_name_table = toml.loads(fh.read())

    def db_connect(self):
        """
        Connect/create the SQLite3 database.
        """

        conn = None
        try:
            db_file = os.path.join(
                self.db_config['db_path'],
                self.db_config['db_filename']
            )
            conn = sqlite3.connect(db_file)
            logger.info(sqlite3.version)
            logger.info(
                            'Successfully conect to the database:\n{}'.
                            format(db_file)
                        )
        except Error as e:
            logger.warn(e)
            raise e

        self.conn = conn

    def db_create_table(self):
        """
        create the database table.
        """

        if self.conn is None:
            logger.warn('database does not exist.')
            return False

        try:
            c = self.conn.cursor()
            c.execute(
                self.db_config['sql_query']['create_traj_table']
            )
            self.conn.commit()

            logger.info('Create the table successfully.')
        except Error as e:
            logger.error(e)
            return False

        return True

    def db_insert_entry(self, entry):
        """
        insert data into the database.

        Parameters
        ----------
        entry: tuple
            imgpath: str
            full path of the trajectory results.

            category: int
            category of the trajectory results.
            (details can be found in readme.md)

            pollynet_station: str
            pollynet station name associated with the trajectories.

            gdas1_station: str
            GDAS1 station name associated with the trajectories.

            ending_height: float
            ending height for the trajectories.
            (details can be found in readme.md)

            start_time: datetime obj
            start time of the results.

            stop_time: datetime obj
            stop time of the results.

            upload_time: datetime obj
            uploading time of the results.

        History
        -------
        2019-10-01. First edition by Zhenping.
        """

        if type(entry) is dict:
            entry = [entry]

        for item in entry:
            single_entry_tuple = (
                item['imgpath'],
                item['category'],
                item['pollynet_station'],
                item['gdas1_station'],
                item['ending_height'],
                item['start_time'].strftime('%Y-%m-%d %H:%M:%S'),
                item['stop_time'].strftime('%Y-%m-%d %H:%M:%S'),
                item['upload_time'].strftime('%Y-%m-%d %H:%M:%S'),
                datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            )

            try:
                c = self.conn.cursor()
                c.execute(self.db_config['sql_query']
                          ['insert_traj_entry'], single_entry_tuple)
                self.conn.commit()
                c.close()

            except Error as e:
                logger.error(e)
                logger.info(single_entry_tuple[0])

        return True

    def db_drop_table(self):
        """
        delete the table.
        """

        if self.conn is None:
            logger.warn('database does not exist.')
            return False

        try:
            c = self.conn.cursor()
            c.execute(
                self.db_config['sql_query']['drop_traj_table']
            )
            self.conn.commit()

            logger.info('Delete the table successfully.')
        except Error as e:
            logger.error(e)
            return False

        return True

    def db_delete_entry(self, entry_dict):
        """
        delete entry.

        Parameters
        ----------
        entry_dict: dict
            filename: str
            filename of the trajectory results.

            pollynet_station: str
            pollynet station name.

        History
        -------
        2019-10-01. First edition by Zhenping
        """

        try:
            c = self.conn.cursor()
            c.execute(
                        self.db_config['sql_query']['delete_entry'],
                        (
                            entry_dict['imgpath'],
                            entry_dict['pollynet_station']
                        )
                    )
            self.conn.commit()
            logger.info(
                'Delete the entries with the searching arguments {}'.
                format(str(entry_dict))
                )

            c.close()
        except Error as e:
            logger.error(e)
            return False

        return True

    def db_close(self,):
        """
        close the database.
        """
        self.conn.close()

    def scan_traj_files(self, start_time,
                        elapse_time=datetime.timedelta(days=30)):
        """
        scan the trajectory figures.

        Parameters
        ----------
        start_time: datetime obj
        start time of the searching.

        Keywords
        --------
        elapse_time: timedelta obj
        the searching limit away from the start time.

        Returns
        -------
        figInfoList: dict
            filename: str
            path: str
            station: str

        History
        -------
        2019-10-01. First edition by Zhenping
        """
        fileList = []
        stationList = list(self.station_name_table.keys())

        dateList = [start_time - datetime.timedelta(days=iDay)
                    for iDay in range(
                        0,
                        int(elapse_time / datetime.timedelta(days=1) + 1)
                                     )]
        for thisDate in dateList:
            for station in stationList:
                trjPath = os.path.join(
                    config['TRAJECTORY_ROOT'],
                    station,
                    thisDate.strftime('%Y'),
                    thisDate.strftime('%m'),
                    thisDate.strftime('%d')
                )

                figs = glob.glob(os.path.join(trjPath, '*.png'))
                for fig in figs:
                    figInfo = {
                        'filename': os.path.basename(fig),
                        'path': os.path.dirname(fig),
                        'station': station
                    }

                    fileList.append(figInfo.copy())

        return fileList

    def setup_insert_entries(self, figInfoList):
        """
        convert the figure metadata to database entry.

        Parameters
        ----------
        figInfoList: list
        each single element is the info for each single trajectory plot.

        Returns
        -------
        entryList: list
        each single element is the entry that will be directly inserted into
        the databasse.

        History
        -------
        2019-10-01. First edition by Zhenping
        """

        entryList = []
        entry = {}

        for figInfo in figInfoList:
            pollynetStationNameList = \
                (self.station_name_table[figInfo['station']]['name_PollyNET'])
            for pollynetStation in pollynetStationNameList:
                entry['imgpath'] = os.path.join(
                    figInfo['path'],
                    figInfo['filename']
                )
                entry['category'] = figInfo['category']
                entry['pollynet_station'] = pollynetStation
                entry['gdas1_station'] = figInfo['station']
                entry['ending_height'] = figInfo['ending_height']
                entry['start_time'] = figInfo['start_time']
                entry['stop_time'] = figInfo['stop_time']
                entry['upload_time'] = figInfo['upload_time']

                entryList.append(entry.copy())

        return entryList

    def parse_traj_file(self, fileList):
        """
        parse the content from trajectory results filenames

        Parameters
        ----------
        fileList: list
        each single element is the info for each single trajectory plot, it has
        the variables below:
            filename: str
            path: str
            station: str

        Returns
        -------
        figInfoList: list

        History
        -------
        2019-09-30. First edition by Zhenping
        """

        figInfoList = []
        figInfo = {}

        # convert timedelta string to timedelta object
        dtObj = datetime.datetime.strptime(
            config['INTERVAL_TRAJ_FIG'],
            '%H:%M:%S'
            )
        tdObj = datetime.timedelta(
            hours=dtObj.hour,
            minutes=dtObj.minute,
            seconds=dtObj.second
            )

        traj_prof_pat = re.compile(
            "(?P<date>\d{8})_(?P<hour>\d{2})_(?P<height>\d{5})" +
            "_trajectories_prof.png")
        traj_map_pat = re.compile(
            "(?P<date>\d{8})_(?P<hour>\d{2})_(?P<height>\d{5})" +
            "_trajectories_map.png")
        traj_geonames_20_pat = re.compile(
            "(?P<date>\d{8})_.*-geonames-abs-region-ens-below2.0km.png")
        traj_geonames_50_pat = re.compile(
            "(?P<date>\d{8})_.*-geonames-abs-region-ens-below5.0km.png")
        traj_geonames_80_pat = re.compile(
            "(?P<date>\d{8})_.*-geonames-abs-region-ens-below8.0km.png")
        traj_geonames_md_pat = re.compile(
            "(?P<date>\d{8})_.*-geonames-abs-region-ens-belowmd.png")
        traj_region_land_20_pat = re.compile(
            "(?P<date>\d{8})_.*-land-use-abs-occ-ens-below2.0km.png")
        traj_region_land_50_pat = re.compile(
            "(?P<date>\d{8})_.*-land-use-abs-occ-ens-below5.0km.png")
        traj_region_land_80_pat = re.compile(
            "(?P<date>\d{8})_.*-land-use-abs-occ-ens-below8.0km.png")
        traj_region_land_md_pat = re.compile(
            "(?P<date>\d{8})_.*-land-use-abs-occ-ens-belowmd.png")

        for item in fileList:
            if traj_prof_pat.match(item['filename']):
                res = re.search(traj_prof_pat, item['filename'])

                # construct the returned dict
                figInfo['filename'] = item['filename']
                figInfo['path'] = item['path']
                figInfo['station'] = item['station']
                figInfo['ending_height'] = res.group('height')
                figInfo['start_time'] = datetime.datetime.strptime(
                    res.group('date'), '%Y%m%d')
                figInfo['stop_time'] = figInfo['start_time'] + tdObj
                figInfo['upload_time'] = datetime.datetime.utcfromtimestamp(
                    os.path.getmtime(
                        os.path.join(item['path'], item['filename'])
                    )
                )
                # The category table can be found in readme.md
                figInfo['category'] = 10

            elif traj_map_pat.match(item['filename']):
                res = re.search(traj_map_pat, item['filename'])

                # construct the returned dict
                figInfo['filename'] = item['filename']
                figInfo['path'] = item['path']
                figInfo['station'] = item['station']
                figInfo['ending_height'] = res.group('height')
                figInfo['start_time'] = datetime.datetime.strptime(
                    res.group('date'), '%Y%m%d')
                figInfo['stop_time'] = figInfo['start_time'] + tdObj
                figInfo['upload_time'] = datetime.datetime.utcfromtimestamp(
                    os.path.getmtime(
                        os.path.join(item['path'], item['filename'])
                    )
                )
                # The category table can be found in readme.md
                figInfo['category'] = 9

            elif traj_region_land_20_pat.match(item['filename']):
                res = re.search(traj_region_land_20_pat, item['filename'])

                # construct the returned dict
                figInfo['filename'] = item['filename']
                figInfo['path'] = item['path']
                figInfo['station'] = item['station']
                figInfo['ending_height'] = 0
                figInfo['start_time'] = datetime.datetime.strptime(
                    res.group('date'), '%Y%m%d')
                figInfo['stop_time'] = figInfo['start_time'] + \
                    datetime.timedelta(hours=23, minutes=59, seconds=59)
                figInfo['upload_time'] = datetime.datetime.utcfromtimestamp(
                    os.path.getmtime(
                        os.path.join(item['path'], item['filename'])
                    )
                )
                # The category table can be found in readme.md
                figInfo['category'] = 5

            elif traj_region_land_50_pat.match(item['filename']):
                res = re.search(traj_region_land_50_pat, item['filename'])

                # construct the returned dict
                figInfo['filename'] = item['filename']
                figInfo['path'] = item['path']
                figInfo['station'] = item['station']
                figInfo['ending_height'] = 0
                figInfo['start_time'] = datetime.datetime.strptime(
                    res.group('date'), '%Y%m%d')
                figInfo['stop_time'] = figInfo['start_time'] + \
                    datetime.timedelta(hours=23, minutes=59, seconds=59)
                figInfo['upload_time'] = datetime.datetime.utcfromtimestamp(
                    os.path.getmtime(
                        os.path.join(item['path'], item['filename'])
                    )
                )
                # The category table can be found in readme.md
                figInfo['category'] = 6

            elif traj_region_land_80_pat.match(item['filename']):
                res = re.search(traj_region_land_80_pat, item['filename'])

                # construct the returned dict
                figInfo['filename'] = item['filename']
                figInfo['path'] = item['path']
                figInfo['station'] = item['station']
                figInfo['ending_height'] = 0
                figInfo['start_time'] = datetime.datetime.strptime(
                    res.group('date'), '%Y%m%d')
                figInfo['stop_time'] = figInfo['start_time'] + \
                    datetime.timedelta(hours=23, minutes=59, seconds=59)
                figInfo['upload_time'] = datetime.datetime.utcfromtimestamp(
                    os.path.getmtime(
                        os.path.join(item['path'], item['filename'])
                    )
                )
                # The category table can be found in readme.md
                figInfo['category'] = 7

            elif traj_region_land_md_pat.match(item['filename']):
                res = re.search(traj_region_land_md_pat, item['filename'])

                # construct the returned dict
                figInfo['filename'] = item['filename']
                figInfo['path'] = item['path']
                figInfo['station'] = item['station']
                figInfo['ending_height'] = 0
                figInfo['start_time'] = datetime.datetime.strptime(
                    res.group('date'), '%Y%m%d')
                figInfo['stop_time'] = figInfo['start_time'] + \
                    datetime.timedelta(hours=23, minutes=59, seconds=59)
                figInfo['upload_time'] = datetime.datetime.utcfromtimestamp(
                    os.path.getmtime(
                        os.path.join(item['path'], item['filename'])
                    )
                )
                # The category table can be found in readme.md
                figInfo['category'] = 8

            elif traj_geonames_20_pat.match(item['filename']):
                res = re.search(traj_geonames_20_pat, item['filename'])

                # construct the returned dict
                figInfo['filename'] = item['filename']
                figInfo['path'] = item['path']
                figInfo['station'] = item['station']
                figInfo['ending_height'] = 0
                figInfo['start_time'] = datetime.datetime.strptime(
                    res.group('date'), '%Y%m%d')
                figInfo['stop_time'] = figInfo['start_time'] + \
                    datetime.timedelta(hours=23, minutes=59, seconds=59)
                figInfo['upload_time'] = datetime.datetime.utcfromtimestamp(
                    os.path.getmtime(
                        os.path.join(item['path'], item['filename'])
                    )
                )
                # The category table can be found in readme.md
                figInfo['category'] = 1

            elif traj_geonames_50_pat.match(item['filename']):
                res = re.search(traj_geonames_50_pat, item['filename'])

                # construct the returned dict
                figInfo['filename'] = item['filename']
                figInfo['path'] = item['path']
                figInfo['station'] = item['station']
                figInfo['ending_height'] = 0
                figInfo['start_time'] = datetime.datetime.strptime(
                    res.group('date'), '%Y%m%d')
                figInfo['stop_time'] = figInfo['start_time'] + \
                    datetime.timedelta(hours=23, minutes=59, seconds=59)
                figInfo['upload_time'] = datetime.datetime.utcfromtimestamp(
                    os.path.getmtime(
                        os.path.join(item['path'], item['filename'])
                    )
                )
                # The category table can be found in readme.md
                figInfo['category'] = 2

            elif traj_geonames_80_pat.match(item['filename']):
                res = re.search(traj_geonames_80_pat, item['filename'])

                # construct the returned dict
                figInfo['filename'] = item['filename']
                figInfo['path'] = item['path']
                figInfo['station'] = item['station']
                figInfo['ending_height'] = 0
                figInfo['start_time'] = datetime.datetime.strptime(
                    res.group('date'), '%Y%m%d')
                figInfo['stop_time'] = figInfo['start_time'] + \
                    datetime.timedelta(hours=23, minutes=59, seconds=59)
                figInfo['upload_time'] = datetime.datetime.utcfromtimestamp(
                    os.path.getmtime(
                        os.path.join(item['path'], item['filename'])
                    )
                )
                # The category table can be found in readme.md
                figInfo['category'] = 3

            elif traj_geonames_md_pat.match(item['filename']):
                res = re.search(traj_geonames_md_pat, item['filename'])

                # construct the returned dict
                figInfo['filename'] = item['filename']
                figInfo['path'] = item['path']
                figInfo['station'] = item['station']
                figInfo['ending_height'] = 0
                figInfo['start_time'] = datetime.datetime.strptime(
                    res.group('date'), '%Y%m%d')
                figInfo['stop_time'] = figInfo['start_time'] + \
                    datetime.timedelta(hours=23, minutes=59, seconds=59)
                figInfo['upload_time'] = datetime.datetime.utcfromtimestamp(
                    os.path.getmtime(
                        os.path.join(item['path'], item['filename'])
                    )
                )
                # The category table can be found in readme.md
                figInfo['category'] = 4

            figInfoList.append(figInfo.copy())

        return figInfoList


def convert_to_pollyDB_entry(entryList):
    """
    convert the entry list to pollyDB done_filelist entry.

    Parameters
    ----------
    entryList: list of entries, which have the elements below.
        imgpath: str
        full path of the trajectory results.

        category: int
        category of the trajectory results.
        (details can be found in readme.md)

        pollynet_station: str
        pollynet station name associated with the trajectories.

        gdas1_station: str
        GDAS1 station name associated with the trajectories.

        ending_height: float
        ending height for the trajectories.
        (details can be found in readme.md)

        start_time: datetime obj
        start time of the results.

        stop_time: datetime obj
        stop time of the results.

        upload_time: datetime obj
        uploading time of the results.

    Returns
    -------
    pollyDB_entryList: list of pollyDB entries, which have the elements below
        |name             |example                                   |
        |:---------------:|:-----------------------------------------|
        |lidar            |PollyXT_CGE                               |
        |location         |Evora                                     |
        |starttime        |20190926 00:00:00                         |
        |stoptime         |20190926 05:57:00                         |
        |last_update      |20190930 20:35:07                         |
        |lambda           |355                                       |
        |image            |PollyXT_CGE/2019/09/26/SIG.png            |
        |level            |0                                         |
        |info             |Meteorological data from GDAS1 at evora   |
        |nc_zip_file      |201909/2019_09_26_Thursday_00_00_10.nc.zip|
        |nc_zip_file_size |3036106                                   |
        |active           |1                                         |
        |GDAS             |1                                         |
        |GDAS_timestamp   |20190926 00:00:00                         |
        |lidar_ratio      |50                                        |
        |software_version |1.3                                       |
        |product_type     |SIG                                       |
        |product_starttime|20190926 00:00:00                         |
        |product_stoptime |20190926 00:59:30                         |

    History
    -------
    2019-10-08. First edition by Zhenping.
    """

    if type(entryList) is dict:
        # convert dict to list
        entryList = [entryList]

    # load the pollyAPP configuration
    with open(config['POLLYAPP_CONFIG_FILE'], 'r', encoding='utf-8') as fh:
        POLLYAPP_CONFIG = toml.loads(fh.read())

    # connect to the polly database
    # -> if you do it locally, you need to project the remote port to your
    # localhost. Use the command below:
    # ssh -f Picasso@rsd.tropos.de -L 7800:localhost:3306 -N
    try:
        con = MySQLdb.connect(
            host=POLLYAPP_CONFIG['DATABASE_HOST'],
            port=int(POLLYAPP_CONFIG['DATABASE_PORT']),
            user=POLLYAPP_CONFIG['DATABASE_USER'],
            passwd=POLLYAPP_CONFIG['DATABASE_PASSWORD'],
            db=POLLYAPP_CONFIG['DATABASE_NAME']
        )
        c = con.cursor()
    except Exception as e:
        logger.error('Failure in connecting the polly database')
        raise ConnectionError

    pollyDB_entryList = []
    for entry in entryList:

        # determine whether the entry contains the figures of
        # 1: geonames-abs-regions-ens-below2.0
        # 5: land-use-occ-ens-below2.0
        if (not entry['category'] == 1) and (not entry['category'] == 5):
            continue
        elif entry['category'] == 1:
            product_type = 'airmass_origin_geo'
        elif entry['category'] == 5:
            product_type = 'airmass_origin_landuse'

        # find the polly, nc_zip_file and some other info from the polly
        # database
        c.execute("""SELECT
                        l.name,
                        loc.name,
                        ld.starttime,
                        ld.stoptime,
                        ld.nc_zip_file,
                        ld.nc_zip_file_size,
                        l.active,
                        ld.gdas,
                        ld.gdas_timestamp,
                        ld.software_version
                     FROM
                        lidar_data ld
                            INNER JOIN
                        lidar l
                            INNER JOIN
                        location loc
                     WHERE
                        (loc.name = %s) AND
                        (ld.starttime >= %s) AND
                        (ld.stoptime <= %s) AND
                        (l.location_fk = loc.id) AND
                        (ld.location_fk = loc.id) AND
                        (ld.lidar_fk = l.id);""", (
                        entry['pollynet_station'],
                        entry['start_time'].strftime('%Y-%m-%d %H:%M:%S'),
                        entry['stop_time'].strftime('%Y-%m-%d %H:%M:%S')
                    )
                  )

        res = c.fetchall()

        for item in res:
            pollyDB_entry = {
                'lidar': item[0],
                'location': item[1],
                'starttime': item[2].strftime('%Y%m%d %H:%M:%S'),
                'stoptime': item[3].strftime('%Y%m%d %H:%M:%S'),
                'last_update':
                    datetime.datetime.now().strftime('%Y%m%d %H:%M:%S'),
                'lambda': '355',
                'image': os.path.relpath(
                    entry['imgpath'],
                    config['TRAJECTORY_ROOT']),
                'level': '0',
                'info': 'ensemble trajectory plot that was reanalysed ' +
                        'from HYSPLIT outputs',
                'nc_zip_file': item[4],
                'nc_zip_file_size': item[5],
                'active': item[6],
                'GDAS': '',
                'GDAS_timestamp': '',
                'lidar_ratio': '50',
                'software_version': item[9],
                'product_type': product_type,
                'product_starttime':
                    entry['start_time'].strftime('%Y%m%d %H:%M:%S'),
                'product_stoptime':
                    entry['stop_time'].strftime('%Y%m%d %H:%M:%S')
            }

            pollyDB_entryList.append(pollyDB_entry)

    return pollyDB_entryList


def setup_done_filelist(file, pollyDB_entryList):
    """
    Create the done_filelist.txt to enable the database scanning script to
    write the data into the database.

    Parameters
    ----------
    file: str
    absolute file path of the done_filelist
    pollyDB_entryList: list

    History
    -------
    2019-10-08. First edition by Zhenping.
    """

    with open(file, 'a', encoding='utf-8') as fh:
        for entry in pollyDB_entryList:
            logger.info('write {image}, {loc}, {polly} to the done_filelist'.
                        format(
                            image=entry['image'],
                            loc=entry['location'],
                            polly=entry['lidar']
                            )
                        )
            for key, value in entry.items():
                fh.write('{key}={value}\n'.format(key=key, value=value))

            fh.write('------\n')


def scan_traj_into_sqliteDB():
    """
    scan and add trajectory plots into the sqlite3 Database.
    """

    logger.info('Start to scan the backward trajectory results...')

    scanner = TrajScanner()

    scanner.db_connect()

    scanner.db_create_table()

    # the elapse_time can be control to load all the previous results with
    # being set with a extremely large number, let's say 100000
    fileList = scanner.scan_traj_files(datetime.datetime.now(),
                                       elapse_time=datetime.timedelta(days=30))

    fileInfoList = scanner.parse_traj_file(fileList)

    entryList = scanner.setup_insert_entries(fileInfoList)

    scanner.db_insert_entry(entryList)

    scanner.db_close()


def make_done_filelist_4_traj():
    """
    create the done_filelist for the trajectory plots.
    """

    logger.info('Start to scan the backward trajectory results')

    scanner = TrajScanner()

    filelist = scanner.scan_traj_files(datetime.datetime.now(),
                                       elapse_time=datetime.timedelta(days=30))
    fileInfoList = scanner.parse_traj_file(filelist)
    entryList = scanner.setup_insert_entries(fileInfoList)

    doneEntryList = convert_to_pollyDB_entry(entryList)
    setup_done_filelist(config['DONE_FILELIST'], doneEntryList)


def main():

    # scan the trajectory plots to a local SQLite3 Database
    # scan_traj_into_sqliteDB()

    # scan the trajectory plots and save it to done_filelist.txt
    make_done_filelist_4_traj()


if __name__ == "__main__":
    main()
