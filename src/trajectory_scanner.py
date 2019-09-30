import os
import sys
import datetime
import toml
import sqlite3
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

logger = logger_init(
    datetime.datetime.now(),
    os.path.join(PROJECTDIR, 'log'),
    force=True,
    mode=config['LOGMODE']
)


class TrajScanner(object):

    def __init__(self):
        """
        initialize the sqlite3 database
        """

        # load configuration
        configFile = os.path.join(
            PROJECTDIR, 'config', config['DB_CONFIG_FILE'])
        self.db_config = toml.load(configFile)

        lookupTFile = os.path.join(
            PROJECTDIR, 'config', config['STATION_NAME_FILE'])
        with open(lookupTFile, 'r', encoding='utf-8') as fh:
            self.station_name_table = toml.loads(fh.read())

        conn = None
        try:
            db_file = os.path.join(
                self.db_config['db_path'],
                self.db_config['db_filename']
            )
            conn = sqlite3.connect(db_file)
            logger.info(sqlite3.version)
            logger.info(
                            'Successfully create the database:\n{}'.
                            format(db_file)
                        )
        except Error as e:
            logger.warn(e)
        finally:
            if conn:
                conn.close()

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
                self.db_config['sql_query']['create_traj_table'],
                (self.db_config['table_name'])
            )
            self.conn.commit()

            logger.info('Create the table successfully.')
        except Error as e:
            logger.error(e)
            return False

        return True

    def db_insert_entry(self, entry):
        """
        insert data into the database

        Parameters
        ----------
        entry: tuple
        """
        if type(entry) is dict:
            entry = [entry]

        for item in entry:
            single_entry_tuple = tuple(
                self.db_config['name'],
                item['filename'],
                item['category'],
                item['pollynet_station'],
                item['gdas1_station'],
                item['path'],
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
                return False

        return True

    def db_delete_entry(self, entry_dict):
        """
        delete entry
        """
        try:
            c = self.conn.cursor()
            c.execute(
                        self.db_config['sql_query']['delete_entry'],
                        (
                            self.db_config['table_name'],
                            entry_dict['filename'],
                            entry_dict['pollynet_station']
                        )
                    )
            self.conn.commit()

            c.close()
        except Error as e:
            logger.error(e)
            return False

        return True

    def db_close(self,):
        self.conn.close()

    def scan_traj_files(
        self,
        start_time,
        elapse_time=datetime.timedelta(days=30)
    ):
        """
        scan the trajectory figures.
        """
        fileList = []
        stationList = os.listdir(config['TRAJECTORY_ROOT'])

        dateList = [start_time - datetime.timedelta(days=iDay)
                    for iDay in range(
                        0,
                        elapse_time / datetime.timedelta(days=1)
                                     )]
        for thisDate in datetime:
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

                    fileList.append(figInfo)

    def setup_insert_entries(self, figInfoList):
        """
        convert the figure metadata to database entry.
        """

        entryList = []
        entry = {}

        for figInfo in figInfoList:
            for pollynetStation in self.station_name_table[figInfo['station']]:
                entry['filename'] = figInfo['filename']
                entry['category'] = figInfo['category']
                entry['pollynet_station'] = pollynetStation
                entry['gdas1_station'] = figInfo['station']
                entry['path'] = figInfo['path']
                entry['start_time'] = figInfo['start_time']
                entry['stop_time'] = figInfo['stop_time']

                entryList.append(entry)

        return entryList

    def parse_traj_file(self, fileList):
        """
        parse the content from trajectory results filenames

        Parameters
        ----------
        fileList: list

        Returns
        -------
        figInfoList: list

        History
        -------
        2019-09-30. First edition by Zhenping
        """

        figInfoList = []
        figInfo = {}

        traj_prof_pat = re.compile(
            "(?P<date>\d{8})_(?P<hour>\d{2})_(?P<height>\d{5})" +
            "_trajectories_prof.png")
        traj_map_pat = re.compile(
            "(?P<date>\d{8})_(?P<hour>\d{2})_(?P<height>\d{5})" +
            "_trajectories_map.png")
        traj_geonames_20_pat = re.compile(
            "(?P<date>\d{8})_.*_geonames-abs-region-ens-below2.0km.png")
        traj_geonames_50_pat = re.compile(
            "(?P<date>\d{8})_.*_geonames-abs-region-ens-below5.0km.png")
        traj_geonames_80_pat = re.compile(
            "(?P<date>\d{8})_.*_geonames-abs-region-ens-below8.0km.png")
        traj_geonames_md_pat = re.compile(
            "(?P<date>\d{8})_.*_geonames-abs-region-ens-belowmd.png")
        traj_region_land_20_pat = re.compile(
            "(?P<date>\d{8})_.*_land-use-abs-occ-ens-below2.0km.png")
        traj_region_land_50_pat = re.compile(
            "(?P<date>\d{8})_.*_land-use-abs-occ-ens-below5.0km.png")
        traj_region_land_80_pat = re.compile(
            "(?P<date>\d{8})_.*_land-use-abs-occ-ens-below8.0km.png")
        traj_region_land_md_pat = re.compile(
            "(?P<date>\d{8})_.*_land-use-abs-occ-ens-belowmd.png")

        for item in fileList:
            if traj_prof_pat.match(item['filename']):
                res = re.search(traj_prof_pat, item['filename'])

                # construct the returned dict
                figInfo['filename'] = item['filename']
                figInfo['path'] = item['path']
                figInfo['station'] = item['station']
                figInfo['start_time'] = datetime.datetime.strptime(
                    res.group('date'), '%Y%m%d')
                figInfo['stop_time'] = figInfo['start_time'] + \
                    config['INTERVAL_TRAJ_FIG']
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
                figInfo['start_time'] = datetime.datetime.strptime(
                    res.group('date'), '%Y%m%d')
                figInfo['stop_time'] = figInfo['start_time'] + \
                    config['INTERVAL_TRAJ_FIG']
                # The category table can be found in readme.md
                figInfo['category'] = 9

            elif traj_region_land_20_pat.match(item['filename']):
                res = re.search(traj_region_land_20_pat, item['filename'])

                # construct the returned dict
                figInfo['filename'] = item['filename']
                figInfo['path'] = item['path']
                figInfo['station'] = item['station']
                figInfo['start_time'] = datetime.datetime.strptime(
                    res.group('date'), '%Y%m%d')
                figInfo['stop_time'] = figInfo['start_time'] + \
                    datetime.timedelta(hours=23, minutes=59, seconds=59)
                # The category table can be found in readme.md
                figInfo['category'] = 5

            elif traj_region_land_50_pat.match(item['filename']):
                res = re.search(traj_region_land_50_pat, item['filename'])

                # construct the returned dict
                figInfo['filename'] = item['filename']
                figInfo['path'] = item['path']
                figInfo['station'] = item['station']
                figInfo['start_time'] = datetime.datetime.strptime(
                    res.group('date'), '%Y%m%d')
                figInfo['stop_time'] = figInfo['start_time'] + \
                    datetime.timedelta(hours=23, minutes=59, seconds=59)
                # The category table can be found in readme.md
                figInfo['category'] = 6

            elif traj_region_land_80_pat.match(item['filename']):
                res = re.search(traj_region_land_80_pat, item['filename'])

                # construct the returned dict
                figInfo['filename'] = item['filename']
                figInfo['path'] = item['path']
                figInfo['station'] = item['station']
                figInfo['start_time'] = datetime.datetime.strptime(
                    res.group('date'), '%Y%m%d')
                figInfo['stop_time'] = figInfo['start_time'] + \
                    datetime.timedelta(hours=23, minutes=59, seconds=59)
                # The category table can be found in readme.md
                figInfo['category'] = 7

            elif traj_region_land_md_pat.match(item['filename']):
                res = re.search(traj_region_land_md_pat, item['filename'])

                # construct the returned dict
                figInfo['filename'] = item['filename']
                figInfo['path'] = item['path']
                figInfo['station'] = item['station']
                figInfo['start_time'] = datetime.datetime.strptime(
                    res.group('date'), '%Y%m%d')
                figInfo['stop_time'] = figInfo['start_time'] + \
                    datetime.timedelta(hours=23, minutes=59, seconds=59)
                # The category table can be found in readme.md
                figInfo['category'] = 8

            elif traj_geonames_20_pat.match(item['filename']):
                res = re.search(traj_geonames_20_pat, item['filename'])

                # construct the returned dict
                figInfo['filename'] = item['filename']
                figInfo['path'] = item['path']
                figInfo['station'] = item['station']
                figInfo['start_time'] = datetime.datetime.strptime(
                    res.group('date'), '%Y%m%d')
                figInfo['stop_time'] = figInfo['start_time'] + \
                    datetime.timedelta(hours=23, minutes=59, seconds=59)
                # The category table can be found in readme.md
                figInfo['category'] = 1

            elif traj_geonames_50_pat.match(item['filename']):
                res = re.search(traj_geonames_50_pat, item['filename'])

                # construct the returned dict
                figInfo['filename'] = item['filename']
                figInfo['path'] = item['path']
                figInfo['station'] = item['station']
                figInfo['start_time'] = datetime.datetime.strptime(
                    res.group('date'), '%Y%m%d')
                figInfo['stop_time'] = figInfo['start_time'] + \
                    datetime.timedelta(hours=23, minutes=59, seconds=59)
                # The category table can be found in readme.md
                figInfo['category'] = 2

            elif traj_geonames_80_pat.match(item['filename']):
                res = re.search(traj_geonames_80_pat, item['filename'])

                # construct the returned dict
                figInfo['filename'] = item['filename']
                figInfo['path'] = item['path']
                figInfo['station'] = item['station']
                figInfo['start_time'] = datetime.datetime.strptime(
                    res.group('date'), '%Y%m%d')
                figInfo['stop_time'] = figInfo['start_time'] + \
                    datetime.timedelta(hours=23, minutes=59, seconds=59)
                # The category table can be found in readme.md
                figInfo['category'] = 3

            elif traj_geonames_md_pat.match(item['filename']):
                res = re.search(traj_geonames_md_pat, item['filename'])

                # construct the returned dict
                figInfo['filename'] = item['filename']
                figInfo['path'] = item['path']
                figInfo['station'] = item['station']
                figInfo['start_time'] = datetime.datetime.strptime(
                    res.group('date'), '%Y%m%d')
                figInfo['stop_time'] = figInfo['start_time'] + \
                    datetime.timedelta(hours=23, minutes=59, seconds=59)
                # The category table can be found in readme.md
                figInfo['category'] = 4

            figInfoList.append(figInfo)

        return figInfoList


def main():
    scanner = TrajScanner()
    scanner.db_create_table()

if __name__ == "__main__":
    main()
