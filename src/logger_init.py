import logging
import os
import datetime
import sys


def logger_init(time, folder, *args, force=False, mode='INFO'):
    """
    create the logger with using a new log file.

    Parameters
    ----------

    Returns
    -------

    History
    -------
    2019-09-29. First edition by Zhenping
    """

    if not os.path.exists(folder):
        os.mkdir(folder)

    # initialize the logger
    logfile = '{time}_log'.format(
        time=datetime.datetime.strftime(time, '%Y%m%d_%H%M%S')
        )
    logFullpath = os.path.join(folder, logfile)
    logModeDict = {
        'INFO': logging.INFO,
        'WARNING': logging.WARNING,
        'DEBUG': logging.DEBUG,
        'ERROR': logging.ERROR
        }
    logger = logging.getLogger(__name__)
    logger.setLevel(logModeDict[mode])

    fh = logging.FileHandler(logfile)
    fh.setLevel(logModeDict[mode])
    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(logModeDict[mode])

    formatterFh = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s' +
                                    ' - %(funcName)s - %(lineno)d' +
                                    ' - %(message)s')
    formatterCh = logging.Formatter('%(message)s')
    fh.setFormatter(formatterFh)
    ch.setFormatter(formatterCh)

    logger.addHandler(fh)
    logger.addHandler(ch)

    return logger
