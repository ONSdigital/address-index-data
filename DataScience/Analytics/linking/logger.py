"""
Simple Logger
=============

A function to set up a logging method.


Author
------

:author: Sami Niemi (sami.niemi@valtech.co.uk)


Version
-------

:version: 0.1
:date: 25-Nov-2016
"""
import logging
import logging.handlers


def set_up_logger(log_filename, logger_name='logger'):
    """
    Sets up a logger.

    :param log_filename: name of the file to save the log
    :type log_filename: str
    :param logger_name: name of the logger
    :type logger_name: str

    :return: logger instance
    """
    # create logger
    logger = logging.getLogger(logger_name)
    logger.setLevel(logging.DEBUG)

    # Add the log message handler to the logger
    handler = logging.handlers.RotatingFileHandler(log_filename)

    # create formatter
    formatter = logging.Formatter('%(asctime)s - %(module)s - %(funcName)s - %(levelname)s - %(message)s')

    # add formatter to ch
    handler.setFormatter(formatter)

    # add handler to logger
    logger.addHandler(handler)

    return logger
