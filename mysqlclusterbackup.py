#!/usr/bin/env -S uv run  --script

##########################################################
# mysqlclusterbackup.py
#
# Written by: David C. Brown
# Date: July 2025.
#
# This script helps automate the backup and restore
# of Galera MySQL Clusters.  It uses Percona's XtraBackup
# to back up the files including the ability to do
# incremental backups.
#
# It can also handle restoring backups and backup
# rotations.
##########################################################

import configparser
import argparse
import os
import sys
import re
import json
import subprocess
import logging
import inflect
from datetime import datetime
from logging.handlers import RotatingFileHandler


logger = None
p = inflect.engine()

class Config:
    """
    Class to handle configuration settings.
    """
    def __init__(self, config_file='mysqlclusterbackup.cfg'):
        self.config = configparser.ConfigParser()
        self.config_file = config_file

        if not os.path.exists(self.config_file):
            raise FileNotFoundError(f"Configuration file '{self.config_file}' not found.")

        self.config.read(self.config_file)

        # Load MYSQL_CLUSTER_BACKUP section
        self.mysql_data = self.config.get('MYSQL_CLUSTER_BACKUP', 'MYSQL_DATA')
        self.root_backup_path = self.config.get('MYSQL_CLUSTER_BACKUP', 'ROOT_BACKUP_PATH')
        self.notification_email = self.config.get('MYSQL_CLUSTER_BACKUP', 'NOTIFICATION_EMAIL')
        self.notification_from = self.config.get('MYSQL_CLUSTER_BACKUP', 'NOTIFICATION_FROM')
        self.smtp_server = self.config.get('MYSQL_CLUSTER_BACKUP', 'SMTP_SERVER')
        self.xtrabackup_path = self.config.get('MYSQL_CLUSTER_BACKUP', 'XTRABACKUP_PATH')
        self.logs_directory = self.config.get('MYSQL_CLUSTER_BACKUP', 'LOGS_DIRECTORY')

        # Load MYSQL_CLUSTER_BACKUP_ROTATION section
        self.beginning_of_week = self.config.getint('MYSQL_CLUSTER_BACKUP_ROTATION', 'BEGINNING_OF_WEEK')
        self.yearly_backup_date = self.config.getint('MYSQL_CLUSTER_BACKUP_ROTATION', 'YEARLY_BACKUP_DATE')
        self.weekly_backup_count = self.config.getint('MYSQL_CLUSTER_BACKUP_ROTATION', 'WEEKLY_BACKUP_COUNT')
        self.monthly_backup_count = self.config.getint('MYSQL_CLUSTER_BACKUP_ROTATION', 'MONTHLY_BACKUP_COUNT')
        self.yearly_backup_count = self.config.getint('MYSQL_CLUSTER_BACKUP_ROTATION', 'YEARLY_BACKUP_COUNT')


    def get(self, section, key, fallback=None):
        return self.config.get(section, key, fallback=fallback)

    def getint(self, section, key, fallback=None):
        return self.config.getint(section, key, fallback=fallback)

    def getboolean(self, section, key, fallback=None):
        return self.config.getboolean(section, key, fallback=fallback)

    def getfloat(self, section, key, fallback=None):
        return self.config.getfloat(section, key, fallback=fallback)


def setup_logging(config):
    log_file = os.path.join(config.logs_directory, 'mysqlclusterbackup.log')
    
    # Ensure the logs directory exists
    os.makedirs(config.logs_directory, exist_ok=True)

    # Set up the root logger
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    # Create a rotating file handler
    file_handler = RotatingFileHandler(
        log_file,
        maxBytes=10 * 1024 * 1024,  # 10 MB
        backupCount=5
    )
    file_handler.setLevel(logging.INFO)

    # Create a console handler
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)

    # Create a formatter and add it to the handlers
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    file_handler.setFormatter(formatter)
    console_handler.setFormatter(formatter)

    # Add the handlers to the logger
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

    return logger


def parse_arguments():
    parser = argparse.ArgumentParser(description="MySQL Cluster Backup and Restore Tool")
    
    # Create a mutually exclusive group for the main operations
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument('-b', '--backup', action='store_true', help='Perform a full backup')
    group.add_argument('-i', '--incremental', action='store_true', help='Perform an incremental backup')
    group.add_argument('-p', '--prepare', action='store_true', help='Prepare a backup for restoration')
    group.add_argument('-r', '--restore', action='store_true', help='Restore from a backup')
    group.add_argument('-t', '--rotate', action='store_true', help='Rotate backups')

    # Add optional arguments
    parser.add_argument('-c', '--config', default='mysqlclusterbackup.cfg', help='Path to the configuration file')
    parser.add_argument('-d', '--date', default='mysqlclusterbackup.cfg', help='Date to prepare / restore a backup from')
    parser.add_argument('-v', '--verbose', action='store_true', help='Increase output verbosity')

    return parser.parse_args()


def get_most_recent_backup(root_backup_path):
    """
    Return the most recent backup directory in the
    given backup path.

    :param root_backup_path:
    :return:
    """
    most_recent = (None, None)

    for item in os.listdir(root_backup_path):
        full_path = os.path.join(root_backup_path, item)
        if os.path.isdir(full_path):
            try:
                # Attempt to parse the directory name as a date
                date_obj = datetime.strptime(item, "%Y-%m-%d")
                if most_recent[1] is None or date_obj > most_recent[1]:
                    most_recent = (full_path, date_obj)
            except ValueError:
                # If the directory name doesn't match the expected format, skip it
                continue

    return most_recent


def verify_backup_date(backup_date, root_backup_path):
    """
    verify_backup_date(str: backup_date)
 
    Verify the date format and existance of the backup.
   
    :param: backup_date:  str:  format: "YYYY-MM-DD"
    """
    try:
        date_obj = datetime.strptime(backup_date, "%Y-%m-%d")
    except:
        logger.error(f"Invalid date format provided: {backup_date}")    
        logger.error("Expected format: YYYY-MM-DD")
        sys.exit()
    
    path = os.path.join(root_backup_path, backup_date)
    if os.path.isdir(path):
        return True
    else:
        logger.error(f"A backup does not exist at the location provided: {path}")
        sys.exit(1)
    

def find_incrementals(directory_path):
    """
    find_incrementals(directory_path)

    Provided a path to a backup directory, this function will search
    the directory for incremental directories designated incr# with #
    being the numeric order that incrementals were created. 

    :param: directory_path - The path to a valid backup.

    :return: - Dict: response = {
                                 'base_backup': directory_path,
                                 'incrementals': ['/path/to/backup/incr1', 
                                                  '/path/to/backup/incr2',
                                                  '/path/to/backup/incr3']
                                }

    Notes: 
           * The incrementals will be in numeric order. (2 before 10)
           * If no incrementals exist, an empty list will be returned.
           * If the directory_path doesn't exist, None will be returned.
                   
    """
    response = {
        'base_backup': directory_path, 
        'incrementals': [] 
     }
   
    if not os.path.isdir(directory_path):
        return None
    
    subdirs = [d for d in os.listdir(directory_path) 
              if os.path.isdir(os.path.join(directory_path, d))]
    
    # Filter for incr# directories and extract numbers
    incr_dirs = []
    for subdir in subdirs:
        match = re.match(r'incr(\d+)$', subdir)
        if match:
            num = int(match.group(1))
    
            incr_dirs.append((num, subdir))

    # Sort  numericly for proper "prepare" of backup for restore
    incr_dirs.sort(key=lambda x: x[0])
    
    # Add sort-ordered incremental paths to response.
    response['incrementals'] = [
        os.path.join(directory_path, dir_name) 
        for _, dir_name in incr_dirs
    ]
    
    return response


def find_next_incr_directory(backup_path):
    """
    Find what the next incr directory name should be in the given directory.
    
    Args:
        backup_path (str): Path to search in. Defaults to current directory.
        
    Returns:
        str: Name of the next incr directory (e.g., 'incr3' if incr1 and incr2 exist).
    """
    if not backup_path or not os.path.exists(backup_path):
        logger.critical(f"Error: find_next_incr_directory provided non-existing directory.")
        logger.critical(f"Backup path does not exist: {backup_path}")
        return None

    try:
        # Get all items in the directory
        items = os.listdir(backup_path)
        
        # Filter for directories only
        directories = [item for item in items 
                      if os.path.isdir(os.path.join(backup_path, item))]
        
        # Pattern to match 'incr' followed by one or more digits
        pattern = re.compile(r'^incr(\d+)$')
        
        incr_dirs = []
        
        for dir_name in directories:
            match = pattern.match(dir_name)
            if match:
                number = int(match.group(1))
                incr_dirs.append((dir_name, number))
        
        if not incr_dirs:
            return os.path.join(backup_path, "incr1")
        
        # Find the highest number and return the next one
        highest_number = max(incr_dirs, key=lambda x: x[1])[1]
        return os.path.join(backup_path, f"incr{highest_number + 1}")
        
    except FileNotFoundError:
        print(f"Directory '{backup_path}' not found.")
        return None

    except PermissionError:
        print(f"Permission denied to access '{backup_path}'.")
        return None

    except Exception as e:
        print(f"An error occurred: {e}")
        return None


def get_latest_backup(config):
    """
    get_latest_backup(config)

    Returns a dictionary with details of the most recent
    backups, including any incrementals that exist.

    :param:  config - The configuration of this script.

    :return: dict: Response with backup details.

    Example: response = {'last_backup_today': bool,
                         'last_backup_incr': bool,
                         'next_backup_loc': str} 

    Note: next_backup_loc is a path to the next incremental
          backup if 'last_backup_incr' is True. Other wise,
          it's the next full backup location.
    """
    response = {'last_backup_today': False,
                'last_backup_incr': False,
                'next_backup_loc': None,
                'base_backup_loc': None
               }     
    backup_path = config.root_backup_path

    if os.path.exists(backup_path):
        logger.debug(f"Backup path found: {backup_path}")
        today = datetime.today().strftime('%Y-%m-%d')
        todays_backup = os.path.join(backup_path, today)

        if os.path.exists(todays_backup):
            response['last_backup_today'] = True
            response['base_backup_loc'] = todays_backup
            logger.debug(f"A backup for today was found: {todays_backup}")

            next_incremental = find_next_incr_directory(todays_backup)
            if next_incremental:
                response['next_backup_loc'] = next_incremental
                if next_incremental.endswith('incr1'):
                    response['last_backup_incr'] = False
                else:
                    response['last_backup_incr'] = True

                logger.debug(f"Next incremental location discovered: {next_incremental}")
        else:
            response['next_backup_loc'] = todays_backup
   
    return response    


def prepare_backup(backup_base):
    """
    prepare_backup(backup_base)

    Examines the provided backup directory and looks for
    incremental backups.   Once it has the incremental 
    directories, it will begin by preparing the base
    backup and then prepare the incremental backups 
    in order.

    :param: backup_base - The backup's base directory.

    :return: 
    """
    logger.info(f"Preparing backup for restore: {backup_base}")

    incrementals = find_incrementals(backup_base)
    found_incr = incrementals['incrementals']
    # Incrementals + base backup
    to_prepare = len(incrementals['incrementals']) + 1 

    logger.info(f"One base backup directory and {p.number_to_words(len(found_incr))} incrementals found.")
    logger.info(f"Base => {backup_base}")
 
    if len(incrementals == 0:
        # Only base backup, prepare it and quit.
        try:
            target_dir = f"--target-dir={backup_base}"
            result = subprocess.run(['xtrabackup', '--prepare' '--decompress', target_dir],
                                     capture_output=True, text=True, check=True) 
        except Exception as w:
            logger.error("Preparing the base directory failed.")
            logger.error(e)
            logger.error(e.stderr)
            sys.exit(1)

        return

    else:
        # Base and incrementals to prepare, so --apply-logs-only on base
        # TODO:  Add incremenntals to preapre below
        try:
            target_dir = f"--target-dir={backup_base}"
            result = subprocess.run(['xtrabackup', '--prepare' '--decompress', '--apply-logs-only', target_dir],
                                     capture_output=True, text=True, check=True) 
        except Exception as w:
            logger.error("Preparing the base directory failed.")
            logger.error(e)
            logger.error(e.stderr)
            sys.exit(1)

    count = 0 
    if len(incrementals) != 0:
        for incr in found_incr:
            count += 1
            if count == len(incrementals):
                logger.info(f"Incremental => {incr} - Last")
            else:
                logger.info(f"Incremental => {incr}") 


def perform_incremental_backup(full_backup_path, incremental_backup_path):
    """
    perform_incremental_backup(full_backup_path, incremental_backup_path)
    """
    logger.info(f"Incremental backup location: {incremental_backup_path}")
    base_backup = f"--incremental-basedir={full_backup_path}"
    incremental_target = f"--target-dir={incremental_backup_path}"
    try:
        result = subprocess.run(['xtrabackup', '--backup', '--compress', incremental_target, base_backup], 
                                  capture_output=True, text=True, check=True)
        
        logger.info(f"xtradb stdout: {result.stdout}")
        logger.info(f"xtradb stderr: {result.stderr}")

    except subprocess.CalledProcessError as e:
        logger.error("Incremental backup failed!")
        logger.error(e)
        logger.error(e.stderr)


def perform_backup(backup_path):
    """
    perform_backup(backup_path)

    """
    logger.info(f"Backup location: {backup_path}")
    target = f"--target-dir={backup_path}"
    try:
        result = subprocess.run(['xtrabackup', '--backup', '--compress', target], 
                                  capture_output=True, text=True, check=True)
        
        logger.debug(f"xtradb stdout: {result.stdout}")
        logger.debug(f"xtradb stderr: {result.stderr}")
        logger.info(f"Backup completed successfully.")

    except subprocess.CalledProcessError as e:
        logger.error("Backup failed!")
        logger.error(e)
        logger.error(e.stderr)

    return    

def main():
    global logger
    args = parse_arguments()
    
    try:
        # Load configuration first
        config = Config(args.config)
    except FileNotFoundError as e:
        print(f"Error: {e}")
        return
    except configparser.Error as e:
        print(f"Configuration error: {e}")
        return

    try:
        # Setup logging after configuration is loaded
        logger = setup_logging(config)

        if args.verbose:
            logger.setLevel(logging.DEBUG)
            logger.debug("Verbose mode enabled")

        if args.backup:
            logger.info("Performing full backup")
            # Add your backup logic here
            state = get_latest_backup(config)
            if state['last_backup_today']:
                logger.error(f"A full backup already exists for today: {state['base_backup_loc']}")
            perform_backup(state['next_backup_loc'])

        elif args.incremental:
            # Incremental backup logic 
            logger.info("Performing incremental backup")
            state = get_latest_backup(config)
            if state['last_backup_today'] and state['base_backup_loc']:
                logger.info(f"Incremental backup location: {state['next_backup_loc']}")
                logger.info(f"Base backup location: {state['base_backup_loc']}")
                perform_incremental_backup(state['base_backup_loc'], state['next_backup_loc'])
            else:
                logger.critical(f"Unable to preform incremental backup, there is no existing daily backup.")
                logger.critical(f"Expected daily backup location: {state['next_backup_loc']}")
      
        elif args.prepare:
            # Prepare backup
            if args.date:
                verify_backup_date(args.date, config.root_backup_path)
                prepare_backup(os.path.join(config.root_backup_path, args.date)) #datetime.strptime(args.date, '%Y-%m-%d')))
            else:
                logger.error("A date is required of the backup is required for preparing a backup.")

        elif args.restore:
            logger.info("Restoring from backup")
            # Add your restore logic here
        elif args.rotate:
            logger.info("Rotating backups")
            # Add your rotation logic here

    except Exception as e:
        print(f"An unexpected error occurred: {e}")


if __name__ == "__main__":
    main()
