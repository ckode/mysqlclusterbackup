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
import os
import logging
from datetime import datetime
from logging.handlers import RotatingFileHandler
import argparse

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
        self.backup_root_path = self.config.get('MYSQL_CLUSTER_BACKUP', 'BACKUP_ROOT_PATH')
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


def main():
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
        elif args.incremental:
            logger.info("Performing incremental backup")
            current_backup = get_most_recent_backup(config.xtrabackup_path)
            # Add your incremental backup logic here
        elif args.prepare:
            logger.info("Preparing backup for restoration")
            # Add your prepare logic here
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