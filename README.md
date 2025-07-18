# MySQL Galera (XtraDB) Cluster Backup Management Script

This script automates the backup and restore processes for MySQL Galera (XtraDB) Clusters using Percona's XtraBackup. It provides functionality for full backups, incremental backups, backup preparation, restoration, and backup rotation.

# NOTE:  This is a Work in Progress and is not yet usable.

## Features

- Full backups
- Incremental backups
- Backup preparation for restoration
- Backup restoration
- Backup rotation
- Configurable settings via a configuration file
- Logging with rotation

## Requirements

- Python 3.13 or higher
- Percona XtraBackup
- MySQL Galera Cluster or Percona XtraDB Cluster (I've only tested XtraDB)
- Python's UV Package Manager
## Installation

1. Clone this repository or download the `mysqlclusterbackup.py` script.
2. Install Percona XtraBackup if not already installed.
3. Ensure Python's UV Package Manager is installed.

## Notes:

While this script uses uv, you still run it like a cli application 
without the 'uv run' command.

## Configuration

Create a configuration file named `mysqlclusterbackup.cfg` in the same directory as the script, or specify a custom path when running the script. The configuration file should have the following structure:

```ini
[MYSQL_CLUSTER_BACKUP]
MYSQL_DATA: /data/mysql
BACKUP_ROOT_PATH: /data/backup
NOTIFICATION_EMAIL: your.email@example.com
NOTIFICATION_FROM: backup.system@example.com
SMTP_SERVER: smtp.example.com
XTRABACKUP_PATH: /usr/bin/xtrabackup
LOGS_DIRECTORY: /data/mysqlclusterbackup/logs

[MYSQL_CLUSTER_BACKUP_ROTATION]
BEGINNING_OF_WEEK: 0
YEARLY_BACKUP_DATE: 1
WEEKLY_BACKUP_COUNT: 4
MONTHLY_BACKUP_COUNT: 6
YEARLY_BACKUP_COUNT: 1
```
