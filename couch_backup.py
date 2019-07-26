#!/usr/bin/python3

import subprocess
import datetime
import json
import sys
import os
import time
import glob
from urllib import request, parse
from PyLogger import Logger
import config
import logging
date_now = datetime.datetime.now()
ROOT_BACKUP_DIR = config.ROOT_BACKUP_DIR
LOGS_FILE = config.LOGS_FILE
S3_BUCKET = config.S3_BUCKET
BACKUP_LOCKS_DIR = config.BACKUP_LOCKS

SLACK_WEBHOOK_URL = config.SLACK_WEBHOOK_URL
BACKUP_COMMAND = config.BACKUP_COMMAND
AWS_COMMAND = config.AWS_COMMAND
CHANNEL = config.CHANNEL

LOCK_VALIDITY = config.LOCK_VALIDITY

logging_helper = Logger("INFRA_CB_BACKUP")
logger = logging.getLogger("infra.INFRA_CB_BACKUP")

def run_shell_script(command, logger):
    p = subprocess.Popen(command, stdout=logger,stderr=logger, shell=True)
    p_status = p.wait()
    return p_status

def get_credentials():
        return {
        "username" : "",
        "password" : ""
        }

def post_on_slack(message):
        payload = {"channel": CHANNEL,
               'username': 'Alert',
               'text': message,
               'icon_emoji': ':bangbang:'}
        url = SLACK_WEBHOOK_URL
        req = request.Request(url=url, data=json.dumps(payload).encode("utf-8"))
        resp = request.urlopen(req)
        return resp

def get_aws_command():
        s3, directory = get_bucket_location()
        return AWS_COMMAND % (directory, s3)

def get_bucket_location():
        return S3_BUCKET, max(glob.glob(os.path.join(ROOT_BACKUP_DIR, '*')), key=os.path.getmtime)

def run_ls_on_s3():
        command = "aws s3 ls %s" % (max(glob.glob(os.path.join(ROOT_BACKUP_DIR, '*')), key=os.path.getmtime))

def remove_backup_dir():
        pass

def run_backup(backup_type, bucket = []):
        logger.info("Starting Couchbase backup")
        logger.info("Backup Type %s" % backup_type)
        logger.info("Backup buckets %s " % ("all" if len(bucket) ==0 else ",".join(bucket)))
        post_on_slack("Couchbase backup started : backup_type = %s and buckets = %s" % (backup_type,"all" if len(bucket) ==0 else ",".join(bucket)))
        if backup_type.lower() not in ["full", "diff", "accu"]:
                logger.error("Incorrect backup type")
                raise Exception("Incorrect backup type mentioned")

        credentials = get_credentials();
        full_backup = len(bucket) == 0
        logfile = None

        bucket_command = ""
        if not full_backup:
                bucket_command = "-b %s" % (",".join(bucket))
        print(bucket_command)
        print(BACKUP_COMMAND)
        print(credentials)
        command = BACKUP_COMMAND % (backup_type.lower(), ROOT_BACKUP_DIR, credentials["username"], credentials["password"], bucket_command)
        logger.info("Command is %s", command)
        status_cb_command = run_shell_script(command, logfile)
        if status_cb_command != 0:
                post_on_slack("An error occured for the backup process : %d" % (status_cb_command))
                logger.error("CB backup failed")
                raise Exception("backup process failed")

        post_on_slack("Backup completed successfully. Starting s3 sync.")
        aws_command = get_aws_command()
        logger.info("Starting AWS Sync : Command is %s" % aws_command)
        status_aws = run_shell_script(aws_command, logfile)
        if status_aws != 0:
                post_on_slack("An error occured for the aws sync process : %s %s" % (aws_command, str(status_aws)))
                logger.error("AWS Sync failed. Exiting.")
                raise Exception("AWS sync failed")
        post_on_slack("AWS sync completed successfully : %s" %  aws_command)
        logger.info("Completed successfully")

def check_lock(bucket):
        print(BACKUP_LOCKS_DIR + bucket)
        if os.path.exists(BACKUP_LOCKS_DIR + bucket):
                date_time_str = open(BACKUP_LOCKS_DIR + bucket, "r").read().strip()
                date_time_obj = datetime.datetime.strptime(date_time_str,"%Y-%m-%d %H:%M:%S.%f")
                date_time_now = datetime.datetime.now()
                date_diff = date_time_now - date_time_obj
                if date_diff.total_seconds()/3600 > LOCK_VALIDITY:
                        logger.info("Lock is invalid since %d hours have passed" % (LOCK_VALIDITY))
                        remove_lock(bucket)
                else:
                        raise Exception("A lock already exists. Skipping the backup now")

def create_lock(bucket):
        if os.path.exists(BACKUP_LOCKS_DIR +bucket):
                raise Exception("Cannot create lock. Something went wrong")
        f = open(BACKUP_LOCKS_DIR + bucket, "w")
        f.write(str(datetime.datetime.now()))
        f.close()
        logger.info("File lock for bucket %s created" % (bucket))

def remove_lock(bucket):
        os.remove(BACKUP_LOCKS_DIR + bucket)
        logger.info("Lock removed for bucket %s"% bucket)

if __name__ == "__main__":
        if len(sys.argv) != 3 and sys.argv[2].strip() == "":
                post_on_slack("Invalid parameters to script : %s" % ("|".join(sys.argv)))
        all_buckets = sys.argv[2].strip().split(",")
        for bucket in all_buckets:
                try:
                        check_lock(bucket)
                        create_lock(bucket)
                        run_backup(sys.argv[1], [bucket])
                        remove_lock(bucket)
                except Exception as e:
                        logger.error("Something went wrong while backing up %s : %s" % (bucket, str(e)))
                        post_on_slack("Bucket : %s, Error %s" % (bucket, str(e)))

