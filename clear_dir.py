import shutil
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
delete = True
date_now = datetime.datetime.now()
ROOT_BACKUP_DIR = config.ROOT_BACKUP_DIR
LOGS_FILE = config.LOGS_FILE
S3_BUCKET = config.S3_BUCKET


SLACK_WEBHOOK_URL = config.SLACK_WEBHOOK_URL
BACKUP_COMMAND = config.BACKUP_COMMAND
AWS_COMMAND = config.AWS_COMMAND
CHANNEL = config.CHANNEL

KEEP_BACKUP_FOR = config.KEEP_BACKUP_FOR
logging_helper = Logger("INFRA_CB_BACKUP")
logger = logging.getLogger("infra.INFRA_CB_BACKUP")

def run_shell_script(command, logger):
    p = subprocess.Popen(command, stdout=subprocess.PIPE,stderr=subprocess.PIPE, shell=True)
    (output, error) = p.communicate()
    p_status = p.wait()
    logger.info(output)
    logger.error(error)
    return output, error, p_status

def post_on_slack(message):
        payload = {"channel": CHANNEL,
               'username': 'Alert',
               'text': message,
               'icon_emoji': ':bangbang:'}
        url = SLACK_WEBHOOK_URL
        req = request.Request(url=url, data=json.dumps(payload).encode("utf-8"))
        resp = request.urlopen(req)
        return resp

def get_aws_files_count(path):
        aws_command = "aws s3 ls %s --recursive | wc -l" % (path)
        print(aws_command)
        op, error, status = run_shell_script(aws_command, logger)
        if(status!=0):
                post_on_slack("Something went wrong fetching the file count from AWS")
                raise Exception("Cannot get files count from AWS")
        return op

def get_local_machine_files_count(path):
        ls_command = 'ls -lh %s --recursive | grep "json\|cbb" | wc -l' % (path)
        print(ls_command)
        op, error, status = run_shell_script(ls_command, logger)
        if(status!=0):
                post_on_slack("Something went wrong counting the files on local")
                raise Exception("Cannot get files count on local machine")
        return op

deleted = []
count_mismatch = []

def delete_script():
        """Checking for n days prior. will run it only once a day"""
        date_yesterday = date_now - datetime.timedelta(days=KEEP_BACKUP_FOR)
        u = (sorted(glob.glob(os.path.join(ROOT_BACKUP_DIR, '%d-%02d-%02d*/*'%(date_yesterday.year, date_yesterday.month, date_yesterday.day))), key=os.path.getmtime))
        issue = False
        print(u, os.path.join(ROOT_BACKUP_DIR, '%d-%02d-%02d*/*'%(date_now.year, date_now.month, date_now.day)))
        for each in u:
                print(each)
                folder_id = each.strip().split("/")[-1]
                aws_count = get_aws_files_count(S3_BUCKET + folder_id)
                local_count = get_local_machine_files_count("/".join(each.strip().split("/")[:-1]) + "/" + folder_id)
                if aws_count != local_count:
                        logger.info("The count for documents does not match for folder %s. Not deleting the folder" % (folder_id))
                        count_mismatch.append(folder_id)
                        issue = True
                else:
                        logger.info("The count for documents does matches for folder %s. Deleting the folder" % (folder_id))
                        deleted.append(folder_id)
                        #delete and shutil.rmtree("/".join(each.strip().split("/")[:-1]) + "/" + folder_id)
        if issue:
                post_on_slack("Directory not deleted because of count mismatch")
        elif not issue and len(u)!=0:
                shutil.rmtree("/".join(u[0].split("/")[:-1]))
                post_on_slack("Directories deleted %s " % (",".join(deleted)))
                post_on_slack("Directories with doc count mismatch %s " % (",".join(count_mismatch)))



if __name__ == "__main__":
        post_on_slack("Running the script to delete already backed up directories")
        try:
                delete_script()
        except Exception as e:
                print(e)
                post_on_slack("An error occured : %s" % str(e))



