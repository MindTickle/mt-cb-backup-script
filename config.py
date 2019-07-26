import datetime
import time
date_now = datetime.datetime.now()

ROOT_BACKUP_DIR = "/backups/data/new_data/"
BACKUP_LOCKS = "/backups/locks/"
LOGS_FILE = "/backups/logs/%d.%02d.%02d/%d" % (date_now.year, date_now.month, date_now.day, int(time.time()))
S3_BUCKET = ""


SLACK_WEBHOOK_URL = ''
BACKUP_COMMAND = "/opt/couchbase/bin/cbbackup -m %s http://localhost:8091 %s -u %s -p %s %s --verbose"
AWS_COMMAND = 'aws s3 sync "%s" "%s"'

CHANNEL = "cb_backup_monitoring"

KEEP_BACKUP_FOR = 7
LOCK_VALIDITY = 4

