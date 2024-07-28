#!/usr/bin/python3
import sys
import time
import boto3
import logging
import datetime
import threading
from os import path
from tqdm import tqdm
from botocore.exceptions import ClientError, ParamValidationError
ROLE_ARN = ""
​
def setup_logger(logfile):
    logger = logging.getLogger('s3_restore')
    logger.setLevel(logging.DEBUG)
    formatter = logging.Formatter('%(message)s')
    fh = logging.FileHandler(logfile)
    logger.addHandler(fh)
​
    return logger
​
def read_file(fname):
    lines = []
    with open(fname) as f:
        for i,l in enumerate(f):
            lines.append(l.replace('\n', ''))
​
    return lines
​
def chunks(lst, n):
    """Yield successive n-sized chunks from lst."""
    for i in range(0, len(lst), n):
        yield lst[i:i + n]
​
def diff(first, second):
    second = set(second)
    return [item for item in first if item not in second]
​
def request_retrieval(s3_client, logger, files, bucket_name, retain_days, tier, chunk_index):
    bar = tqdm(total=len(files), position=chunk_index)
    for f in files:
        try:
            response = s3_client.restore_object(
                Bucket=bucket_name,
                Key=f,
                RestoreRequest={
                    'Days': retain_days,
                    'GlacierJobParameters': {
                        'Tier': tier
                    }
                }
            )
            logger.info(f)
            if response['ResponseMetadata']['HTTPStatusCode'] == 200:
                bar.write(f"{f} already available to download")
            elif response['ResponseMetadata']['HTTPStatusCode'] == 202:
                bar.write(f"{f} request ok")
        except ClientError as e:
            code = e.response['Error']['Code']
            if code == 'NoSuchKey':
                bar.write(f"{f} not found, skipping")
            if code == 'RestoreAlreadyInProgress':
                bar.write(f"{f} restore already in progress, ignoring")
                logger.info(f)
​
        bar.update(1)
​
​
def main():
    input_bucket = input("Bucket name: ")
    logfile = f"bucket_{input_bucket}.log"
    logger = setup_logger(logfile)
​
    log = []
    if path.exists(logfile):
        log = read_file(logfile)
​
    filepath = 'list.txt'
    if len(sys.argv) > 1:
        filepath = sys.argv[1]
​
    input_retain_days = int(input("How many days you want to keep the restored files: "))
    input_tier = input("Retrieval tier (Standard, Bulk, Expedited): ")
    if input_tier not in ('Standard', 'Bulk', 'Expedited'):
        print(f"You typed {input_tier}, valid values are Standard, Bulk, Expedited")
        return -1
​
    input_threads = input("How many threads to run?\nGood value is cpu-cores*2: ")
    lines = read_file(filepath)
​
    print("\n")
​
    if len(log) > 0:
        prev_len = len(lines)
        lines = diff(lines, log)
        print(f"Log found. Skipping {prev_len - len(lines)} entries")
​
    print(f"Will have to process {len(lines)} files")
​
    split_by = max(int(len(lines) / int(input_threads)), 1)
​
    est_hours = len(lines)/int(input_threads)/5/60/60
    est_hours_format = str(datetime.timedelta(hours=est_hours))
    if input(f"This will take approximately { est_hours_format }hours\nContinue? (y/[n]): ") != "y":
        sys.exit(1)
​
    sts = boto3.client('sts')
    response = sts.assume_role(RoleArn=ROLE_ARN, RoleSessionName="s3_restore_script", DurationSeconds=3600)

    session = boto3.Session(aws_access_key_id=response['Credentials']['AccessKeyId'],
                      aws_secret_access_key=response['Credentials']['SecretAccessKey'],
                      aws_session_token=response['Credentials']['SessionToken'])

    s3 = session.client('s3')
    threads = []
    print(f"{input_threads} threads, {split_by} files per thread")

    timer_start = time.time()
    chunk_index = 0
    for chunk in chunks(lines, split_by):
        t = threading.Thread(target=request_retrieval, args=(s3, logger, chunk, input_bucket, input_retain_days, input_tier, chunk_index))
        t.start()
        threads.append(t)
        chunk_index += 1
​
    for t in threads:
        t.join()
​
    print(f"Execution took {time.time()-timer_start}")
​
​
if __name__ == "__main__":
    main()
