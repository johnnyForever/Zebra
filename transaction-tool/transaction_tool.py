#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# version 1.0
import os
from os import path
import sys
import logging
import time
import paramiko
import psycopg2
from psycopg2 import sql
from psycopg2 import OperationalError, Error, DatabaseError, InterfaceError

## CONFIGURATION
env = ''
username = 'username'

## DOCKER COMMANDS
check_job_completion = "docker logs --since 2m docker-compose_backend_1 | grep -i \'{0} status=FINISHED\'"
jobs = ['homebankingExportJob','transactionProcessJob']
job_exec = ['docker exec docker-compose_backend_1 curl -u management:secret -X GET http://localhost:',
       '8080/management/jolokia/exec/p2p-rest:module=core,category=job,name=JobRunner/runJob/', '/DEFAULT']

def logger_setup(log_file='run.log'):
    '''Examples of possible message processing scenarios:
    logger.debug('debug message'), logger.info('info message'),logger.warning('warn message'),
    logger.error('error message'),logger.critical('critical message')'''
    logger = logging.getLogger(env)
    if logger is None:
        print(f"{63 * '*'}\n*Something went wrong at the beginning of script execution*\n{63 * '*'}")
        quit()
    logger.setLevel(logging.DEBUG)
    fh = logging.FileHandler(log_file)
    fh.setLevel(logging.DEBUG)
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    fh.setFormatter(formatter)
    logger.addHandler(fh)

    return logger

def load_scripts(filepath, logger):
    '''Simply verify the existence of files and then load its contents. Returns individual strings/sql queries.'''
    full_path = os.path.dirname(os.path.realpath(sys.argv[0])) + '/' + filepath
    if path.exists(full_path):
        logger.debug(f'Scripts file found in location {full_path}')
    else:
        logger.error('Scripts file not found! Aborting ...')
        quit()
    with open(full_path, 'r') as file:
        loaded_file = file.read()
    logger.info(filepath.lstrip('/') +  f" script file loaded from:\npath: {full_path}\n")
    return loaded_file.split(';')

def db_setup(logger, port=, user='', passw='', db=''):
    ''' All parameters required to successfully set up a db connection are default arguments of this function.
    So they are not necessarily entered as parameters of the function call. The exception is the logger object,
    that is the prerequisites for successful execution of db_setup.'''
    try:
        conn = psycopg2.connect(dbname=db, user=user, host=env, password=passw, port=port)
        cur = conn.cursor()
    except UnboundLocalError as ue: logger.error(ue)
    except OperationalError as oe:
        logger.error(oe)
        db_close(cur, conn)
        return None
    else:
        logger.debug(f"Db connection with {env} successfully created")
    print(48 * '*' + f"\n\t  Db connection successfully\n\t  establisht with \n\t  HOST: {env}\n" + 48 * '*' + "\n\t  Version: 1.0" + "\n\t  Last revision: 23.02.2020\n" + 48 * '*')
    return cur, conn

def check_loan_to_process_first(pg_query, cur, logger, transa='transaction', trans_out='transaction_out'):
    '''This function only works with two sql returned from file "scripts.sql".
    In the first phase, it should execute the script on index 2 and then in the second phase one located on index 3.'''
    try:
        stdout = cur.execute(sql.SQL(pg_query).format(sql.Identifier(transa),sql.Identifier(trans_out)))
        stdout = cur.fetchall()
        logger.debug(f'Sql query executed \ttable name: {transa}, {trans_out}')
    except InterfaceError as ie:logger.error(ie)
    except AttributeError as ae:
        logger.error(ae)
        db_close(cur, conn)
        return None
    logger.info(f"Query executed\n{pg_query}\n")
    return stdout

def magic_inserts(pg_insert, cur, conn, logger):
    '''Simply inserts magic insert one and two. Return count of affected rows'''
    total_count = None
    try:
        cur.execute(pg_insert.format("transaction_in", "transaction_out", "transaction", "account"))
        conn.commit()
    except DatabaseError as de:
        logger.critical("Rollback in progress...\nInsert failed due to\n", de)
        conn.rollback()
        db_close(cur, conn)
    logger.info(f"{str(total_count)} transactions inserted successfully ")


def db_close(cur, conn):
    '''It should be called upon termination process and close all functional db connections'''
    cur.close()
    conn.close()
    print('\nDb connection fully terminated\n')
    quit()

def paramiko_setup(logger):
    '''Create a paramiko client to enable ssh interaction'''
    try:
        client = paramiko.SSHClient()
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        client.connect(hostname=env, username=username)
    except IOError as ioe:
        logger.error(ioe)
        client.close()
    return client

def job_run(cur , conn, client, logger, job, job_name, check_for_finish):
    '''Execute Job, two minutes countdown for job finish completion confirmation'''
    stdin, stdout, stderr = client.exec_command(job)
    if 0 == stdout.channel.recv_exit_status():
        time.sleep(20)
        counter = 0
        while True:
            time.sleep(10)
            counter += 1
            print(f"\nLooking for confirmation of {job_name} completion: {str(20 + (counter * 10))} sec ...")
            logger.info(f"Attempt to check the result of {job_name} run.\n Countdown iteration {counter}")
            stdin, stdout, stderr = client.exec_command(check_for_finish.format(job_name))
            print(f"{job_name} finished with status code {str(stdout.channel.recv_exit_status())} \n\t0 = executed successfully\n\t1 = executed without response or something like that\n\t127 etc. error)\n")
            if 0 == stdout.channel.recv_exit_status():
                logger.info(f"{job_name} finished with status code STDOUT: {stdout.channel.recv_exit_status()}\n")
                stdout = stdout.readlines()
                if len(stdout) > 0:
                    logger.info(f"{job_name} FINISHED")
                break
            elif counter < 10:
                continue
            else:
                logger.critical(f"Timeout for {job_name} end - time 2 min has expired\nENDING ...!")
                db_close(cur, conn)
                client.close()
    else:
        stderr.readlines()
        logger.error(f'An error occurred while trying to run {job_name}\nDue to following error:\t{stderr}')
        db_close(cur, conn)
        client.close()

    logger.info(f"Commands executed:\n{job}\n{check_for_finish.format(job_name)}")
    print(f"{job_name} job finshed with status COMPLETED\n")

def main():
    ''' SET-UP'''
    logger = logger_setup()
    cur, conn = db_setup(logger)
    logger.info(f"Paramiko onnecting to host {env} with loginname => {username}")
    client = paramiko_setup(logger)
    magic_insert_one, magic_insert_two, first_select, second_select, select_final = load_scripts('scripts.sql', logger)
    log_and_print = lambda item : [logger.debug(item), print(item)]
    log_and_print("\nAll connections are set. Starting to process ...")

    var_symbols = []
    loans_to_process = check_loan_to_process_first(first_select, cur, logger)
    print(f"\nCount {str(len(loans_to_process))} transactions to be processed\n")
    for co_no, status in loans_to_process:
        logger.info(f"Contract number {co_no} going to be processed.\tTransaction status: {status}")
        var_symbols.append(co_no)
    print(f"First sequesnce started ...\n{var_symbols}\n")

    '''First sequesnce - homebankingExportJob'''
    job_run(cur, conn, client, logger, f'{job_exec[0]}{job_exec[1]}{jobs[0]}{job_exec[2]}', jobs[0], check_job_completion)

    '''First sequesnce - MAGIC INSERT ONE'''
    magic_inserts(magic_insert_one ,cur , conn, logger)
    log_and_print(f"First sequence - Transactions inserted")
    logger.info(magic_insert_one)
    time.sleep(5)

    '''First sequesnce - transactionProcessJob'''
    job_run(cur, conn, client, logger, f'{job_exec[0]}{job_exec[1]}{jobs[1]}{job_exec[2]}',jobs[1], check_job_completion)
    print("The first sequences of transactions successfully executed.\nNow you need to invest your loan demand as an user in investor role.\nWhen you do so, confirm by entering 'y' co continue processing, 'n' to quit or ANY key to reload loan count.\n")

    while True:
        loans_to_process = check_loan_to_process_first(second_select, cur, logger)
        log_and_print(f"Following amount of loans could be processed {str(len(loans_to_process))}")
        ready = input("Enter: ")
        ready = ready.lower().rstrip()
        if ready in ('y', 'yes', 'ye'):
            for co_no, status in loans_to_process:
                logger.info(f"Contract number {co_no} going to be processed.\tTransaction status: {status}")
                print("\n")
            break
        elif ready in ('n' ,'no'):
            logger.debug("Session terminated by user")
            db_close(cur, conn)
            client.close()
            quit()
        else:
            print("\n")
            continue

    '''Second sequesnce - homebankingExportJob'''
    log_and_print(f"\n{'*' * 11} Second sequence started {'*' * 11}\n")
    job_run(cur, conn, client, logger, f'{job_exec[0]}{job_exec[1]}{jobs[0]}{job_exec[2]}',jobs[0], check_job_completion)

    '''Second sequesnce - MAGIC INSERT ONE'''
    one = magic_inserts(magic_insert_one ,cur , conn, logger)
    log_and_print(f"Second sequence - Transactions IN inserted")
    time.sleep(5)

    '''Second sequesnce - MAGIC INSERT TWO'''
    two = magic_inserts(magic_insert_two ,cur , conn, logger)
    log_and_print(f"\nSecond sequence - Transactions OUT inserted")
    logger.info(f"{two} \n{magic_insert_two}\n")
    time.sleep(5)

    '''Second sequesnce - transactionProcessJo - second round'''
    job_run(cur, conn, client, logger, f'{job_exec[0]}{job_exec[1]}{jobs[1]}{job_exec[2]}',jobs[1], check_job_completion)

    try:
        final_check = cur.execute(sql.SQL(second_select).format(sql.Identifier("transaction"),sql.Identifier("transaction_in"),sql.Identifier("transaction_out")))
        final_check = cur.fetchall()
        print(f'The final count of loans that have been processed in recent time: {str(len(final_check))}')
        if not final_check:
            logger.info("No loans for final check found")
        else:
            for index, item in enumerate(final_check):
                print(f"{str(index + 1)}. Contract number: {item[0]}\n Transaction status: {item[1]}\n transaction_out_id: {item[2]}\n transaction_in_id: {item[3]}\n")
            logger.info(f"Final check for contract number:\n{final_check}")
    except AttributeError as ae:logger.error(ae)
    except IndexError as ie:logger.error(ie)

    db_close(cur, conn)
    client.close()
    logger.close()

if __name__ == "__main__":
    main()
