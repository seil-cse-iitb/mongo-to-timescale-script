import datetime
import json
import os
import os.path
import pymongo as pm
import smtplib
import sys
import time
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from mysql.connector import (connection)


# "power_k_lh_a",
# "power_test"
# "aravali_230",
# "aravali_231",
# "aravali_235",
# "aravali_236",
# "aravali_237",
# "aravali_238",
# "aravali_239",
# "aravali_240",
# "aravali_241",
# "aravali_242",
# "aravali_243",
# "aravali_244",
# "aravali_245",
# "aravali_246",
# "aravali_247",
# "aravali_8",
# "aravali_meter11",
# "aravali_meter19",
# "test_ee",
# "test_seil_rish"


def report_error(toemail, errorsubject, errormsg):
    fromaddr = "seil@cse.iitb.ac.in"
    toaddr = toemail
    msg = MIMEMultipart()
    msg['From'] = fromaddr
    msg['To'] = toaddr
    msg['Subject'] = errorsubject

    body = errormsg
    msg.attach(MIMEText(body, 'plain'))

    server = smtplib.SMTP('imap.cse.iitb.ac.in', 25)
    server.starttls()
    server.login(fromaddr, "seilers")
    text = msg.as_string()
    server.sendmail(fromaddr, toaddr, text)
    server.quit()


def save_log(string):
    log = open('log', 'a')
    log.write(str(string))
    log.close()


def str_from_timestamp(timestamp):
    return datetime.datetime.fromtimestamp(timestamp).strftime("%d/%m/%Y %H:%M:%S")


def timestamp_from_str(str):
    return time.mktime(datetime.datetime.strptime(str,
                                                  "%d/%m/%Y %H:%M:%S").timetuple())


def connect_mongo():
    db_mo = pm.MongoClient(mongo_host, 27017)
    con = db_mo['data']  # new database
    return db_mo, con


def update_transfered_records_log(table_name, str_backuped_till, records_copied, mysql_table_name):
    dict = {table_name: str_backuped_till}
    if os.path.exists(transfered_records_log):
        file = open(transfered_records_log, 'r')
        dict = json.load(file)
        file.close()
        file = open(transfered_records_log, 'w')
        dict[table_name] = str_backuped_till
        json.dump(dict, file)
    else:
        file = open(transfered_records_log, 'w')
        json.dump(dict, file)
    file.flush()
    file.close()
    print("Records till: (" + table_name + ")=>" + str_backuped_till + " inserted, no. of records: " + str(
        records_copied) + " to mysql table: " + mysql_table_name)
    save_log("Records till: (" + table_name + ")=>" + str_backuped_till + " inserted, no. of records: " + str(
        records_copied) + " to mysql table: " + mysql_table_name + "\n")


def fetch_from(table_name):
    if os.path.exists(transfered_records_log):
        file = open(transfered_records_log, 'r')
        dictionary = json.load(file)
        if table_name in dictionary.keys():
            return timestamp_from_str(dictionary[table_name])
    return ts_backup_from


def fetch_till(ts_fetch_from):
    return ts_fetch_from + records_batch_size

# -----------------------------------------------------------
config_fp = open("config.json", 'r')
config = json.load(config_fp)
records_batch_size = config['records_batch_size']
mongo_host = config['mongo_host']
mysql_user = config['mysql_user']
mysql_pass = config['mysql_pass']
mysql_host = config['mysql_host']
mysql_db_name = config['mysql_db_name']
mysql_tables = config['mysql_tables']
backup_from = config['backup_from']
backup_till = config['backup_till']
schema = config['schema']
transfered_records_log = config["transfered_records_log"]
ts_backup_from = timestamp_from_str(backup_from)
ts_backup_till = timestamp_from_str(backup_till)
channelwise_tables = config['channelwise_tables']
# channelwise_tables = {"1": ['aravali_236']}
# -----------------------------------------------------------

save_log("-------------------------------------------------------")
save_log("script started at "+str_from_timestamp(time.time())+"\n")
try:
    mongo_db, mongo_con = connect_mongo()  # connection to mongo db
except:
    print("Unexpected error In Mongo Connection:", sys.exc_info()[0])
    tFile = str(time.strftime("%d-%m-%Y"))
    tWrite = time.strftime("%H:%M:%S", time.localtime(time.time()))
    with open("./mongotomysqlscripterrror/error_" + tFile, "a+") as errF:
        errF.write(str(tWrite) + "\n" + str(sys.exc_info()[0]))

mysql_con = connection.MySQLConnection(user=mysql_user, password=mysql_pass, host=mysql_host, db=mysql_db_name)
cursor = mysql_con.cursor()
try:
    for channel in channelwise_tables:
        tables = channelwise_tables[channel]
        for table_name in tables:
            ts_fetch_from = ts_backup_from
            ts_fetch_till = ts_backup_from
            mysql_con.rollback()
            while ts_fetch_till < ts_backup_till:
                select_cols = schema[channel]
                ts_fetch_from = fetch_from(table_name)
                ts_fetch_till = fetch_till(ts_fetch_from)
                if ts_fetch_till > ts_backup_till:
                    ts_fetch_till = ts_backup_till
                mongo_query = {"$query": {"TS": {"$gte": ts_fetch_from, "$lt": ts_fetch_till}}, "$orderby": {"TS": 1}}
                print("------------------------------------------")
                print("coping from: " + channel + "-" + table_name + " to " + mysql_tables[channel])
                rows = mongo_con[table_name].find(mongo_query, select_cols)
                records_to_be_copied = 0
                col_str = ""
                for col in schema[channel]:
                    col_str += "`" + col + "`,"
                col_str += "`sensor_id`"
                blank_mysql_query = "insert into `" + str(mysql_tables[channel]) + "` (" + col_str + ") values "
                filled_mysql_query = blank_mysql_query
                print("started building query!")
                for row in rows:
                    value_str = ""
                    for col in schema[channel]:
                        if row[col] is None:
                            value = "NULL"
                        elif float(row[col]) == float('inf'):
                            value = "'-1'"
                        else:
                            value = "'" + str(row[col]) + "'"
                        value_str += " " + str(value) + ","
                    value_str += "'" + table_name + "'"
                    filled_mysql_query += " (" + value_str + "),"
                    records_to_be_copied += 1
                print("ended building query! no. of records: " + str(records_to_be_copied))
                filled_mysql_query = filled_mysql_query[:-1]
                print("removed comma from last!")
                if records_to_be_copied > 0:
                    try:
                        cursor.execute(filled_mysql_query)
                        # print(filled_mysql_query)
                    except Exception as e:
                        records_to_be_copied = 0
                        print("exception during query execution: ", e)
                        save_log("collection: " + table_name + " " + str(e) + "\n")
                        mysql_con.rollback()
                        break
                    print("executed query!")
                    mysql_con.commit()
                    print("commited query!")
                update_transfered_records_log(table_name, str_from_timestamp(ts_fetch_till), records_to_be_copied,
                                              mysql_tables[channel])
                print("no. of records copied: " + str(records_to_be_copied) + " into mysql table: " + str(
                    mysql_tables[channel]))

except Exception as e:
    print("outer exception: ", e)
    save_log(str(e) + "\n")
    report_error("sapantanted99@gmail.com", "mongo to mysql script stopped", "outer exception: " + str(e) + "\n")
finally:
    mysql_con.close()

save_log("script ended at "+str_from_timestamp(time.time())+"\n")
