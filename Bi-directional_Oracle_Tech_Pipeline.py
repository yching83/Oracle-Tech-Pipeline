# -*- coding: utf-8 -*-
"""
Created on Mon JUN 15 22:18:07 2020

@author: e64le52
"""

import pandas as pd
import numpy as np
import datetime
from datetime import date
import cx_Oracle;
from sqlalchemy import types, create_engine
import os
import smtplib, ssl
import datetime
import pyodbc
import pandas as pd
import time
from datetime import date

from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email.mime.text import MIMEText
from email.utils import formatdate
from email import encoders


class myoracleconnection(object):
    def __init__(self):
        self.user = 'DATABASE1'
        self.password = 'xxxxx'
        self.host = 'host_url'
        self.port = '1630'
        self.service_name = 'some_service'
    def connecthandler(self):
        self.con = cx_Oracle.connect(self.user, self.password, '{}:{}/{}'.format(self.host,self.port,self.service_name))
        return self.con
        #print("Oracle version: {}".format(self.con.version))
    def closeConn(self):
        self.dbConn.close()


class Logger():
    def __init__(self):
        datestamp = gettimestamp()
        # Setup up logging
        FORMAT = '%(asctime)-15s %(levelname)-8s %(name)-6s: %(message)s'
        logpath=os.path.join(os.path.split(sys.argv[0])[0], "uson_import_"+datestamp+".log")
        logging.basicConfig(filename=logpath, level=logging.DEBUG, format=FORMAT)
        self.consoleHandler = logging.StreamHandler()
        self.consoleHandler.setLevel(logging.DEBUG)
        self.consoleHandler.setFormatter(logging.Formatter(FORMAT))
        self.logger = logging.getLogger("uson-import")
        self.logger.addHandler(self.consoleHandler)

    def getConsoleHandler(self):
        return self.consoleHandler

    def logmsg(self, message, value, severity):
        if severity == 'info':
            self.logger.info(message + pformat(value))
        elif severity == 'error':
            self.logger.error(message + pformat(value))
        elif severity == 'debug':
            self.logger.debug(message + pformat(value))
        elif severity == 'infoonly':
            self.logger.info(message)
        elif severity == 'erroronly':
            self.logger.error(message)
        elif severity == 'debugonly':
            self.logger.debug(message)
        else:
            pass

       

class dbInteraction(myoracleconnection):
       
    def child(self):
        dataconns = (super(dbInteraction, self).connecthandler())
        self.dbConn = dataconns
        self.dbCursor = self.dbConn.cursor()
        self.query = ""
        self.df = ""
        self.errorcode = "" #cleaning before sending the query
        self.errormsg = ""
       
    def send_query(self, query):
        self.query = query
        self.errorcode = "" #cleaning before sending the query
        self.errormsg = ""
        try:
            print("Executing query...")
            self.dbCursor.execute(query)
        except cx_Oracle.DatabaseError as exc:
            error, = exc.args
            self.errorcode = error.code
            self.errormsg = error.message
            print("Error Oracle Code is: {}".format(self.errorcode))
            print("Error Oracle Message is: {}".format(self.errormsg))
   
    def send_querymany(self, stmt, values):
        self.query = stmt
        self.errorcode = ""
        self.errormsg = ""
        try:
            print("Executing many...")
            self.dbCursor.executemany(stmt,values)
        except cx_Oracle.DatabaseError as exc:
            error, = exc.args
            self.errorcode = error.code
            self.errormsg = error.message
            print("Error Oracle Code is: {}".format(self.errorcode))
            print("Error Oracle Message is: {}".format(self.errormsg))
       
    def fetch_all(self):
        if self.query == "" or "SELECT" not in self.query.upper():
            return -1
        else:
            print("Fetching results...")
            return self.dbCursor.fetchall() #fetchall is to get the contents from the query stored using the dbcursor
   
    def get_description(self):
        if self.query == "" or "SELECT" not in self.query.upper():
            return -1
        else:
            return self.dbCursor.description
   
    def gen_dataframe(self):
        if self.query == "" or "SELECT" not in self.query.upper():
            return -1
        else:
            desc = self.get_description()
            #print("desc:{}" .format(desc))
            col_names = [row[0] for row in desc]
            #print("col_names:{}" .format(col_names))
            data = self.fetch_all()
            #print("data:{}" .format(data))
            self.df = pd.DataFrame(data, columns = col_names)
            return 0

    def get_dataframe(self):
        return self.df
       
    def print_dataframe(self):
        print(self.get_dataframe())
       
    def do_commit(self):
        self.errorcode = ""
        self.errormsg = ""
        try:
            self.dbConn.commit()
        except cx_Oracle.DatabaseError as exc:
            error, = exc.args
            self.errorcode = error.code
            self.errormsg = error.message
            print("Error Oracle Code is: {}".format(self.errorcode))
            print("Error Oracle Message is: {}".format(self.errormsg))
       


class exl_dataframe(dict):
   
    def __init__(self,wbname,workshtname):
        wb = wbname
        wksheet = workshtname
        self.excel_data_df = pd.read_excel(wb, wksheet)
   
    def get_exceldf(self):
        return self.excel_data_df
   
    def get_rowcount(self):
        self.rowcnt = (len(self.excel_data_df.index))
        return self.rowcnt
   

class data_key(object):
     
    def __init__(self, dataframe, aList, keystring):
        self.df = dataframe
        self.key_structure = aList
        self.key_name = keystring


    def create_hash(self,row):
        myhash = ""
        for i, item in enumerate(row):
            # updating the value of the row
            myhash = myhash + str(item).strip()
            print("i: {}; item: {}".format(i,item))
        return myhash

    def create_method(self):
        self.df[self.key_name] = self.df[self.key_structure].apply(lambda row: self.create_hash(row), axis=1)
        return self.df

   

class merge_myframes(object):
   
    def __init__(self, dataframe1, dataframe2, key, path):
        #There are also other various types of joins https://towardsdatascience.com/why-and-how-to-use-merge-with-pandas-in-python-548600f7e738 similar to SQL
        #(left, right, inner, outer)
        #Similarly, we can also write as pd.merge(left_df, right_df, on='column_name', how='outer'
        #https://realpython.com/pandas-merge-join-and-concat/
        #Couple of Rules:
        ''' 1. merge() for combining data on common columns or indices
            2. .join() for combining data on a key column or an index
            3. concat() for combining DataFrames across rows or columns '''
        #Some info specifically with merge: https://jakevdp.github.io/PythonDataScienceHandbook/03.07-merge-and-join.html
       
        #self.final_df_inner = dataframe1.merge(dataframe2, left_on = key, right_on = key, how = 'inner')
        self.final_df_merge = pd.merge(dataframe1, dataframe2, on= key)
        self.path = path
   
    def send_myframe(self):
        print("Saving the final dataframe to csv in my path")
        self.final_df_merge.to_csv(self.path)

    def get_mynewframe(self):
        return self.final_df_merge
   

class create_query:
    def __init__(self, table, df):
        self.sql_empty = ""
        self.table = table
        self.df = df
        self.pdlist = (list(self.df.columns.values.tolist()))
        #We are not using the following. However in some instances where headers contain non alpha-numeric, we need to scrape and clean
        self.pdheaders = str(', '.join("\"" + str(e).upper() + "\"" for e in self.pdlist))

    def empty_table(self):
        self.sql_empty = "DELETE FROM " + self.table
       
    def get_emptytblstmt(self):
        return self.sql_empty

    def query_insert_table_batch_cmd(self):
        sqlmlineplacer = "(" + str(', '.join(":" + str(idx+1) for (idx, value) in enumerate(self.pdlist))) + ")"
        sql_text = 'INSERT INTO '+ self.table +' ('+ self.pdheaders + ') VALUES ' + sqlmlineplacer
        return sql_text

    def query_insert_table_batch_data(self):
        df_sub = self.df.loc[:,self.pdlist]
        print("df_sub:{}".format(df_sub))
        return df_sub.values.tolist()


class descriptive_Statistics(object):
   
    def __init__(self):
        self.df = ""
        self.key_structure = ""
        self.avgdf = ""
       
    def Diff_df(self,dataframe, ColA, ColB, Diff_column ):
        self.df[Diff_column] = df[ColB] - df[ColA]
        return self.df
   
    def Mean_df(self,dataframe, Key, aList):
        avgdf = (df.groupby(Key, as_index=True)[self.key_structure].mean())
        self.avgdf = avgdf
        return self.avgdf
   
class send_mail(object):
   
    def __init__(self, send_from, file_send_to, subject, emailbody, isTls=True):
     
         msg = MIMEMultipart()
         msg['From'] = send_from
         mailfile = open(file_send_to,"r+")
         lines = mailfile.readlines()
         send_to = [i.strip() for i in lines]
         print("Send to: {}" .format(send_to))
         msgto = "; ".join(i.strip() for i in lines)
         print("msgto: {}".format(msgto))
     
         msg['To'] = msgto
         msg['Date'] = formatdate(localtime = True)
         msg['Subject'] = subject
         #Email text or body
         with open(emailbody, 'r') as file:
             text = file.read()#.replace('\n', '')
         msg.attach(MIMEText(text))

         #filepath = 'C:\\Users\\e64le52\\Desktop\\TestFolder\\testscripts'
         directory = os.fsencode(filepath)

         for file in os.listdir(directory):
             filename = os.fsdecode(file)
             print("filename: {}" .format(filename))
             if filename.endswith(".csv"):
                 #print(filename)
                 varfilepath = (filepath+"\\"+filename)
                 print ("varfilepath: {}" .format(varfilepath))#or filename.endswith(".py"):
                 #print(os.path.join(directory, filename))
                 part = MIMEBase('application', "octet-stream")
                 part.set_payload(open(varfilepath, "rb").read())
                 encoders.encode_base64(part)
     
                 part.add_header('Content-Disposition', 'attachment', filename=filename)
                 msg.attach(part)
                 continue
             else:
                 continue

         print("msg: {}" .format(msg))
         smtp = smtplib.SMTP('mail.usoncology.com')
         if isTls:
             smtp.starttls()

         smtp.sendmail(send_from, send_to, msg.as_string())
         smtp.quit()

       

''' Call our classes from here '''
dbconn = myoracleconnection().connecthandler()
dbproc = dbInteraction()
dbproc.child()



''' This is to run the Oracle data frame'''
mysqlcmd = """SELECT PRAC_ID, PRAC_ABBR, PARENT_ID, PARENT_NAME, PAYER_NAME, PLAN_TYPE, CODE, PAYER_FEE, FEE_EFF_DATE
                   FROM ZYC_PAYERFEETEST  --WHERE PRAC_ID = 161 AND PAYER_FEE > 50000
                   WHERE PAYER_FEE > 50000
                   AND EXTRACT(month from FEE_EFF_DATE) = 5
                   AND EXTRACT(year from FEE_EFF_DATE) = 2020
                   ORDER BY FEE_EFF_DATE DESC"""
dbproc.send_query(mysqlcmd)
dbproc.gen_dataframe()
df_db = dbproc.get_dataframe()
print(df_db.head(5))

''' Excel: This is to run the excel data frame'''
   
myexcel = 'C:\\Users\\e64le52\\Desktop\\TestFolder\\testscripts\\testpayerfee.xlsx'
myworksheet = 'testwrksht'
exldf = exl_dataframe(myexcel,myworksheet)
df_exl = exldf.get_exceldf()
print(df_exl.head(5))
 

''' Excel: Try Adding Our Keys Here '''
list1 = ['PRAC_ID', 'CODE','PARENT_ID', 'PLAN_TYPE','FEE_EFF_DATE']
mykey = 'MixedKey'

keyframe = data_key(df_exl, list1, mykey)
newdf_exl = keyframe.create_method()
print(newdf_exl.head(5))



''' Oracle: Try Adding Our Keys Here '''
list1 = ['PRAC_ID', 'CODE','PARENT_ID', 'PLAN_TYPE','FEE_EFF_DATE']
mykey = 'MixedKey'

keyframe = data_key(df_db, list1, mykey)
newdf_db = keyframe.create_method()
print(newdf_db.head(5))
   
''' Create final dataframe and saving to path: Let's use our new dataframes for both oracle table and excel'''

savemyfinaldf = (r"C:\Users\e64le52\Desktop\TestFolder\testscripts\feefinaldf.csv")
#We want to include only the new column and our key to merge
framework = merge_myframes(newdf_db, newdf_exl[['UPDATED_PAYER_FEE','MixedKey']], mykey,savemyfinaldf )
framework.send_myframe()
mynewdf = framework.get_mynewframe()

''' Insert the final dataframe merged back into Oracle database'''

''' A. Clean the data out of the Oracle table first'''
importback = create_query('ZYC_combinedpayerfee', mynewdf)
importback.empty_table()
sql_toempty = importback.get_emptytblstmt()
print(sql_toempty)
dbproc.send_query(sql_toempty)
dbproc.do_commit()
print("Success...table has been truncated")

''' B. Insert the data into Oracle table '''

oraclesqlinsert = importback.query_insert_table_batch_cmd()
print("This is my sql insert statement: ", oraclesqlinsert)
datacontent = importback.query_insert_table_batch_data()
print("This is my data content: ", datacontent)
dbproc.send_querymany(oraclesqlinsert, datacontent)

dbproc.do_commit()

#dbproc.send_query(sql_toempty)


#dbproc.print_dataframe()
dbconn.close()
   
# Sending Email with Attachment
filepath = 'C:\\Users\\e64le52\\Desktop\\TestFolder\\testscripts'
emailfile = 'emailformat.txt'
emailusers = 'email-list.txt'
receivers_put = (filepath+'\\'+emailusers)
emailbody_put = (filepath+'\\'+emailfile)
sender = 'AutoUSON@mckesson.com'

timestr = date.today()
sendmsg = send_mail(sender, # send_from
          receivers_put,# send_to
           #"SECURE: Data Export - {}"
          "Testing123".format(timestr),
          emailbody_put
 )
