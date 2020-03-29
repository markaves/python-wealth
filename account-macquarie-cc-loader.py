## Title: MAcquarie Credit Card Loader
## Description:  This script reads the csv credit card file from the Macquarie website and loads it in to the database
## Created by: Mark Neil C. Aves
## Date: 27-03-2020

## SQL Table Creation:

#CREATE TABLE `transactions` (
#  `transaction_id` int(11) NOT NULL,
#  `date` date NOT NULL,
#  `details` varchar(30) NOT NULL,
#  `account` varchar(30) NOT NULL,
#  `category` varchar(30) NOT NULL,
#  `notes` varchar(50) NOT NULL,
#  `debit` float NOT NULL,
#  `credit` float NOT NULL,
#  `description` varchar(50) NOT NULL
#) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

import csv
import datetime
import json
import re
import time
import mysql.connector

cnx = mysql.connector.connect(
    host="localhost",
    user="python",
    passwd="password",
    database="wealth"
)

cursor = cnx.cursor()

add_entry = ("INSERT INTO transactions"
              "(transaction_id, date, details, account, category, subcategory, notes, debit, credit, description)"
              "VALUES (NULL, %(date)s, %(details)s, %(account)s, %(category)s, %(subcategory)s, %(notes)s, %(debit)s, %(credit)s, %(description)s)")

data=[]
with open('archive/transactions(1).csv') as csv_file:
    csv_reader = csv.reader(csv_file, delimiter=',')
    count=0
    for row in csv_reader:
        count=count+1
        data.append(row)

    for i in range(0,count):
        #time.sleep(1)
        cnt=count-i-1

        date = data[cnt][0]
        details = data[cnt][1]
        account = data[cnt][2]
        category = data[cnt][3]
        subcategory = data[cnt][4]
        notes = data[cnt][5]
        debit = data[cnt][6]
        credit = data[cnt][7]
        description = data[cnt][8]

        if date != "Transaction Date":
            data_entry = {
              'date': datetime.datetime.strptime(date, '%d %b %Y').strftime('%Y-%m-%d'),
              'details': details,
              'account': account,
              'category': category,
              'subcategory': subcategory,
              'notes': notes,
              'debit': debit,
              'credit': credit,
              'description': description

            }
            #cursor.execute(query_super, data_super)
            sql = ("SELECT * FROM amp_super WHERE date = '%s' and details = '%s' and account = '%s'"
                    % (datetime.datetime.strptime(date, '%d %b %Y').strftime('%Y-%m-%d'), details, account))

            #print(sql)

#            cursor.execute(sql)
#            myresult = cursor.fetchall()
#            if not myresult:
#                    #print("mark")
            print(data_entry)
            cursor.execute(add_entry, data_entry)
            cnx.commit()

            #for x in myresult:
            #    print(x)
            #    print("mar")
