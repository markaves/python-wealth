## Title: AMP Super Loader
## Description:  This script reads the csv file from AMP website and loads it in to the database
## Created by: Mark Neil C. Aves
## Date: 27-03-2020

## SQL Table Creation:

#CREATE TABLE `amp_super` (
#  `transaction_id` int(11) NOT NULL,
#  `date` date NOT NULL,
#  `description` varchar(30) NOT NULL,
#  `source` varchar(30) NOT NULL,
#  `amount` decimal(10,2) NOT NULL
#) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

import csv
import datetime
import json
import re
import datetime
import mysql.connector

cnx = mysql.connector.connect(
    host="localhost",
    user="python",
    passwd="password",
    database="wealth"
)

cursor = cnx.cursor()

add_super = ("INSERT INTO amp_super"
              "(transaction_id, date, description, source, amount)"
              "VALUES (NULL, %(date)s, %(description)s, %(source)s, %(val)s)")

data=[]
with open('archive/SearchResult(2).csv') as csv_file:
    csv_reader = csv.reader(csv_file, delimiter=',')
    count=0
    for row in csv_reader:
        count=count+1
        data.append(row)

    for i in range(0,count):
        cnt=count-i-1
        date = data[cnt][0]
        desc = data[cnt][1]
        source = data[cnt][2]
        debit_tmp = data[cnt][4].replace('$', '')
        credit_tmp = data[cnt][3].replace('$', '')

        if debit_tmp:
            val=debit_tmp.replace(',', '')
        if credit_tmp:
            val=credit_tmp.replace(',', '')

        if date:
            if date != "Date":

                data_super = {
                  'date': datetime.datetime.strptime(date, '%d %b %Y').strftime('%Y-%m-%d'),
                  'description': desc,
                  'source': source,
                  'val': val
                }
                #cursor.execute(query_super, data_super)
                #print(data_super)
                sql = ("SELECT * FROM amp_super WHERE date = '%s' and description = '%s' and source = '%s' and amount ='%s'"
                        % (datetime.datetime.strptime(date, '%d %b %Y').strftime('%Y-%m-%d'), desc, source, val))

                cursor.execute(sql)
                myresult = cursor.fetchall()
                if not myresult:
                        print(data_super)
                        cursor.execute(add_super, data_super)
                        cnx.commit()
