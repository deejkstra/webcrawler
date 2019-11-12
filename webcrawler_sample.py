#!/usr/bin/env python

import logging
logging.basicConfig(filename='webcrawler.log', level=logging.INFO)

import os
import requests
import sqlite3
from sqlite3 import Error
import datetime
import time
import threading
import schedule
import smtplib
from mailjet_rest import Client

# CONFIG
DB_NAME=os.environ.get('WEBCRAWLER_DB_NAME', 'prices.db')

PRICES_API=os.environ.get('WEBCRAWLER_PRICES_API', '')

MAILJET_API_KEY=os.environ.get('MAILJET_API_KEY', '')
MAILJET_API_SECRET=os.environ.get('MAILJET_API_SECRET', '')
MAILJET_FROM_EMAIL=os.environ.get('MAILJET_FROM_EMAIL', '')
MAILJET_TO_EMAIL=os.environ.get('MAILJET_TO_EMAIL', '')
MAILJET_NAME=os.environ.get('MAILJET_NAME', '')
MAILJET_SUBJECT=os.environ.get('MAILJET_SUBJECT', 'Price Alert!')
MAILJET_CUSTOM_ID=os.environ.get('MAILJET_CUSTOM_ID', 'PriceServiceApp')

# QUERIES
initialize_tables_query = '''
CREATE TABLE IF NOT EXISTS prices (
	id text PRIMARY KEY,
	type text,
	dynamicDisplayPrice double,
	basePrice double,
	timestamp timestamp);'''

select_query = '''SELECT id FROM prices'''

insert_query = '''
INSERT INTO prices(id, type, dynamicDisplayPrice, basePrice, timestamp)
VALUES (?,?,?,?,?)
'''

update_query = '''
UPDATE prices
SET dynamicDisplayPrice = ?, timestamp = ?
WHERE id = ? AND dynamicDisplayPrice != ? AND ? > timestamp
'''

# MESSAGE TEMPLATE

home_alert_message = '''
The price of home %s has changed from %s to %s, above the base price %s.
unsubscribe
'''

apartment_alert_message = '''
The price of apartment %s has changed from %s to %s, below the base price %s.
unsubscribe
'''

# database connection
db_connection=None

# smtp server connection
mailjet_server=None

# UUIDs of API data w/ timestamp of last DB operation
ids={}

# Thread pool
threads=[]

def get_connection(conn):
	if conn:
		return conn
	try:
		conn=sqlite3.connect(DB_NAME)
	except Error as e:
		logging.error(e)
	return conn

def close_connection(conn):
	if conn:
		conn.close()

def do_query(query):
	conn=get_connection(db_connection)
	with conn:
		c=conn.cursor()
		c.execute(query)

def do_update_query(data):
	conn=get_connection(db_connection)
	with conn:
		c=conn.cursor()
		c.execute(update_query, data)
		conn.commit()

def do_select_query(query):
	conn=get_connection(db_connection)
	with conn:
		c=conn.cursor()
		c.execute(query)
		return c.fetchall()

def do_insert_query(data):
	conn=get_connection(db_connection)
	with conn:
		c=conn.cursor()
		c.execute(insert_query, data)
		return c.lastrowid

def initialize_service():
	# init tables
	do_query(initialize_tables_query)

	# init id data
	select_data = do_select_query(select_query)
	for row in select_data:
		ids[row[0]]=None

# Get API data
def get_api_data(url=PRICES_API):
	response=requests.get(url=url)
	return response.json()

def get_mailjet_server(server):
	if server:
		return server
	
	if MAILJET_API_KEY and MAILJET_API_SECRET:
		server=Client(auth=(MAILJET_API_KEY, MAILJET_API_SECRET), version='v3.1')
	
	return server

def close_mailjet_server(server):
	if server:
		server.quit()

def get_mailjet_message(msg):
	return {
		'Messages':[{
			"From":{
				"Email":MAILJET_FROM_EMAIL,
				"Name":MAILJET_NAME
			},
			"To":[{
				"Email":MAILJET_TO_EMAIL,
				"Name":MAILJET_NAME
			}],
			"Subject":MAILJET_SUBJECT,
			"TextPart":msg,
			"CustomID":MAILJET_CUSTOM_ID
		}]
	}

def send_home_alert(row_id, new_price, base_price):
	server=get_mailjet_server(mailjet_server)

	old_price=ids[row_id]

	msg=home_alert_message%(row_id, old_price, new_price, base_price)
	logging.info(msg)

	if server:
		result=server.send.create(data=get_mailjet_message(msg))
		logging.info(result.json())

def send_apartment_alert(row_id, new_price, base_price):
	server=get_mailjet_server(mailjet_server)
	
	old_price=ids[row_id]

	msg=apartment_alert_message%(row_id, old_price, new_price, base_price)
	logging.info(msg)

	if server:
		result=server.send.create(data=get_mailjet_message(msg))
		logging.info(result.json())

def process_data():
	logging.info('Starting thread')

	raw_data = get_api_data()

	timestamp=datetime.datetime.now()

	for row in raw_data['properties']:
		row_id=row['id']
		row_type=row['type']
		dynamicDisplayPrice=row['dynamicDisplayPrice']
		basePrice=row['basePrice']

		# Email Notifications
		if row_type == 'home' and dynamicDisplayPrice > basePrice:
			if row_id in ids and ids[row_id] != dynamicDisplayPrice:
				send_home_alert(row_id, new_price=dynamicDisplayPrice, base_price=basePrice)
		elif row_type == 'apartment' and dynamicDisplayPrice < basePrice:
			if row_id in ids and ids[row_id] != dynamicDisplayPrice:
				send_apartment_alert(row_id, new_price=dynamicDisplayPrice, base_price=basePrice)

		# build update/insert queries
		if row_id in ids: # update
			if ids[row_id] != dynamicDisplayPrice:
				ids[row_id] = dynamicDisplayPrice
			do_update_query((dynamicDisplayPrice, timestamp, row_id, dynamicDisplayPrice, timestamp))
		else: # insert
			ids[row_id]=dynamicDisplayPrice
			do_insert_query((row_id, row_type, dynamicDisplayPrice, basePrice, timestamp))

	logging.info('Closing thread')

def job():
	x = threading.Thread(target=process_data)
	x.start()

def handle_cleanup():
	logging.info(do_select_query('select * from prices'))
	close_connection(db_connection)
	close_mailjet_server(mailjet_server)
	logging.info('Ending Pricing Service')

def main():
	try:
		logging.info('Starting Pricing Service')
		initialize_service()
		while True:
			schedule.run_pending()
	except KeyboardInterrupt:
		logging.error('Service Interrupted')
	finally:
		handle_cleanup()

# poll the API every 5 seconds
schedule.every(5).seconds.do(job)

main()

