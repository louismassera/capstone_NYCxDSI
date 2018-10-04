#####
# helper functions
#####

import os
import json
import pandas as pd
import datetime

#not in use
def openfile(fn):
	with open(fn, 'r') as data:
		js = json.load(data)
	return js

#not in use
def json_to_dataframe(json_data, entity):
	try:
		return pd.read_json(json_data[entity])
	except AttributeError:
		pass
		#df = pd.from_dict(json_data[entity])
	try:
		return pd.DataFrame(json_data[entity]) 
	except:
		raise AttributeError('error')
	#return df

def split_city(city_df, city_col):
	spl = city_df[city_col].str.split(',', n=1, expand=True)
	city_df['boro'] = spl[0]
	city_df['state'] = spl[1]
	return city_df

def replace_city_col(df):
	df = split_city(df,'city')
	df['city'] = df['boro']
	df.drop(['boro'], axis=1, inplace=True)
	return df

def write_to_csv(data, fn):
	return data.to_csv(fn, index=None)

def millis_to_utc(row, column):
    ms = row[column]
    return datetime.datetime.fromtimestamp(float(ms)/1000.0)

def iso_utc_millis_col(col_name,new_col_name, df):
	df[new_col_name] = df.apply(lambda x: millis_to_utc(x, col_name),axis=1)
	df[new_col_name] = df[new_col_name].apply(lambda x: x.isoformat())
	return df

def reorder_cols(cols_order, df):
	return df[cols_order]

def transform_alerts(data):
	#alerts = json_to_dataframe(data, "alerts")
	alerts = replace_city_col(data)
	alerts = iso_utc_millis_col('pubMillis','pub_utc_date',alerts)
	alerts['reportDescription'] = alerts['reportDescription'].str.replace('\r\n',' ')
	order_als = ['uuid','city','state','country','confidence','location','magvar',
					'nThumbsUp','reliability','reportDescription','reportRating',
					'roadType','street','subtype','type','pubMillis','pub_utc_date']

	alerts = reorder_cols(order_als, alerts)
	
	alerts.rename(
	    columns={
	        'pubMillis': 'pub_millis',
	        'roadType': 'road_type',
	        'reportDescription': 'report_description',
	        'reportRating': 'report_rating',
	        'nThumbsUp': 'thumbs_up'
	    }, 
	    inplace=True
	)
	return alerts

def transform_jams(data):
	#jams = json_to_dataframe(data, JAMS)
	print('in jams')
	print()
	jams = replace_city_col(data)
	jams = iso_utc_millis_col('pubMillis','pub_utc_date',jams)
	
	#uuid and id are the same, keeping uuid
	order_jms = ['uuid','blockingAlertUuid', 'city', 'state', 'country', 'delay', 
			'endNode', 'length', 'level', 'line', 'roadType', 'street','segments', 
			'speed','speedKMH','turnType', 'type', 'pubMillis','pub_utc_date']

	jams = reorder_cols(order_jms, jams)

	jams.rename(
	    columns={
	        'pubMillis': 'pub_millis',
	        'roadType': 'road_type',
	        'blockingAlertUuid': 'blocking_alert_uuid',
	        'endNode': 'end_node',
	        'speedKMH': 'speed_KMH',
	        'turnType': 'turn_type'
	    }, 
	    inplace=True
	)
	return jams

def transform_irreg(data):
	#irreg = json_to_dataframe(data, IRREG)
	irreg = replace_city_col(data)
	irreg = iso_utc_millis_col('detectionDateMillis', 'detection_utc_date', irreg)
	irreg = iso_utc_millis_col('updateDateMillis','update_utc_date', irreg)
	
	order_irs = ['id','alerts', 'alertsCount', 'causeAlert', 'causeType', 'city', 'state','country',
       'delaySeconds', 'detectionDate', 'detectionDateMillis', 'detection_utc_date','driversCount',
       'endNode', 'highway',  'jamLevel', 'length', 'line', 'nComments',
       'nImages', 'nThumbsUp', 'regularSpeed', 'seconds', 'severity', 'speed',
       'street', 'trend', 'type', 'updateDate', 'updateDateMillis', 'update_utc_date',
       ]

	irreg = reorder_cols(order_irs, irreg)

	irreg.rename(
	    columns={
	        'alertsCount': 'alerts_count',
	        'causeAlert': 'cause_alert',
	        'causeType': 'cause_type',
	        'delaySeconds': 'delay_seconds',
	        'endNode': 'end_node',
	        'detectionDate': 'detection_date',
	        'detectionDateMillis': 'detection_date_millis',
	        'driversCount': 'drivers_count',
	        'jamLevel': 'jam_level',
	        'nComments': 'n_comments',
	        'nImages': 'n_images',
	        'nThumbsUp': 'n_thumbs_up',
	        'regularSpeed': 'regular_speed',
	        'updateDate': 'update_date', 
	        'updateDateMillis': 'update_date_millis',
	    }, 
	    inplace=True
	)
	return irreg

def transform_coord_location(df, col_loc, col_id, id_name):
	lines = df[[col_loc, col_id]].reset_index(drop=True)
	coordinates = lines[col_loc].apply(pd.Series)
	coordinates = coordinates.join(lines)
	coordinates = coordinates[[col_id, 'x', 'y']]
	coordinates['order'] = 0
	coordinates.rename(
	    columns={
	        col_id: id_name
	    },
	    inplace=True
	)
	return coordinates

def transform_coord_lines(df, col_id, id_name):
	lines = df['line'].apply(pd.Series).stack().reset_index(level=1).apply(pd.Series)

	lines.rename(
	    columns={
	        'level_1': 'order'
	    },
	    inplace=True
	)
	lines = lines.join(df)
	lines = lines[['order', 0, col_id]].reset_index(drop=True)
	coordinates = lines[0].apply(pd.Series)
	coordinates = coordinates.join(lines)
	coordinates = coordinates[['order', col_id, 'x', 'y']]
	coordinates.rename(
	    columns={
	        col_id: id_name
	    },
	    inplace=True
	)
	return coordinates