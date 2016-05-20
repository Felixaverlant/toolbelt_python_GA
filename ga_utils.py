import json
import pandas as pd
import auth
import config as config

service = auth.main()

def list_accounts():
	"""List all accounts """
	service = auth.main()
	accounts = service.management().accounts().list().execute()
	df = pd.read_json(json.dumps(accounts.get('items')))
	return df[['id','name']]

def list_properties(account):
	"""List all properties based on id column """
	properties = service.management().webproperties().list(
        accountId=account).execute()
	df = pd.read_json(json.dumps(properties.get('items')))
	return df[['accountId', 'id' , 'name']]

def list_profiles(account, property):
	"""List all properties based on id column from list_accounts() & id column form list_properties()"""
	profiles = service.management().profiles().list(
          accountId=account,
          webPropertyId=property).execute()
	df = pd.read_json(json.dumps(profiles.get('items')))
	return df[['accountId', 'id', 'name']]

def get_example(profile_id):
	"""Example query based on id column from list_profiles()"""
	return ga_to_df(service.data().ga().get(
		ids='ga:' + profile_id,
		start_date='7daysAgo',
		end_date='today',
		metrics='ga:sessions,ga:bounceRate',
		dimensions='ga:date'
	).execute())

def ga_to_df(d):
	"""Take a resp json from GA and transform to pandas DF """
	columns = [x['name'] for x in d['columnHeaders']]
	rows = [x for x in d['rows']]
	return pd.DataFrame(rows, columns=columns)

def raw_query(profile_id, start_date, end_date,metrics,dimensions):
	""" Utility method to make raw query """
	return service.data().ga().get(
		ids='ga:' + profile_id,
		start_date=start_date,
		end_date=end_date,
		metrics=metrics,
		dimensions=dimensions
	).execute()

def get_transactions(profile_id, start_date, end_date):
	""" Get transactions based on ga:productName """
	return ga_to_df(service.data().ga().get(
		ids='ga:' + profile_id,
		start_date=start_date,
		end_date=end_date,
		metrics='ga:itemQuantity',
		dimensions='ga:transactionId,ga:productName'
	).execute())

def get_all_pages(profile_id, start_date, end_date):
	""" Get all pages for a period of time """
	return ga_to_df(service.data().ga().get(
		ids='ga:' + profile_id,
		start_date=start_date,
		end_date=end_date,
		metrics='ga:pageviews',
		dimensions='ga:pagePath,ga:date'
	).execute())

def get_all_pageviews(profile_id, start_date, end_date):
	""" Get all pages for a period of time """
	return ga_to_df(service.data().ga().get(
		ids='ga:' + profile_id,
		start_date=start_date,
		end_date=end_date,
		metrics='ga:pageviews',
		dimensions='ga:date'
	).execute())

def filter_devices(device=''):
	if (device != ''):
		return 'ga:deviceCategory==' + device

def first_step(profile_id, start_date, end_date, device=''):
	device = filter_devices(device)
	return(
		ga_to_df(service.data().ga().get(
			ids='ga:' + profile_id,
			start_date=start_date,
			end_date=end_date,
			metrics = "ga:sessions, ga:bounceRate",
			filters = device
		).execute())
	)

def start_CO(first_step_CO, profile_id,start_date, end_date, device=''):
	device = filter_devices(device)

	return (
		ga_to_df(service.data().ga().get(
			ids='ga:' + profile_id,
		    start_date = start_date,
		    end_date = end_date,
		    metrics="ga:sessions",
		    sort="-ga:sessions",
		    segment= "sessions::condition::ga:pagePath=@"+first_step_CO,
		    filters = device
		).execute())
	)

def transactions(profile_id, start_date, end_date, device=''):
	device = filter_devices(device)

	return (
		ga_to_df(service.data().ga().get(
			ids='ga:' + profile_id,
			start_date = start_date,
			end_date = end_date,
			metrics="ga:transactions",
			filters= device
		).execute())
	)

def get_checkout_infos(profile_id,start_date, end_date, first_step_CO, device=''):
	f_step = first_step(config.profile_id,start_date, end_date)
	s_co = start_CO(config.first_step_CO, config.profile_id,start_date, end_date)
	transac = transactions(config.profile_id,start_date, end_date)
	
	total_sessions = f_step['ga:sessions']
	BR = float(f_step['ga:bounceRate'])
	
	before_shopping_percent = 100 - BR
	
	before_shopping = float(total_sessions) * BR / 100

	entry_CO_percent = int(float(s_co['ga:sessions']) / float(total_sessions) * 100)
	
	trans = transac['ga:transactions']
	transactions_percent = int(float(trans) / float(total_sessions) * 100)

	sco_f = float(s_co['ga:sessions'])
	bshop_f = float(before_shopping)
	transac_f = transac
	br_f = BR
	stays = pd.DataFrame(data={"status":["stays" for i in range(4)], "percent":[100, before_shopping_percent, entry_CO_percent, transactions_percent], "steps":["total", "Shopping", "Checkout", "Conversions"]})
	
	lost = pd.DataFrame(
		data={
			"status":["lost" for i in range(4)], 
			"percent": [
				0,
				br_f, 
				100-sco_f / bshop_f *100, 
				100 - float(trans) / float(s_co['ga:sessions']) * 100
			], 
			"steps":["total", "Shopping", "Checkout", "Conversions"]
		})

	df = pd.concat([stays,lost])
	df['device'] = device

	return df

