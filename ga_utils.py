import json
import pandas as pd
import auth

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

def get_transactions(profile_id):
	return ga_to_df(service.data().ga().get(
		ids='ga:' + profile_id,
		start_date='2016-01-01',
		end_date='2016-05-20',
		metrics='ga:itemQuantity',
		dimensions='ga:transactionId,ga:productName'
	).execute())