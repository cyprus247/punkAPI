'''
Short script that grabs random beers from punkapi and shoves them into mongodb
extra functions could be added to remove some of the clutter
'''
from pymongo import MongoClient #pymongo version 3.4
import requests # requests version 2.18.4
import pandas as pd #pandas version 0.20.3
from pandas.io.json import json_normalize

client = MongoClient('mongodb://<<user>>:<<superSecretPassword>>@ds161306.mlab.com:61306/cheap_database')
db=client.cheap_database
url = 'https://api.punkapi.com/v2/beers/random'
#create empty dataframes to load up with data 
big_df = pd.DataFrame()
food_df= pd.DataFrame()
hops_df= pd.DataFrame()
malt_df= pd.DataFrame()
mash_df= pd.DataFrame()
#iterate up to a number of beers, can be replaced with a parameter later
while (len(big_df.index) < 200):
    response = requests.get(url)
    json_r=response.json()
	#flatten the response json
    df = json_normalize(json_r,sep='_')
	#add the "_id" column to specify as index for mongodb
    df['_id'] = df.iloc[0]['id']
	#ugly workaround for empty dataframe
    if (len(big_df.index)==0):
        big_df=big_df.append(df)
        # separate food recommendations in another data frame
        for food in df.iloc[0]['food_pairing']:
            beer_id=df.iloc[0]['_id']
            food_df = food_df.append(pd.Series([beer_id,food]),ignore_index=True)
        #separate hops, malt and mash in dataframes of their own
        hops_df_row = json_normalize(df.iloc[0]['ingredients_hops'],sep='_')
        hops_df_row['beer_id'] = df.iloc[0]['_id']
        hops_df = hops_df.append(hops_df_row)
        malt_df_row = json_normalize(df.iloc[0]['ingredients_malt'],sep='_')
        malt_df_row['beer_id'] = df.iloc[0]['_id']
        malt_df = malt_df.append(malt_df_row)
        mash_df_row =json_normalize(df.iloc[0]['method_mash_temp'],sep='_')
        mash_df_row['beer_id'] = df.iloc[0]['_id'] 
        mash_df = malt_df.append(mash_df_row)
        
    elif (df.iloc[0]['_id'] in big_df['_id'].tolist()):
		#ensure we don't insert duplicates
        #print("We already drank this one") 
        continue
    else:
		#insert data into dataframes
        # separate food recommendations in another data frame
        for food in df.iloc[0]['food_pairing']:
            beer_id=df.iloc[0]['_id']
            food_df = food_df.append(pd.Series([beer_id,food]),ignore_index=True)
        #separate hops, malt and mash in data frames of their own
        hops_df_row=json_normalize(df.iloc[0]['ingredients_hops'],sep='_')
        hops_df_row['beer_id'] = df.iloc[0]['_id']
        hops_df = hops_df.append(hops_df_row)
        malt_df_row = json_normalize(df.iloc[0]['ingredients_malt'],sep='_')
        malt_df_row['beer_id'] = df.iloc[0]['_id']
        malt_df = malt_df.append(malt_df_row)
        mash_df_row =json_normalize(df.iloc[0]['method_mash_temp'],sep='_')
        mash_df_row['beer_id'] = df.iloc[0]['_id'] 
        mash_df = mash_df.append(mash_df_row)    
        big_df=big_df.append(df)
#rename food dataframe columns to prepare for insert        
food_df.columns= ["beer_id","food"] 
#create mongodb collections      
beers = db.beers_collection
hops = db.hops_collection
food = db.food_collection
malt = db.malt_collection
mash = db.mash_collection
#insert data
beers.insert_many(big_df.to_dict('records'))
hops.insert_many(hops_df.to_dict('records'))
food.insert_many(food_df.to_dict('records'))
malt.insert_many(malt_df.to_dict('records'))
mash.insert_many(mash_df.to_dict('records'))