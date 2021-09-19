#!/usr/bin/env python
# coding: utf-8

# In[2]:


import requests
import json
import pandas as pd
import numpy as np
import os
from pandas.io.json import json_normalize
import ast
import json
import os
import geopandas as gpd
from datetime import date
from scipy import stats

import googlemaps
gmaps = googlemaps.Client(key='key')

# Walk Score api key
from walkscore import WalkScoreAPI
api_key = 'key'
walkscore_api = WalkScoreAPI(api_key = api_key)


# In[3]:

# #### CoreLogic API

# In[4]:

import requests

from requests.auth import HTTPBasicAuth


# In[5]:


clientKey = 'key'
clientSecret = 'key'


# In[6]:


# Generate API token

res=requests.post("https://prod.corelogicapi.com/oauth/token?grant_type=client_credentials", auth=HTTPBasicAuth(clientKey, clientSecret))

resapi = json.loads(res.text)

access_token = resapi["access_token"]

print(access_token)


# In[7]:


# Search for a Property Endpoint

streetAddress = "1920 6th Street, Santa Monica"
zipcode = 90405

url = "https://api-prod.corelogic.com/property?address={}&zip5={}".format(streetAddress, zipcode)

payload={}

headers = {

  'content-type': '{{contentType}}',

  'Authorization': 'Bearer ' + access_token,

}

response = requests.request("GET", url, headers=headers, data=payload)

res = json.loads(response.text)

print(res)

propId = res['data'][0]['corelogicPropertyId']


# In[8]:


# Function to call and obtain response text from CoreLogic API

def corelogic_api(address, zipcode, access_token):

    try:

        # Call Property API Endpoint
        streetAddress = address
        zipcode = zipcode

        url = "https://api-prod.corelogic.com/property?address={}&zip5={}".format(streetAddress, zipcode)

        payload={}

        headers = {

          'content-type': '{{contentType}}',

          'Authorization': 'Bearer ' + access_token,

        }

        response = requests.request("GET", url, headers=headers, data=payload)

        res = json.loads(response.text)

        print(res)

        propId = res['data'][0]['corelogicPropertyId']


        # Retrive Property Details
        url = " https://api-prod.corelogic.com/property/{}/property-detail".format(propId)

        payload={}

        headers = {

          'Authorization': 'Bearer ' + access_token,

        }

        response = requests.request("GET", url, headers=headers, data=payload)

        propDetails = json.loads(response.text)

        if propDetails:
            return propDetails
        else:
            return 0

    except Exception as e:
        print("Exception", e)
        pass


# In[9]:


# Function Call

corelogic_api("4 Embarcadero, San Francisco", 90102, access_token)


# #### Append to Original Pandas DF containing CoreLogic response for each property

# In[10]:


# Iterate over rows in each pandas DataFrame and append CL API call response
df_sf_la_mf_agg_v7_og = pd.read_csv("df_sf_la_mf_agg_v7_og.csv")

df_sf_la_mf_agg_v7_og['CoreLogic'] = df_sf_la_mf_agg_v7_og[94:194].apply(lambda x: corelogic_api(x['Address'], x['Zip Code'], access_token), axis=1)


# In[11]:




# #### Walkscore API

# In[12]:




def walkscore(lat, long, address, scores):

    try:

        result = walkscore_api.get_score(latitude = lat, longitude = long, address = address)

        # the WalkScore for the location
        if scores == 'walk':

            ws = result.walk_score
            return ws

        # the TransitScore for the location
        if scores == 'transit':

            ts = result.transit_score
            return ts

    except Exception as e:
        print(e)
        pass


# #### BLS Data

# ##### Employment

# In[78]:


# Series IDs
msa_emp_ids = {'SMS06310800000000001':'Los Angeles-Long Beach-Anaheim, CA',
               'SMS06418600000000001':'San Francisco-Oakland-Hayward, CA',
               'SMS06419400000000001':'San Jose-Sunnyvale-Santa Clara, CA'}

headers = {'Content-type': 'application/json'}
data = json.dumps({"seriesid": ['SMS06419400000000001'],"startyear":"2017", "endyear":"2021"})
p = requests.post('https://api.bls.gov/publicAPI/v2/timeseries/data/', data=data, headers=headers)
json_data = json.loads(p.text)



# In[79]:


# Iterate over data for each series

df_bls_sj = pd.DataFrame()

for series in json_data['Results']['series']:

    i = 0

    for data in series['data']:

        # Counter for pandas dataframe constructor index
        i = i + 1

        # Remove footnotes
        data.pop('footnotes', None)

        df_bls_sj = df_bls_sj.append(data, ignore_index=True)



# In[80]:


df_bls_sj.to_pickle('df_bls_sj.pickle')


# ##### Wages

# In[90]:


# BLS weekly earnings

msa_wage_ids = {'SMU06310800500000011':'Los Angeles-Long Beach-Anaheim, CA',
                'SMU06418600500000011': 'San Francisco-Oakland-Hayward, CA',
                'SMU06419400500000011': 'San Jose-Sunnyvale-Santa Clara, CA'}

headers = {'Content-type': 'application/json'}
data = json.dumps({"seriesid": ['SMU06419400500000011'], "startyear":"2017", "endyear":"2021"})
p = requests.post('https://api.bls.gov/publicAPI/v2/timeseries/data/', data=data, headers=headers)
json_data = json.loads(p.text)



# In[92]:


# Iterate over data for each series

df_bls_wage_sj = pd.DataFrame()

for series in json_data['Results']['series']:

    i = 0

    for data in series['data']:

        # Counter for pandas dataframe constructor index
        i = i + 1

        # Remove footnotes
        data.pop('footnotes', None)

        df_bls_wage_sj = df_bls_wage_sj.append(data, ignore_index=True)


# In[93]:


df_bls_wage_sj.to_pickle('df_bls_wage_sj.pickle')


# #### Function to geocode using google maps places API

# In[8]:


# function to geocode address to lat and long

count = 0

def geocode(address, field):

    global count
    count = count + 1
    print(count)

    try:

        # API Call
        result = gmaps.geocode(address)

        if field == "Lat":

            lat = result[0]['geometry']['location']['lat']
            return lat

        if field == "Lng":

            lng = result[0]['geometry']['location']['lng']
            return lng

    except:
        pass



# In[235]:


# Google Maps API for pulling place details and photos

from collections import defaultdict

def getPlace_details(address, identifier, text):

    # Declare a dictionary
    image_dict = defaultdict(list)

    global c
    c=0

    try:

        # API Call
        result = gmaps.find_place(address, input_type = text, language="en")

        plc_id = result['candidates'][0]['place_id']

        print("Place ID:", plc_id)

        plc_details = gmaps.place(plc_id, fields=["formatted_address", "geometry", "photo"])

        result = plc_details['result']

        #print(result)

        for el in result['photos']:

            ph_refs = [el['photo_reference'] for x in el]

            for ph in list(set(ph_refs)):

                c = c + 1

                # Download / open photo
                img_obj = gmaps.places_photo(ph, max_width = 500, max_height = 400)

                print(img_obj)

                s3 = boto3.client('s3')

                with open('photos/{}_image_{}.png'.format(identifier, c), 'wb') as data:
                    for chunk in img_obj:
                        data.write(chunk)

                s3.upload_file('photos/{}_image_{}.png'.format(identifier, c), 'gmaps-images', 'property_images/{}_image_{}.png'.format(identifier, c))

                # Create a list of dictionary of images for carousel
                url = "https://gmaps-images.s3.us-west-1.amazonaws.com/property_images/{}_image_{}.png".format(identifier, c)

                image_dict[identifier].append(url)

        image_dict = dict(image_dict)

        #print(type(image_dict))

        return image_dict

    except Exception as e:
        print(e)
        pass


# In[385]:


getPlace_details("Mosso 900 Folsom Street San Francisco", "id", "textquery")


# Apply getPlace details function over a pandas dataframe to obtain images

df1['Image_dicts'] = np.where(df1['Property Name'] != df1['Address_Comp'],
                              df1.apply(lambda x: getPlace_details(x['Property Name'] + " " + x['Address_Comp'], x['PropertyID'], "textquery"), axis=1),
                              df1.apply(lambda x: getPlace_details(x['Address_Comp'], x['PropertyID'], "textquery"), axis=1))


# In[179]:


# Apply getPlace details to rows where images weren't pulled in the first attempt

df1['Image_dicts'] = df1.apply(lambda x: getPlace_details(x['Property Name'] + " " + x['City'] + "," + x['State'], x['PropertyID'], "textquery") if type(x['Image_dicts']) == int else x['Image_dicts'], axis=1)


# Export and Read
df2 = pd.read_csv('df_sf_la_mf_agg_v5.csv')
df2.sample(5)


# In[203]:


# Image dictionaries

try:

    df2['Image_dicts'] = df2.loc[df2['Image_dicts'].str.startswith('defaultdict', na=False), 'Image_dicts'].str.extract('({.*})', expand=False).apply(ast.literal_eval)

    df['dict'] =     df.loc[df['img_dict'].str.startswith('defaultdict', na=False), 'img_dict'].str.extract('({.*})', expand=False).apply(ast.literal_eval)

except Exception as e:
    print(e)
    pass


# In[ ]:


# Call function to pull places details + images
img_list_dict = []

for index, row in result_df.iterrows():

    # pass addresses in pandas df
    search_string = row["Property Name"] + " " + row["Address"]
    img_dict = getPlace_details(search_string, row["PropertyID"], "textquery")

    print(img_dict)

    # Create a list of dictionaries
    img_list_dict.append(img_dict)

print("List of dicts", img_list_dict)


# In[ ]:


for d in img_list_dict:

    if d:

        for k,v in d.items():

            print(k,v)


# Google Streetview API

# Import google_streetview for the api module
import google_streetview.api

# Define parameters for street view api
params = [{
           'size': '600x300', # max 640x640 pixels
           'location': '46.414382,10.013988',
           'heading': '151.78',
           'pitch': '-0.76',
           'key': 'AIzaSyC0XCzdNwzI26ad9XXgwFRn2s7HrCWnCOk'
}]

# Create a results object
results = google_streetview.api.results(params)

# Download images to directory 'downloads'
results.download_links('Downloads')


# #### Mapillary API

# In[ ]:


import json, requests
from IPython.display import Image, display # import module to print an image

# set our building blocks
tile_coverage = 'mly1_public'
tile_layer = "image"
access_token = 'key' # client ID
coordinates = '-122.40347,37.780205' # the coordinates of our point of interest
distance = 15 # the maximum distance in meters that our image should be from the point it looks toward

# API call URL - we just want one image, and no pagination
# we use the parameters lookat and close to, because we want an image close to our point of interest but also looking at it
tile_url = 'https://tiles.mapillary.com/maps/vtp/{}/2/{}/{}/{}?access_token={}'.format(tile_coverage,tile.z,tile.x,tile.y,access_token)
response = requests.get(tile_url)

# request a JSON showing the point location and metadata of the images looking at our coordinates
resp = requests.get(url)

data = resp.json()

print(data)
