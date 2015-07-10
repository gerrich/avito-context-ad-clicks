#!/usr/bin/env python

import re
import sys
import csv
import datetime

import sqlite3
conn = sqlite3.connect('database.sqlite')
#conn.row_factory = sqlite3.Row

def dict_factory(cursor, row):
    d = {}
    for idx, col in enumerate(cursor.description):
        d[col[0]] = row[idx]
    return d
conn.row_factory = dict_factory

c = conn.cursor()

"""
for offset in xrange(1000, 1100, 10):
  res = c.execute("select * from trainSearchStream limit 10 offset %d"%offset)
  if len(res) == 0:
    break
  for r in res:
    print r
"""

def tjoin(data):
  return "\t".join([str(a) for a in data])

def parse_date(string_date):
  return datetime.datetime.strptime(string_date, "%Y-%m-%d %H:%M:%S.%f")
 
def get_locations():
  rows = c.execute("select * from Location") 
  return {row["LocationID"]:row for row in rows}
location_dict = get_locations()
location_dict[''] = {"LocationID":0, "Level":"0", "RegionID":-1, "CityID":-1}

# extract data from db for batch of events
def get_data_batch(chunk):
  ids = ",".join(list(set([row['SearchID'] for row in chunk])))
  #print "ids: %s"%ids
  searches = c.execute('select * from SearchInfo where SearchID in (%s)'%(ids,))
  searches = {row['SearchID']:row for row in searches}
  #print "Serches:",searches

  ids = ",".join(list(set([row['AdID'] for row in chunk])))
  #print "ids: %s"%ids
  ads = c.execute('select * from AdsInfo where AdID in (%s)'%(ids,))
  ads = {row['AdID']:row for row in ads}
  #print "Ads:",ads

  for item in chunk:
    item['Search'] = searches[int(item['SearchID'])]
    item['Ad'] = ads[int(item['AdID'])]
    # append search / ad info to each row
    
    
    #print item
    date = parse_date(item["Search"]["SearchDate"])
    
    price = 0
    bad_price = 0
    fifty_price = (0,1)[item['Ad']['Price'][-2:]==".5"]
    m = re.match('\d(0*)(\.5)?$', item['Ad']['Price'])
    if m:
      zeros_price = len(m.group(1))
    else:
      zeros_price = 0 
    
    try:
      price = float(item['Ad']['Price'])
    except:
      bad_price = 1

    sloc = location_dict[item["Search"]["LocationID"]]
    aloc = location_dict[item["Ad"]["LocationID"]]    
 
    print tjoin([
      len(item["Search"]["SearchQuery"]),
      "01"[item["Search"]["CategoryID"] == item["Ad"]["CategoryID"]],
      sloc["Level"],
      aloc["Level"],
      "01"[sloc["Level"] == aloc["Level"]],
      "01"[sloc["LocationID"] == aloc["LocationID"]],
      "01"[sloc["RegionID"] == aloc["RegionID"]],
      "01"[sloc["CityID"] == aloc["CityID"]],
      date.weekday(),
      date.hour,
      price, bad_price, fifty_price, zeros_price,
      item['Position'],
      item['ObjectType'],
      item["HistCTR"],
      item["IsClick"]
      ])
  return chunk


train_data = open("trainSearchStream.tsv", 'r', )
reader = csv.reader(train_data, delimiter='\t', quotechar='"')
chunk = []
chunk_size = 1000
header = reader.next()
for row in reader:
  # get 1000 rows
  if len(chunk) < chunk_size:
    chunk.append(dict(zip(header,row)))
    continue
  
  get_data_batch(chunk)
  chunk=[]
get_data_batch(chunk)

