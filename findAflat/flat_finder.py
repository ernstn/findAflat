# imports
import boto3
from boto3.dynamodb.conditions import Key
from bs4 import BeautifulSoup
import requests as req
import re
import os
from datetime import datetime

# check that enables local testing
if os.getenv("LAMBDA"):
    session = boto3
    table_name = os.getenv("DB_TABLE")
else:
    session = boto3.session.Session(profile_name="default", region_name="eu-central-1")
    table_name = "findAflatDB"

# get dynamoDB table
dynamodb = session.resource("dynamodb")
DBTABLE = dynamodb.Table(table_name)

# define sns client
SNS_CLIENT = boto3.client("sns")
SNS_TOPIC = os.getenv("SNS_TOPIC")

# some parameters for searching
PAGE = "https://www.ohne-makler.net"
RADIUS = "25"
CITY = "Berlin"

# url
URL = PAGE + "/immobilie/list/?class=WHNG&q=" + CITY + "&state=&marketing=SELL&radius=" + RADIUS

# conditions
MIN_ROOMS = 2
MAX_ROOMS = 4
MAX_PRICE = 400000
POSTAL_CODES = ["12157", "12161", "12163", "12165", "12167", "12169", "12203", "12205", "12207", "12209", "12247", "12249", "12277", "12279", "14109", "14129", "14163", "14165", "14167", "14169", "14193", "14195", "14197", "14199", " 14054", "14052", "14050"]

# define global lists to store new information
NEW_DB=[];
NEW_PRICES=[]
NEW_LINKS=[]
INFO=[]

def main():

    # clear global arrays as lambda lives >30 mins
    NEW_DB.clear()
    NEW_PRICES.clear()
    NEW_LINKS.clear()
    INFO.clear()

    # read results from the previous search
    old_db_str = DBTABLE.query(
        KeyConditionExpression=Key("dataID").eq("objectID")
    )
    old_prices_str = DBTABLE.query(
        KeyConditionExpression=Key("dataID").eq("prices")
    )

    if (old_db_str["Count"]==0): # if data base was empty
        old_db = ""
        old_prices = ""
    else:
        old_db=old_db_str["Items"][0]["data"].split(";")
        old_prices=old_prices_str["Items"][0]["data"].split(";")

    next_page = URL

    # parse all pages
    while True:
        soup = parse_page(next_page)
        table = soup.find("table", { "class" : "table" })
        parse_table(table)
        next=get_next_page(soup)
        if not next:
            break
        next_page = PAGE+next

    # save latest found objects to DB to use those as reference for the next search
    string_db = ";".join(NEW_DB)
    string_prices = ";".join(NEW_PRICES)

    DBTABLE.put_item(
        Item={
            "dataID":"objectID",
            "data": string_db
        }
    )
    DBTABLE.put_item(
        Item={
            "dataID": "prices",
            "data": string_prices
        }
    )

    # create E-Mail message
    new_obj = []
    price_changed_obj =  []
    for i in range(len(NEW_DB)):
        if (NEW_DB[i] not in old_db):
            print("send email -> new object ", NEW_LINKS[i])
            new_obj.append(INFO[i])
        elif (NEW_PRICES[i] != old_prices[old_db.index(NEW_DB[i])]):
            print("send email -> price is changed", NEW_LINKS[i], old_prices[old_db.index(NEW_DB[i])], NEW_PRICES[i])
            price_changed_obj.append(INFO[i])

    if (not new_obj and not price_changed_obj):
        return

    text = "New objects:\n" + "\n".join(new_obj)+"\n\nPrice is changed:\n" + "\n".join(price_changed_obj)

    SNS_CLIENT.publish(
            TopicArn=SNS_TOPIC,
            Message=f"{text}",
            Subject=f"New Flats on ohne-makler.de: {datetime.now().date()}"
        )

# read page as bs4 object
def parse_page(url):
    page = req.get(url)
    soup = BeautifulSoup(page.content, "html.parser")
    return soup

# parse bs4 table, get and check flat attributes
def parse_table(table):
    for row in table.findAll("tr"):
        cells = row.findAll("td")
        # cells: 1 - link, picture and PLZ; 2 - link, info, address, 3 - price
        if (len(cells)==3):
            # first check region
            place = cells[1].find("strong", text="Adresse:").next_sibling.split(",")
            # if place consists only of 1 element, that means address is not known
            if (len(place)==2):
                address = "".join(place[0].split())
                plz = re.findall(r"\d+",place[1])[0]
            else:
                address = ""
                plz = re.findall(r"\d+",place[0])[0]
            if (plz and plz in POSTAL_CODES):
                n_rooms = float(cells[1].find("strong", text="Zimmer:").next_sibling.replace(",","."))
                size = cells[1].find("strong", text="Wohnfläche:").next_sibling
                price = "".join(cells[2].find("span").text.split("€")[0].split()).replace(".","")
                type = "".join(cells[0].find("br").next_sibling.split())
                if (n_rooms >= MIN_ROOMS and n_rooms <= MAX_ROOMS and float(price)<=MAX_PRICE and type!="Erdgeschosswohnung"):
                    link = check_link_prefix(cells[1].find("a").get("href"))
                    id = "".join(cells[1].find("strong", text="Objekt-Nr.:").next_sibling.split())
                    print(id, address, plz , n_rooms, size, price, type)
                    NEW_DB.append(id)
                    NEW_PRICES.append(price)
                    NEW_LINKS.append(link)
                    INFO.append(type+" "+ plz + ": " + str(n_rooms) + " rooms, " + size + ", " + price + "€ " + link)

# get link to the next page
def get_next_page(soup):
    next_page = soup.select(".next a")
    return(next_page[0].get("href"))

# some links could not be identified as likns because prefix is left
def check_link_prefix(link):
    if link.startswith( "https:" ):
        return(link)
    else:
        return(PAGE+link)