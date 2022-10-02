import requests
import pandas as pd
import numpy as np
from fake_useragent import UserAgent
from bs4 import BeautifulSoup
import re
import time
import random
import datetime

ua = UserAgent()
#Find the total number of pages
def total_pages(url):
    useragent = ua.random    # randomly choose a user-agent
    headers = {'User-Agent':useragent}
    response = requests.get(url,headers=headers)
    soup = BeautifulSoup(response.content,'lxml')
    pages = soup.find('div',class_="pagination").find_all('li')[-1].find('a').get_text()
    pages = int(pages)    # 'pages' is the total number of pages
    return pages

print(total_pages('https://www.winemag.com/?s=&rating=98.0-*&drink_type=wine&page=1&search_type=reviews'))

#Function to parse each item's page
def help_retrieve(dic,key):
    try:
        return dic[key]
    except KeyError:
        return np.NaN

def parse_item(url):
    item_url = 'https://www.winemag.com/buying-guide' + url
    useragent = ua.random    # randomly choose a user-agent
    headers = {'User-Agent':useragent}
    response = requests.get(item_url,headers=headers)
    if response.status_code == 200:
        soup = BeautifulSoup(response.content,'lxml')
        try:
            description = soup.find('p',class_='description').get_text()
        except AttributeError:
            description = np.NaN
        try:
            taster = soup.find('span',class_='taster-area').get_text()
        except AttributeError:
            taster = np.NaN

        info_list = soup.find_all('li',class_='row')
        info_dict = dict()

        for element in info_list:
            tag = element.find_all('div')[0].get_text().strip()
            value = element.find_all('div')[1].get_text().strip()
            info_dict[tag] = value

        designation = help_retrieve(info_dict,"Designation")
        variety = help_retrieve(info_dict,"Variety")
        appellation = help_retrieve(info_dict,"Appellation")
        winery = help_retrieve(info_dict,"Winery")
        alcohol = help_retrieve(info_dict,"Alcohol")
        bottle_size = help_retrieve(info_dict,"Bottle Size")
        category = help_retrieve(info_dict,"Category")
        importer = help_retrieve(info_dict,"Importer")
        date_published = help_retrieve(info_dict,"Date Published")
        user_avg_rating = help_retrieve(info_dict,"User Avg Rating")

        if user_avg_rating == 'Not rated yet [Add Your Review]':
            user_avg_rating = np.NaN

        related_items = soup.find_all('li',class_='review-item')
        related_items = [element.find('a').get('data-review-id') for element in related_items]    # related_items holds a list of related items' ids

        return (description,taster,designation,variety,appellation,winery,alcohol,bottle_size,\
                category,importer,date_published,user_avg_rating,related_items)
    else:
        print(f"{item_url} request failed! Status code: {response.status_code}. Skip it.")
        raise RuntimeError


#Function to parse each page
# only store the ending of each item's url to save memory, because the former parts are the same for all
pattern = re.compile(r'https://www.winemag.com/buying-guide(?P<ending>.*)')
# request and parse each page
# return a list with items in this page as tuples inside
def parse_page(base_url,i):
    url = re.sub(r"page=\d+",f"page={i}",base_url)  
    useragent = ua.random    # randomly choose a user-agent
    headers = {'User-Agent':useragent}
    response = requests.get(url,headers=headers)
    if response.status_code == 200:
        soup = BeautifulSoup(response.content,'lxml')
        li = soup.find_all('li',class_="review-item")
        page = list()

        for element in li:
            try:
                name = element.find('h3').get_text()
                rating = element.find('span',class_="rating").find('strong').get_text()
                price = element.find('span',class_="price").get_text()
                price = price.replace('$','')
            except AttributeError:
                print(f"Some items are missed from page {i}: {url}. Skip it.")
                continue
            try:
                price = float(price)
            except ValueError:
                price = np.NaN
            item_url = element.find('a',class_="review-listing row").get('href')
            item_url = pattern.search(item_url).group('ending')
            item_id = element.find('a',class_="review-listing row").get('data-review-id')
            try:
                details = parse_item(item_url)
            except RuntimeError:
                details = (np.NaN,)*13
            all_info = (item_id,name,rating,price,item_url) + details
            page.append(all_info)
            
        return page
    else:
        print(f"Page #{i} request failed! Status code: {response.status_code}. Skip it.")
        raise RuntimeError()


#Scrape all the pages
def scrape_all(url):
    result = list()
    start = datetime.datetime.now()
    pages = total_pages(url)

    for i in range(1,pages+1):
        try:
            result.extend(parse_page(url,i))
            time.sleep(random.random())
        except RuntimeError:
            pass
        except:
            print(r"No worry! I'm still working :)")
        finally:
            if i%10 == 0:
                print(f"Process overview: {i} pages have been scraped...")

    end = datetime.datetime.now()
    interval = (end - start).total_seconds()
    hour = int(interval // 3600)
    minute = int((interval % 3600) // 60)
    second = int((interval % 3600) % 60)
    print(f"For base url: {url}")
    print(f"Scraping has been done in {hour}h {minute}min {second}s. Congrats!")
    
    return result
def scrape_url_list(urls, file_name="raw_data.csv"):
    result = list()
    
    for url in urls:
        try:
            print(f"Begin scraping from base url: {url}")
            result.extend(scrape_all(url))
        except:
            print(f"Scraping from base url: {url} FAILED! Going to the next one...")
            continue
        
    df = pd.DataFrame(result,columns=['id','name','rating','price','item_url','description','taster',\
                                      'designation','variety','appellation','winery','alcohol','bottle_size',\
                                        'category','importer','date_published','user_avg_rating','related_items'])
    df.to_csv(file_name,encoding='utf-8')
    
    return df

urls = ['https://www.winemag.com/?s=&rating=98.0-*&drink_type=wine&page=1&search_type=reviews']

df = scrape_url_list(urls)

df.head(10)