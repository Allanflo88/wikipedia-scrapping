from requests import get
from requests import ConnectionError
from requests import Timeout
from urllib.parse import unquote
from concurrent.futures import ThreadPoolExecutor, TimeoutError, as_completed

from bs4 import BeautifulSoup
import mysql.connector as connector

BASE_URL = 'https://pt.wikipedia.org'
RANDOM_PATH = '/wiki/Especial:Aleatória'

def parse_tags(tags):
    return [unquote(tag.get('href')) for tag in tags if tag.get('href') != None]

def save(element, parent):
    links = element.find_all('a')
    parsed_tags = parse_tags(links) if links != None else []
    if RANDOM_PATH not in parent:
        try:
            connection = connector.connect(host='localhost', user='root', password='admin', database='wikipedia')
            if connection and connection.is_connected():
                cursor = connection.cursor()
                [cursor.execute('INSERT INTO wikipedia (parent, child) VALUES ("{}","{}")'.format(parent,tag)) for tag in parsed_tags]
        except connector.Error as e:
            print("database error: {}".format(e))
            parsed_tags = [RANDOM_PATH]
        finally:
            if connection and connection.is_connected():
                connection.commit()
                cursor.close()
                connection.close()

    return  parsed_tags

def get_box(url):
    try:
        with get(url if 'http' in url else BASE_URL + url) as response:
            page = BeautifulSoup(response.text, 'html.parser')
            data = page.find(class_='mw-parser-output')
            return save(data, url)
    except ConnectionError:
        print('url indisponível: {}'.format(url))
        
    except Timeout:
        print('url demorou: {}'.format(url))

links = [RANDOM_PATH]
with ThreadPoolExecutor(max_workers=5) as executor:
    while links:
        threads = [executor.submit(get_box, l)for l in links]
        links = []
        for t in as_completed(threads):
            try:
                links = links + t.result(timeout=5)
            except:
                print('Algo ocorreu')

