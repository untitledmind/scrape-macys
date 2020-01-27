"""Macy's web scraper with asyncronous I/O
Last updated Jan 26 2019
Author: Ethan Brady
"""

import requests
import re
import aiohttp
import asyncio
from aiohttp import ClientSession, ClientConnectorError
import urllib.parse
from bs4 import BeautifulSoup

def call_soup(url: str, headers: str) -> str:
    """visits index and returns soup"""
    response = requests.get(url, headers=headers)
    soup = BeautifulSoup(response.text, 'html.parser')
    return soup

def get_category_href(url: str, agent: str) -> set:
    """finds all category href from index page
    returns set of formatted urls with index page removed
    """
    soup = call_soup(url, agent)
    hrefs = {
        format_link(i.get('href'))
        for i in soup.find_all('a', href=re.compile('/shop'))
    }
    try:
        hrefs.remove(url)
    except KeyError:
        pass
    return hrefs
    
async def fetch_html(url: str, session: ClientSession, headers: dict, **kwargs) -> str:
    """GET request wrapper to fetch page html and convert to soup
    headers simulate Apple iPhone running Safari
    """
    response = await session.request(method='GET', url=url, headers=headers, **kwargs)
    print(f'clicked on link {url}')
    html = await response.text()
    return html

def format_link(url: str) -> str:
    """ensures returned url starts with https://www.macys.com"""
    url = re.sub(r'^/shop/', 'https://www.macys.com/shop/', url)
    url = re.sub(r'^//www.', 'https://www.', url)
    if not re.search(r'^https://', url):
        url = ''.join(['https://', url])
    return url

def soup_products(categories: set) -> set:
    """takes category html, turns to BeautifulSoup,
    then returns product links
    """
    products = {
        format_link(i.get('href'))
        for i in BeautifulSoup(html, 'html.parser').find_all('a', {'class': 'productDescLink'}, href=re.compile('/shop'))
        for html in categories
    }
    return hrefs

def soup_product_data(html: str) -> dict:
    """visits each product link and finds
    formatted json of product data
    """
    soup = BeautifulSoup(html, 'html.parser')
    product = json.loads(soup.find('script', {'id': 'productMktData'}).text)
    # asset = json.loads(soup.find('script', {'type': 'text/javascript'}).text)
    print(product)
    return product

async def make_requests(urls: set, headers: dict, **kwargs) -> list:
    """asynchronously make http requests"""
    async with ClientSession() as session:
        tasks = [
            fetch_html(url, session=session, headers=headers, **kwargs)
            for url in urls
        ]
        results = await asyncio.gather(*tasks)
        return results

if __name__ == '__main__':
    import pathlib
    import sys

    assert sys.version_info >= (3, 7), "Script requires Python 3.7+."
    here = pathlib.Path(__file__).parent
    
    url = 'https://www.macys.com/shop/sitemap-index?id=199462'
    headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_2) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/13.0.4 Safari/605.1.15'}
    
    categories = get_category_href(url, headers)
    
    loop = asyncio.get_event_loop()
    results = loop.run_until_complete(make_requests(urls=categories, headers=headers))
    
    for i in results:
        print(i)