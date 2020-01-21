"""Macy's web scraper
Author: Ethan Brady
last updated Jan 16 2020
This script creates a database Macy's products by scraping their online website with BeautifulSoup and the requests module.
Because the program makes http requests in a for-loop context, it's rather slow.
Note: For speed reasons, the loop currently breaks at the first category;
thus only the first category in the index is scraped for products.
To speed up the script, next step is to implement multithreaded or asynchronous requests.
"""

import requests
import asyncio
import aiofiles
import aiohttp
from aiohttp import ClientSession
from bs4 import BeautifulSoup
import re
import json

def call_soup(url: str, agent: str) -> str:
    """visits url and returns soup"""
    response = requests.get(url, headers=agent)
    soup = BeautifulSoup(response.text, 'html.parser')
    return soup

async def fetch_soup(url: str, session: ClientSession, **kwargs) -> str:
    """GET request wrapper to fetch page html and convert to soup"""
    response = await session.request(method='GET', url=url, **kwargs)
    soup = BeautifulSoup(response.text, 'html.parser')
    return soup

async def make_requests(urls: set, **kwargs):
    async with ClientSession() as session:
        hrefs = {
            format_link(i)
            for i in fetch_soup(url).find_all('a', href=re.compile('/shop'))
            for url in urls
        }

def format_link(url: str):
    """ensures returned url starts with https://www.macys.com"""
    url = re.sub(r'^/shop/', 'https://www.macys.com/shop/', url)
    url = re.sub(r'^//www.', 'https://www.', url)
    if not re.search(r'^https://', url):
        url = ''.join(['https://', url])
    return url

def get_category_href(url: str, agent: str) -> set:
    """finds all category href from index page
    returns set of formatted urls with index page removed
    """
    soup = call_soup(url, agent)
    hrefs = {
        format_link(i.get('href'))
        for i in soup.find_all('a', href=re.compile('/shop'))
    }
    hrefs.remove(url)
    return hrefs

def get_product_href(url: str, agent: str) -> set:
    """finds all product href from category page
    returns set of formatted urls
    """
    soup = call_soup(url, agent)
    hrefs = {
        format_link(i.get('href'))
        for i in soup.find_all('a', {'class': 'productDescLink'}, href=re.compile('/shop'))
    }
    return hrefs

def get_product_data(url: str, agent: str) -> dict:
    """reads json from product page soup"""
    soup = call_soup(url, agent)
    product = json.loads(soup.find('script', {'id': 'productMktData'}).text)
    # asset = json.loads(soup.find('script', {'type': 'text/javascript'}).text)
    return product #, asset

def push_to_sql(products: list) -> None:
    """takes list of each product's data
    pushes to SQL table
    """
    import sqlite3

    with sqlite3.connect('macys_products.db') as conn:
        c = conn.cursor()
        c.execute(
            """CREATE TABLE IF NOT EXISTS products (
                product_id INT,
                name VARCHAR(255),
                category VARCHAR(255),
                image VARCHAR(500),
                url VARCHAR(500),
                product_type VARCHAR(50),
                brand VARCHAR(255),
                description VARCHAR(500),
                currency VARCHAR(5),
                price FLOAT,
                sku VARCHAR(15),
                availability VARCHAR(100),
                price_valid_until VARCHAR(25)
            );
            """
        )

        rows = []
        for product in products:
            name = product.get('name')
            category = product.get('category')
            product_id = int(product.get('productID'))
            image = product.get('image')
            url = product.get('url')
            product_type = product.get('@type')
            brand = product.get('brand').get('name')
            description = product.get('description')
            currency = product.get('offers')[0].get('priceCurrency')
            price = float(product.get('offers')[0].get('price'))
            sku = product.get('offers')[0].get('SKU')
            availability = product.get('offers')[0].get('availability')
            price_valid_until = product.get('offers')[0].get('priceValidUntil')

            data = (
                product_id, name, category, image, url, product_type, brand,
                description, currency, price, sku, availability, price_valid_until
            )
            placeholders = ', '.join(['?'] * len(data))
            print(data)

            rows.append(data)

        c.executemany(f'INSERT INTO products VALUES ({placeholders});', rows)
        print('Pushed data to SQL table')

def main() -> None:
    """begins function chain starting at Macy's index page
    in this order:
    1. index
    2. categories
    3. product links
    4. product data
    5. to SQL
    Note: Currently the loop breaks at the first category;
    thus only the first category in the index is scraped for products.
    """
    url = 'https://www.macys.com/shop/sitemap-index?id=199462'
    agent = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_2) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/13.0.4 Safari/605.1.15'}
    categories = get_category_href(url, agent)
    print(f'{len(categories)} categories found')
    
    product_links = set()
    for i, url in enumerate(categories):
        product_links.update(get_product_href(url, agent))
        print(f'clicked on category {i}...')
        break

    products = []
    for i, url in enumerate(product_links):
        products.append(get_product_data(url, agent))
        print(f'clicked on product {i}...')

    push_to_sql(products)

if __name__ == '__main__':
    import time

    start = time.process_time()
    main()
    end = time.process_time()
    print(f'Script executed in {end - start} seconds')