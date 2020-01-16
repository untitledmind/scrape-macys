"""Macy's web scraper
Author: Ethan Brady
last updated Jan 16 2020

This script creates a database Macy's products by scraping their online website with BeautifulSoup and the requests module.
Because the program makes http requests in a for-loop context, it's rather slow.
To speed up the script, next step is to implement multithreaded or asynchronous requests.
"""

import requests
from bs4 import BeautifulSoup
import re
import json

def call_soup(url, agent):
    """visits url and returns soup"""
    response = requests.get(url, headers=agent)
    soup = BeautifulSoup(response.text, 'html.parser')
    return soup

def format_link(url):
    """ensures returned url starts with https://www.macys.com"""
    url = re.sub(r'^/shop/', 'https://www.macys.com/shop/', url)
    url = re.sub(r'^//www.', 'https://www.', url)
    if not re.search(r'^https://', url):
        url = ''.join(['https://', url])
    return url

def get_category_href(url, agent):
    """finds all category href from index page
    returns set of formatted urls with index page removed
    """
    soup = call_soup(url, agent)
    hrefs = {format_link(i.get('href')) for i in soup.find_all('a', href=re.compile('/shop'))}
    hrefs.remove(url)
    return hrefs

def get_product_href(url, agent):
    """finds all product href from category page
    returns set of formatted urls
    """
    soup = call_soup(url, agent)
    hrefs = {format_link(i.get('href')) for i in soup.find_all('a', {'class': 'productDescLink'}, href=re.compile('/shop'))}
    return hrefs

def get_product_data(url, agent):
    """reads json from product page soup"""
    soup = call_soup(url, agent)
    product = json.loads(soup.find('script', {'id': 'productMktData'}).text)
    # asset = json.loads(soup.find('script', {'type': 'text/javascript'}).text)
    return product #, asset

def post_to_sql(products):
    """takes list of each product's data
    posts to SQL table
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

            data = (product_id, name, category, image, url, product_type, brand, description, currency, price, sku, availability, price_valid_until)
            placeholders = ', '.join(['?'] * len(items))

            print(data)
            print(placeholders)

            c.execute(f'INSERT INTO products VALUES ({placeholders});', data)

def main():
    """begins function chain starting at Macy's index page
    in this order:
    1. index
    2. categories
    3. product links
    4. product data
    5. to SQL
    """
    url = 'https://www.macys.com/shop/sitemap-index?id=199462'
    agent = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_2) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/13.0.4 Safari/605.1.15'}
    categories = get_category_href(url, agent)
    print(len(categories))
    print(categories)
    
    product_links = set()
    for i, url in enumerate(categories):
        product_links.update(get_product_href(url, agent))
        print(f'clicked on category {i}')
        break

    products = []
    for i, url in enumerate(product_links):
        products.append(get_product_data(url, agent))
        print(f'clicked on product {i}')

    post_to_sql(products)

if __name__ == '__main__':
    main()