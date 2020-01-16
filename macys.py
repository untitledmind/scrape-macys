import requests
from bs4 import BeautifulSoup
import re
import json

def call_soup(url, agent):
    """visits website, returns soup"""
    response = requests.get(url, headers=agent)
    soup = BeautifulSoup(response.text, 'html.parser')
    return soup

def format_link(url):
    """ensures link starts with https://www.macys.com"""
    url = re.sub(r'^/shop/', 'https://www.macys.com/shop/', url)
    url = re.sub(r'^//www.', 'https://www.', url)
    if not re.search(r'^https://', url):
        url = ''.join(['https://', url])
    return url

def get_category_href(url, agent):
    """finds all category href from index page soup
    returns set of formatted urls
    """
    soup = call_soup(url, agent)
    hrefs = {format_link(i.get('href')) for i in soup.find_all('a', href=re.compile('/shop'))}
    hrefs.remove(url)
    return hrefs

def get_product_href(url, agent):
    """finds all product href from category page soup
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

# def json_to_sql(product):
#     import sqlite3

#     name = product['name']
#     category = product['category']
#     productID = product['productID']
#     image = product['image']
#     url = product['url']
#     product_type = product['@type']
#     brand = product['brand']['name']
#     description = product['description']
#     currency = product['offers'][0]['itemOffered']['priceCurrency']
#     price = product['offers'][0]['itemOffered']['price']
#     sku = product['offers'][0]['itemOffered']['SKU']
    # availability = product['offers'][0]['itemOffered']['availability']
    # price_valid_until = product['offers'][0]['itemOffered']['priceValidUntil']

#     """CREATE TABLE products


def main():
    """begins function chain starting at Macy's index page"""
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

    print(products)



if __name__ == '__main__':
    main()