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

def get_hrefs(url, agent):
    """finds all href in BeautifulSoup and formats"""
    soup = call_soup(url, agent)
    hrefs = [format_link(i.get('href')) for i in soup.find_all('a', href=re.compile('/shop'))]
    return hrefs

def get_product_data(url, agent):
    """reads json from Macy's javascript asset"""
    soup = call_soup(url, agent)
    product = json.loads(soup.find('script', {'id': 'productMktData'}).text)
    asset = json.loads(soup.find('script', {'type': 'text/javascript'}).text)
    return product, asset

def main():
    """begins function chain starting at Macy's index page"""
    url = 'https://www.macys.com/shop/sitemap-index?id=199462'
    agent = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_2) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/13.0.4 Safari/605.1.15'}
    categories = get_hrefs(url, agent)[:-3]

    products = set()
    for index, item in enumerate(categories):
        products_in_category = get_hrefs(item, agent)[:-3]
        print(f'clicked on category {index}')
        for item in products_in_category:
            products.add(get_product_data(item, agent))

    for i in products:
        print(i)

if __name__ == '__main__':
    main()