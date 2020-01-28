"""Macy's web scraper with asyncronous I/O
Last updated Jan 27 2019
Author: Ethan Brady

issue with SSL handshake in loop 2 in main()
"""

import requests
import re
import aiohttp
import asyncio
from aiohttp import ClientSession, ClientConnectorError
from bs4 import BeautifulSoup
import time

def call_soup(url: str, headers: str) -> str:
    """visits index page and returns soup"""
    response = requests.get(url, headers=headers)
    soup = BeautifulSoup(response.text, 'html.parser')
    print(f'found soup from {url}')
    return soup

def get_category_href(url: str, agent: str) -> set:
    """finds all category href from index page
    returns set of formatted urls
    ensures index page does not reappear and create infinite loop
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
    print(f'{time.process_time()}: gathered {len(hrefs)} category links')
    return hrefs
    
async def fetch_html(url: str, session: ClientSession, headers: dict, **kwargs) -> str:
    """GET request wrapper to fetch page html and convert to soup
    headers simulate Apple iPhone running Safari
    """
    response = await session.request(method='GET', url=url, headers=headers, **kwargs)
    print(f'{time.process_time()}: clicked on link {url}')
    await asyncio.sleep(.0001)
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
    product_links = set()
    for html in categories:
        for i in BeautifulSoup(html, 'html.parser').find_all('a', {'class': 'productDescLink'}, href=re.compile('/shop')):
            pl = format_link(i.get('href'))
            product_links.add(pl)
    print(f'{time.process_time()}: gathered {len(product_links)} product links')
    return product_links

def soup_product_data(product_links: set) -> set:
    """visits each product link and finds
    formatted json of product data
    """
    products = set()
    for html in product_links:
        for soup in BeautifulSoup(html, 'html.parser'):
            p = json.loads(soup.find('script', {'id': 'productMktData'}).text)
            products.add(p)
    print(f'{time.process_time()}: gathered {len(products)} products')
    return products

async def make_requests(urls: set, headers: dict, **kwargs) -> set:
    """asynchronously make http requests"""
    async with ClientSession() as session:
        tasks = {
            fetch_html(url, session=session, headers=headers, **kwargs)
            for url in urls
        }
        results = await asyncio.gather(*tasks)
        return results

def main(url: str, headers: dict) -> None:
    """begins function calls
    index page -> category links -> product links -> product data
    """
    category_links = get_category_href(url, headers)
    
    print('beginning loop 1')
    loop = asyncio.get_event_loop()
    category_html = loop.run_until_complete(make_requests(urls=category_links, headers=headers))
    print('finished loop 1')

    product_links = soup_products(category_html)

    print('beginning loop 2')
    loop2 = asyncio.get_event_loop()
    products = loop2.run_until_complete(make_requests(urls=product_links, headers=headers))
    print('finished loop 2')

    product_data = soup_product_data(products)
    print(product_data)

if __name__ == '__main__':
    import pathlib
    import sys

    assert sys.version_info >= (3, 7), "Script requires Python 3.7+."
    here = pathlib.Path(__file__).parent
    
    url = 'https://www.macys.com/shop/sitemap-index?id=199462'
    browsers = {
        'chrome': {
            'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
            'accept-encoding': 'gzip, deflate, br',
            'accept-language': 'en-US,en;q=0.9',
            'cache-control': 'max-age=0',
            'cookie': 'shippingCountry=US; currency=USD; SignedIn=0; GCs=CartItem1_92_03_87_UserName1_92_4_02_; mercury=true; SEED=4297537474248053226; akavpau_www_www1_macys=1580181542~id=47a71e912de83f9dda99f700a9a9da7d; bm_sz=683F6131A58FEA17FA7F1DED9FBD61A6~YAAQv+ZNF0ltLX5uAQAAOUEk6gbo5IAaamgThDzXjTegz4SaaNaJWADe3Nmmv9d+zZQpRmtOrxc/5+rP+eFpbgi2s6l8hDNMKkEvQie9ev/SDWCwcHXsvUr4vjERPr638cFCR8DDjiJZQ3yuHycNj4AdvxJ1vzVwueB24XM53GGJUNp/Pw2EP7BDYGhoSqI=; FORWARDPAGE_KEY=https%3A%2F%2Fwww.macys.com%2F; check=true; _abck=76A4CB7D055CE7F7AD2660120EF53F23~0~YAAQv+ZNF3dtLX5uAQAA4lQk6gNbLHZ6m3ptJult2X4HW+zMVCTOScMKOxCz8cl5gtEfALJXFA8RTZrqSWb90nWxZdzAXRj9I5ThGDIpeAxaIW4WGbVmKRGQRTNsENwUtRc0xLHRbnL7RUzL2+BGd7QmTTx/2UYqhKAtLLiAjg34BMvxugb23ugECXJnbSBnbdd1iD3XcW8sPGWSzcRXI1Mgu9eOcdHTqDmCVWAE+6+dyy+RReM4NgI8KncFylu58Mudo2szqbpjxeXzydKqRlsp1lD+uvC/IHdee8SgvXJ5/+9ozmqw+ow3oTJ93XFo4tTRnrwE~-1~||1-MtzeFtturg-3500-100-3000-2||~-1; ak_bmsc=D0DFEC694422299BCCD0DFC8A43B0000174DE6BF8A760000FAA62F5E68048468~plG9Ch7CDPTUl6HbnYAm4zWd0xs6SDZkU/GgAeLXdANoIgjgj+cffEu+WNECYgNcgFvIkwbrk51KxCygWNmm9a/8HlKOgcPHWeU2OEZLWk1NSchoJslzHhPgzpw9YgRladfDq8IGq5VXZnkMlITpqIeMaRSu6zedVhWIahI8B73kKkTJOOy423ROchD1bKBjwM1LueAPO2atNKWdPMdZQJXLXqTjgPG9XHLp+Z83wDs8dUuQoSsXRDJEu+jqnBr2ul; AMCVS_8D0867C25245AE650A490D4C%40AdobeOrg=1; mbox=session#fe24d7092cc548ad9e4cf71066190ffd#1580183114|PC#fe24d7092cc548ad9e4cf71066190ffd.28_0#1643426054; AMCV_8D0867C25245AE650A490D4C%40AdobeOrg=-1891778711%7CMCIDTS%7C18290%7CMCMID%7C21222523593080730490068014037402302326%7CMCAAMLH-1580786049%7C9%7CMCAAMB-1580786049%7C6G1ynYcLPuiQxYZrsz_pkqfLG9yMXBpb2zX5dvJdYQJzPXImdj0y%7CMCOPTOUT-1580188449s%7CNONE%7CMCAID%7CNONE%7CvVersion%7C2.4.0; dca=D12; bm_sv=C6D0BFF1457F53433E77D90E3A77639C~7CX6PC/3LnPP5HjGUJqpFhXi5Crl8DSTdwK92ouINhC+fyx3xJTLvkYV1Z54QFjt0qfhZpR0fS9z3rqT1CBi3TFFlvBzF46O4seTdkOfJW1KMo9ij9Q7sZJad7piP6KdfVk3sjouioG+77UxZo/1HsVfNt3yR19lfVFT87SaX20=; SFL=5359; CSL=5359; MISCGCs=USERPC1_92_752253_87_USERLL1_92_32.869876%2C-96.7724843_87_USERST1_92_TX3_87_USERDMA1_92_6233_87_DT1_92_PC3_87_BTZIPCODE1_92_752013_87_BOPSPICKUPSTORE1_92_5359; s_pers=%20c29%3Dmcom%253Ahome%2520page%7C1580183074301%3B%20v30%3Dhome%2520page%7C1580183074309%3B; s_sess=%20s_cc%3Dtrue%3B; utag_main=v_id:016fea24bd730050053f4fa28f0003079001607100838$_sn:1$_ss:0$_st:1580183081109$ses_id:1580181273974%3Bexp-session$_pn:1%3Bexp-session$vapi_domain:macys.com; TLTSID=91094515275742440367218244657145; CRTOABE=1; smtrrmkr=637157781018313685%5E016fea25-2a47-4584-b7be-18744ba893cc%5E016fea25-2a47-4cb9-82fd-70e91e20c041%5E0%5E209.58.147.244; _ga=GA1.2.407340114.1580181304; _gid=GA1.2.1917701923.1580181304; cd_user_id=16fea253339602-08898e5818140e-39607b0f-fa000-16fea25333a7d; RT="z=1&dm=macys.com&si=80b848d4-d1af-44bf-acc4-c378f95dcd59&ss=k5xb3z27&sl=1&tt=pxz&bcn=%2F%2F17c8edc5.akstat.io%2F&ld=q16&ul=2vmq"; CONSENTMGR=ts:1580181372932%7Cconsent:true',
            'if-none-match': "7f071-58rdgZ3c1djXOs360PCz16o5A60",
            'sec-fetch-mode': 'navigate',
            'sec-fetch-site': 'none',
            'sec-fetch-user': '?1',
            'upgrade-insecure-requests': '1',
            'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_2) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/79.0.3945.130 Safari/537.36'
        },
        'firefox': {
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Encoding': 'gzip, deflate, br',
            'Accept-Language': 'en-US,en;q=0.5',
            'Connection': 'keep-alive',
            'DNT': '1',
            'TE': 'Trailers',
            'Upgrade-Insecure-Requests': '1',
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel …) Gecko/20100101 Firefox/72.0'
        },
        'safari': {
            'Cookie': 'bm_sv=6C12CAB332303B1203C2544E6B29C69C~7CX6PC/3LnPP5HjGUJqpFhpvOpWt8fn0jIVrHyIuygc2R2oCXWEuIlERsXQBFo3FDnXMsFFRzmqBECPyajsMkkeK3YVIY6FWDvwgTJgEzEvFfL3sEuFkl7iWPnTzuxZ1/5nEUPM8yQohPSKbQOvDDPe2qy2Z+0bOMEKE8sQSsxE=; CSL=5359; MISCGCs=USERPC1_92_752253_87_USERLL1_92_32.869876%2C-96.7724843_87_USERST1_92_TX3_87_USERDMA1_92_6233_87_DT1_92_PC3_87_BTZIPCODE1_92_752013_87_BOPSPICKUPSTORE1_92_5359; GCs=CartItem1_92_03_87_UserName1_92_4_02_; SFL=5359; dca=D12; FORWARDPAGE_KEY=https%3A%2F%2Fwww.macys.com%2Fshop%2Fproduct%2Fholiday-lane-gold-tone-crystal-pink-rose-pin-created-for-macys%3FID%3D10348036%26CategoryID%3D264958; SEED=-8782270029644492310%7C535-21; akavpau_www_www1_macys=1580185514~id=014b6feef7e1f194c04588716670a7f4; RTD=85be80856760855770855ca08511d0858e7085a6d0852470; ak_bmsc=0252C18C558CD784FB21B5634956EB5E174DE6BF8A760000C9A52F5EDCEAA66C~pl/Y+jLc7tp9kHFxo4ry4trPSqaTCHxbvmUxQdesQx+EcWrUMQOWvimXUl2sqrr4QDRyqW4L/Z9idlRYvp1mjTyGxX6Nmg2CGqcq3WCHXJedsbIH/RV9GpdUEXq6izT8iyUNya2b5Tly7y1JPPLv8iIVYxySaApEPuCMsy6xAH/0fCMYp0Vgda7izmbLzMgwqWEngDkJvO7zyoSdLyHLidOVe8YXwiF090EwjOEn0qBVc17K5Yl5xVpvd5ufouA2Qf; _abck=6BF046B8069834250D8410952CA7B656~0~YAAQv+ZNF59nLX5uAQAAqrEf6gNRMHj+06g0cisKzNlsn2QsKRBZzgh2IG8mtJsy2exwyCs9APwVhgamkGp0UWCBIHvwTOYhFLu88BuO+Xmt6AxgQIRZEYnen1IUBzofFMBk9YkgbeCh925a9dwrQ/AmPx9gA7yeoi239FOcUe+dDwjcIg6UyHlZtOFUP4J4EL4M4lh9dUoGNS4CfZ7X0H7NCeRDZ/QqpvRgJqgztXyGCGfiEELVFCxp/3ye4XQKEidqHui11sV7+J4BnShjLpIj7OQC9uwR1Yr8pxvOQ+B2talFlWtb8dUQmeM/7TGZUxCwm3vJ~-1~||1-hMldRrKyOp-3500-100-3000-2||~-1; SignedIn=0; bm_sz=6B23C6DF8923EFFE1783CC8A25DF4282~YAAQv+ZNF4FnLX5uAQAAcpsf6gbm08MDGc386e/W1H6Y1M5rp3+V1c8rRzgQ62cE0QDfUAdycTZGp9j7p4g2RM22b/D3c10gpXOh21gYbb6TR0R946d+QRD5fnUYRIsW4QnLr1sdpwlATHo0AQeSQYBhqtNJoUr6IQiXb04kTBf0hoGnS9dee6vc+VdRwL4=; currency=USD; mercury=true; shippingCountry=US',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_2) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/13.0.4 Safari/605.1.15',
            'Accept-Language': 'en-us',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive'
        }
    }

    headers = browsers['safari']

    start = time.process_time()
    main(url, headers)
    end = time.process_time()
    print(f'script executed in {end - start} seconds')