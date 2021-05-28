import requests
import json
import dataset


class Shopify_Scraper():

    def __init__(self, base_url):
        self.base_url = base_url

    def download_json(self, page):
        r = requests.get(self.base_url + f'products.json?limit=250&page={page}', timeout=5)
        if r.status_code != 200:
            print('Status Code Error', r.status_code)
        if len(r.json()['products']) > 0:
            data = r.json()['products']
            return data
        else:
            return

    def parse_json(self, json_data):
        products = []
        for prod in json_data:
            main_id = prod['id']
            title = prod['title']
            published_at = prod['published_at']
            product_type = prod['product_type']
            for v in prod['variants']:
                item = {
                    'id': main_id,
                    'title': title,
                    'published_at': published_at,
                    'product_type': product_type,
                    'var_id': v['id'],
                    'var_title': v['title'],
                    'sku': v['sku'],
                    'price': v['price'],
                    'available': v['available'],
                    'created_at': v['created_at'],
                    'updated_at': v['updated_at'],
                    'compare_at_price': v['compare_at_price']
                }
                products.append(item)
        return products


def main():
    allbirds = Shopify_Scraper('https://www.allbirds.co.uk/')
    results = []
    for page in range(1, 10):
        data = allbirds.download_json(page)
        print('Getting page: ', page)
        try:
            results.append(allbirds.parse_json(data))
        except:
            print(f'Completed, total pages = {page - 1}')
            break
    return results


if __name__ == '__main__':
    db = dataset.connect('sqlite:///products.db')
    table = db.create_table('allbirds', primary_id='var_id')
    products = main()
    totals = [item for i in products for item in i]

    for p in totals:
        if not table.find_one(var_id=p['var_id']):
            table.insert(p)
            print('New Product:', p)
