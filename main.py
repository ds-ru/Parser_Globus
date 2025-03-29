import json

import requests
from bs4 import BeautifulSoup
import pandas as pd
import aiohttp
import asyncio
import json


async def fetch_json(text: str):
    soup = BeautifulSoup(text, 'html.parser')
    target_script = soup.find("script", id='__NEXT_DATA__')
    data = json.loads(target_script.string)
    return data


async def fetch_product_data(data):
    if isinstance(data, dict):
        if "brand" in data:
            return data["brand"]
        for key, value in data.items():
            result = await fetch_product_data(value)
            if result is not None:
                return result
    elif isinstance(data, list):
        for item in data:
            result = await fetch_product_data(item)
            if result is not None:
                return result


async def main():
    data = pd.read_excel('sku_ids.xlsx')

    for sku_id in data['Код SKU']:
        url = f"https://online.globus.ru/products/{sku_id}_ST/"
        async with aiohttp.ClientSession(headers={
            "User-Agent": 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3'
        }) as session:
            async with session.get(url) as response:
                parsed_json = await fetch_json(await response.text())
        product_data = await fetch_product_data(parsed_json)
        print(product_data)
        break


if __name__ == "__main__":
    asyncio.run(main())
