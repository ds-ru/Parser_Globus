from bs4 import BeautifulSoup
import pandas as pd
import aiohttp
import asyncio
import json
import openpyxl


async def fetch_json(text: str):
    soup = BeautifulSoup(text, 'html.parser')
    target_script = soup.find("script", id='__NEXT_DATA__')
    data = json.loads(target_script.string)
    return data


async def fetch_brand(data):
    if isinstance(data, dict):
        if "brand" in data:
            if isinstance(data["brand"], dict):
                return data["brand"]["name"]
            else:
                return None
        for key, value in data.items():
            result = await fetch_brand(value)
            if result is not None:
                return result
    elif isinstance(data, list):
        for item in data:
            result = await fetch_brand(item)
            if result is not None:
                return result

async def fetch_name(data):
    if isinstance(data, dict):
        if "product" in data:
            if isinstance(data["product"], dict):
                return data["product"]["name"]
            else:
                return None
        for key, value in data.items():
            result = await fetch_name(value)
            if result is not None:
                return result
    elif isinstance(data, list):
        for item in data:
            result = await fetch_name(item)
            if result is not None:
                return result


async def find_volume_ml(data):
    if isinstance(data, dict):
        # Проверяем атрибут объема в мл
        if data.get("id") == "atr_calc_if_volume_range_in_ml":
            value = data.get("value", [None])[0]
            return (value, "мл") if value else None

        # Рекурсивный поиск в значениях словаря
        for value in data.values():
            result = await find_volume_ml(value)
            if result:
                return result

    elif isinstance(data, list):
        # Рекурсивный поиск в элементах списка
        for item in data:
            result = await find_volume_ml(item)
            if result:
                return result

    return None


async def find_volume_gram(data):
    if isinstance(data, dict):
        # Проверяем выбранный вес
        if data.get("is_selected", False) and "вес" in str(data.get("name", "")).lower():
            value = data.get("value")
            try:
                return (float(value) * 1000, "г") if value else None
            except (ValueError, TypeError):
                return None

        # Рекурсивный поиск в значениях словаря
        for value in data.values():
            result = await find_volume_gram(value)
            if result:
                return result

    elif isinstance(data, list):
        # Рекурсивный поиск в элементах списка
        for item in data:
            result = await find_volume_gram(item)
            if result:
                return result

    return None


async def find_volume_piece(data):
    if isinstance(data, dict):
        # Проверяем атрибут количества в упаковке
        if data.get("id") == "atr_quantity_in_package":
            value = data.get("value", [None])[0]
            return (value, "шт") if value else None

        # Рекурсивный поиск в значениях словаря
        for value in data.values():
            result = await find_volume_piece(value)
            if result:
                return result

    elif isinstance(data, list):
        # Рекурсивный поиск в элементах списка
        for item in data:
            result = await find_volume_piece(item)
            if result:
                return result

    return None



async def main():
    data = pd.read_excel('sku_ids.xlsx')

    result_df = pd.DataFrame(columns=['Код SKU', 'Бренд', 'Наименование', 'Объем', 'Единицы измерения'])

    for sku_id in data['Код SKU']:
        url = f"https://online.globus.ru/products/{sku_id}_ST/"
        async with aiohttp.ClientSession(headers={
            "User-Agent": 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3'
        }) as session:
            async with session.get(url) as response:
                parsed_json = await fetch_json(await response.text())
        print(sku_id)
        name = await fetch_name(parsed_json)
        brand = await fetch_brand(parsed_json)
        volume_ml = await find_volume_ml(parsed_json)
        volume_gram = await find_volume_gram(parsed_json)
        volume_piece = await find_volume_piece(parsed_json)
        if name:
            result_df = pd.concat([result_df, pd.DataFrame({
                'Код SKU': sku_id,
                'Наименование': name,
                'Бренд': brand,
                'Объем': [volume_ml[0] if volume_ml else volume_gram[0] if volume_gram else volume_piece[0] if volume_piece else None],
                'Единицы измерения': [volume_ml[1] if volume_ml else volume_gram[1] if volume_gram else volume_piece[1] if volume_piece else None]
            })], ignore_index=True)

    result_df.to_excel("result.xlsx", index=False)


if __name__ == "__main__":
    asyncio.run(main())
