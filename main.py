from asyncio import Semaphore
from datetime import timedelta

from bs4 import BeautifulSoup
import pandas as pd
import aiohttp
import asyncio
import json
import openpyxl
from win32ctypes.pywin32.pywintypes import datetime


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


async def fetch_product_data(session: aiohttp.ClientSession, semaphore: Semaphore, sku_id: str):
    async with semaphore:
        url = f"https://online.globus.ru/products/{sku_id}_ST/"
        try:
            async with session.get(url) as response:
                text = await response.text()
                json_data = await fetch_json(text)
                return {'sku_id': sku_id, 'data': json_data}
        except Exception as e:
            print(f"Ошибка при обработке {sku_id}: {str(e)}")
            return {'sku_id': sku_id, 'data': None}


async def process_product_data(product_data):
    sku_id = product_data['sku_id']
    json_data = product_data['data']

    if not json_data:
        return None

    name = await fetch_name(json_data)
    if not name:
        return None

    brand = await fetch_brand(json_data)
    volume_ml = await find_volume_ml(json_data)
    volume_gram = await find_volume_gram(json_data)
    volume_piece = await find_volume_piece(json_data)

    return {
        'Код SKU': sku_id,
        'Наименование': name,
        'Бренд': brand,
        'Объем': volume_ml[0] if volume_ml else volume_gram[0] if volume_gram else volume_piece[
            0] if volume_piece else None,
        'Единицы измерения': volume_ml[1] if volume_ml else volume_gram[1] if volume_gram else volume_piece[
            1] if volume_piece else None
    }


async def main():
    start_time = datetime.now()
    print("Старт программы", datetime.now())
    data = pd.read_excel('sku_ids.xlsx')
    # data = data.head(1000)
    result_df = pd.DataFrame(columns=['Код SKU', 'Бренд', 'Наименование', 'Объем', 'Единицы измерения'])

    # Ограничиваем количество одновременных запросов
    semaphore = Semaphore(1000)

    async with aiohttp.ClientSession(headers={
        "User-Agent": 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3'
    }) as session:
        # Этап 1: Пакетное получение всех JSON
        print("Получаем данные с сервера...")
        fetch_tasks = [fetch_product_data(session, semaphore, sku_id) for sku_id in data['Код SKU']]
        all_product_data = await asyncio.gather(*fetch_tasks)

        # Этап 2: Параллельная обработка полученных данных
        print("Обрабатываем полученные данные...")
        process_tasks = [process_product_data(one_product_data) for one_product_data in all_product_data]
        processed_results = await asyncio.gather(*process_tasks)

        # Фильтруем None результаты и добавляем в DataFrame
        valid_results = [r for r in processed_results if r is not None]
        if valid_results:
            result_df = pd.concat([result_df, pd.DataFrame(valid_results)], ignore_index=True)

    # Сохраняем результаты
    result_df.to_excel("result.xlsx", index=False)
    end_time = datetime.now()
    print(f"Готово! Сохранено {len(result_df)} записей. Время работы {end_time - start_time}.")


if __name__ == "__main__":
    asyncio.run(main())
