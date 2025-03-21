import io
import logging.config
import os
import re
import zipfile
from environs import Env

import pandas as pd
import requests

logger = logging.getLogger(__file__)


def get_product_list(last_id, client_id, seller_token):
    """Получение списка товаров из интернет-магазина OZON.

    Аргументы:
        last_id (str) : строка, ?;
        client_id (str) : строка, идентификатор клиента;
        seller_token (str) : строка, токен продавца.
    Возвращает:
        dict: значение из словаря с ключом "result"
    Корректное исполнение функции:
        принимает объекты формата str с именами last_id, client_id, seller_token,
        делает post-запрос по url, получает значение по ключу "result".
    Неккоректное исполнение функции:
        принимает объект иного формата или неверные значения аргументов,
        при попытке сделать запрос выдаст ошибку requests.HTTPError
    """
    url = "https://api-seller.ozon.ru/v2/product/list"
    headers = {
        "Client-Id": client_id,
        "Api-Key": seller_token,
    }
    payload = {
        "filter": {
            "visibility": "ALL",
        },
        "last_id": last_id,
        "limit": 1000,
    }
    response = requests.post(url, json=payload, headers=headers)
    response.raise_for_status()
    response_object = response.json()
    return response_object.get("result")


def get_offer_ids(client_id, seller_token):
    """Получить артикулы товаров магазина озон

    Аргументы:
        client_id (str) : строка, идентификатор клиента;
        seller_token (str) : строка, токен продавца.
    Возвращает:
        offer_ids (list) : список артикулов.
    Корректное исполнение функции:
        Получает список товаров из функции get_product_list,
        из него получаем артикулы товаров.
    Неккоректное исполнение функции:
        принимает объект иного формата или неверные значения аргументов,
        при попытке сделать запрос выдаст ошибку requests.HTTPError в get_product_list,
        если передан объект None, метод extend() выбросит исключение TypeError
    """
    last_id = ""
    product_list = []
    while True:
        some_prod = get_product_list(last_id, client_id, seller_token)
        product_list.extend(some_prod.get("items"))
        total = some_prod.get("total")
        last_id = some_prod.get("last_id")
        if total == len(product_list):
            break
    offer_ids = []
    for product in product_list:
        offer_ids.append(product.get("offer_id"))
    return offer_ids


def update_price(prices: list, client_id, seller_token):
    """Обновить цены товаров

    Аргументы:
        prices (list) : список цен;
        client_id (str) : строка, идентификатор клиента;
        seller_token (str) : строка, токен продавца.
    Возвращает:
        dict : словарь, с обновленными ценами.
    Корректное исполнение функции:
        принимает объекты, формата str с именами client_id, seller_token, формата list с именем prices,
        делает post-запрос по url, получает dict с обновленными ценами.
    Неккоректное исполнение функции:
        принимает объект иного формата или неверные значения аргументов,
        при попытке сделать запрос выдаст ошибку requests.HTTPError или
        при вызове response.json() выдаст json.JSONDecodeError
    """
    url = "https://api-seller.ozon.ru/v1/product/import/prices"
    headers = {
        "Client-Id": client_id,
        "Api-Key": seller_token,
    }
    payload = {"prices": prices}
    response = requests.post(url, json=payload, headers=headers)
    response.raise_for_status()
    return response.json()


def update_stocks(stocks: list, client_id, seller_token):
    """Обновить остатки

    Аргументы:
        stocks (list) : список остатков
        client_id (str) : строка, идентификатор клиента
        seller_token (str) : строка, токен продавца
    Возвращает:
        dict : словарь, с обновленными остатками.
    Корректное исполнение функции:
        принимает объекты, формата str с именами client_id, seller_token, формата list с именем stocks,
        делает post-запрос по url, получает dict с обновленными остатками.
    Неккоректное исполнение функции:
        принимает объект иного формата или неверные значения аргументов,
        при попытке сделать запрос выдаст ошибку requests.HTTPError или
        при вызове response.json() выдаст json.JSONDecodeError
    """
    url = "https://api-seller.ozon.ru/v1/product/import/stocks"
    headers = {
        "Client-Id": client_id,
        "Api-Key": seller_token,
    }
    payload = {"stocks": stocks}
    response = requests.post(url, json=payload, headers=headers)
    response.raise_for_status()
    return response.json()


def download_stock():
    """Скачать файл ostatki с сайта casio

    Возвращает:
        dict : словарь, с остатками часов
    Корректное исполнение функции:
        делает get-запрос по url, получает из архива excel-файл с остатками и формирует список,
        после удаляет excel-файл
    Неккоректное исполнение функции:
        ошибка со стороны сервера, при попытке сделать запрос выдаст ошибку requests.HTTPError
    """
    # Скачать остатки с сайта
    casio_url = "https://timeworld.ru/upload/files/ostatki.zip"
    session = requests.Session()
    response = session.get(casio_url)
    response.raise_for_status()
    with response, zipfile.ZipFile(io.BytesIO(response.content)) as archive:
        archive.extractall(".")
    # Создаем список остатков часов:
    excel_file = "ostatki.xls"
    watch_remnants = pd.read_excel(
        io=excel_file,
        na_values=None,
        keep_default_na=False,
        header=17,
    ).to_dict(orient="records")
    os.remove("./ostatki.xls")  # Удалить файл
    return watch_remnants


def create_stocks(watch_remnants, offer_ids):
    """Получить остатки

        Аргументы:
            watch_remnants (dict) : словарь, с остатками часов;
            offer_ids (list) : список артикулов;
        Возвращает:
            stocks (list) : список с остатками.
        Корректное исполнение функции:
            принимает объекты, формата dict с именем watch_remnants, формата list с именем offer_ids,
            сортирует остатки,
            получает stocks с остатками.
        Неккоректное исполнение функции:
            принимает объект иного формата или неверные
            значения аргументов в функциях get_product_list и download_stock,
            при попытке сделать запрос выдаст ошибку requests.HTTPError
        """
    # Уберем то, что не загружено в seller
    stocks = []
    for watch in watch_remnants:
        if str(watch.get("Код")) in offer_ids:
            count = str(watch.get("Количество"))
            if count == ">10":
                stock = 100
            elif count == "1":
                stock = 0
            else:
                stock = int(watch.get("Количество"))
            stocks.append({"offer_id": str(watch.get("Код")), "stock": stock})
            offer_ids.remove(str(watch.get("Код")))
    # Добавим недостающее из загруженного:
    for offer_id in offer_ids:
        stocks.append({"offer_id": offer_id, "stock": 0})
    return stocks


def create_prices(watch_remnants, offer_ids):
    """Составление цен на товары для Озона, которая равна цене магазина Casio.

        Аргументы:
            watch_remnants (dict): остатки часов
            offer_ids (list): список артикулов
        Возвращает:
            prices (list): список цен на товары.
        Корректное исполнение функции:
            принимает объекты, формата dict с именем watch_remnants, формата list с именем offer_ids,
            получает prices с ценами.
        Неккоректное исполнение функции:
            принимает объект иного формата или неверные
            значения аргументов в функциях get_product_list и download_stock,
            при попытке сделать запрос выдаст ошибку requests.HTTPError
    """
    prices = []
    for watch in watch_remnants:
        if str(watch.get("Код")) in offer_ids:
            price = {
                "auto_action_enabled": "UNKNOWN",
                "currency_code": "RUB",
                "offer_id": str(watch.get("Код")),
                "old_price": "0",
                "price": price_conversion(watch.get("Цена")),
            }
            prices.append(price)
    return prices


def price_conversion(price: str) -> str:
    """Вспомогательная функция для преобразования строки для функции create_prices.

    Аргументы:
        price (str) : строка, вида "5'990.00 руб.";
    Возвращает:
        str: строка, вида "5990".
    Корректное исполнение функции:
        принимает объект формата str с именем price,
        преобразовывает к другому виду и возвращает также str.
    Неккоректное исполнение функции:
        принимает объект иного формата, возникает ошибка AttributeError.
    """
    return re.sub("[^0-9]", "", price.split(".")[0])


def divide(lst: list, n: int):
    """Вспомогательная функция для разделения списка lst на части по n элементов.

    Аргументы:
        list (str): список
        n (int): количество элементов в одной части.
    Возвращает:
        list: Список состоящий из частей по n элементов.
    """
    for i in range(0, len(lst), n):
        yield lst[i : i + n]


async def upload_prices(watch_remnants, client_id, seller_token):
    """Выгрузка цен товаров в магазине Озон.

        Аргументы:
            watch_remnants (dict): остатки часов
            client_id (str): идентификатор клиента
            seller_token (str): токен продавца
        Возвращает:
            list: cписок цен на товары
        Корректное исполнение функции:
            принимает объекты, формата str с именами client_id, seller_token, формата dict с именем watch_remnants,
            делает post-запрос по url функции update_price, получает list с ценами.
        Неккоректное исполнение функции:
            принимает объект иного формата или неверные значения аргументов в функции update_price,
            при попытке сделать запрос выдаст ошибку requests.HTTPError
    """
    offer_ids = get_offer_ids(client_id, seller_token)
    prices = create_prices(watch_remnants, offer_ids)
    for some_price in list(divide(prices, 1000)):
        update_price(some_price, client_id, seller_token)
    return prices


async def upload_stocks(watch_remnants, client_id, seller_token):
    """Выгрузка остатков товаров в магазине Озон.

        Аргументы:
            watch_remnants (dict): остатки часов
            client_id (str): идентификатор клиента
            seller_token (str): токен продавца
        Возвращает:
            stocks (list): cписок остатков
            not_empty (list) : список ?
        Корректное исполнение функции:
            принимает объекты, формата str с именами client_id, seller_token, формата dict с именем watch_remnants,
            делает post-запрос по url функции update_stocks, получает list с остатками.
        Неккоректное исполнение функции:
            принимает объект иного формата или неверные значения аргументов в функции update_stocks,
            при попытке сделать запрос выдаст ошибку requests.HTTPError
    """
    offer_ids = get_offer_ids(client_id, seller_token)
    stocks = create_stocks(watch_remnants, offer_ids)
    for some_stock in list(divide(stocks, 100)):
        update_stocks(some_stock, client_id, seller_token)
    not_empty = list(filter(lambda stock: (stock.get("stock") != 0), stocks))
    return not_empty, stocks


def main():
    env = Env()
    seller_token = env.str("SELLER_TOKEN")
    client_id = env.str("CLIENT_ID")
    try:
        offer_ids = get_offer_ids(client_id, seller_token)
        watch_remnants = download_stock()
        # Обновить остатки
        stocks = create_stocks(watch_remnants, offer_ids)
        for some_stock in list(divide(stocks, 100)):
            update_stocks(some_stock, client_id, seller_token)
        # Поменять цены
        prices = create_prices(watch_remnants, offer_ids)
        for some_price in list(divide(prices, 900)):
            update_price(some_price, client_id, seller_token)
    except requests.exceptions.ReadTimeout:
        print("Превышено время ожидания...")
    except requests.exceptions.ConnectionError as error:
        print(error, "Ошибка соединения")
    except Exception as error:
        print(error, "ERROR_2")


if __name__ == "__main__":
    main()
