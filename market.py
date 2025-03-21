import datetime
import logging.config
from environs import Env
from seller import download_stock

import requests

from seller import divide, price_conversion

logger = logging.getLogger(__file__)


def get_product_list(page, campaign_id, access_token):
    """Получить список товаров магазина Яндекс-маркет.

        Аргументы:
            campaign_id (str): идентификатор кампании
            page (str): токен страницы
            access_token (str): токен продавца
        Возвращает:
            dict: значение из словаря с ключом "result"
        Корректное исполнение функции:
            принимает объекты формата str с именами page, campaign_id, access_token,
            делает post-запрос по url, получает значение по ключу "result".
        Неккоректное исполнение функции:
            принимает объект иного формата или неверные значения аргументов,
            при попытке сделать запрос выдаст ошибку requests.HTTPError
        """
    endpoint_url = "https://api.partner.market.yandex.ru/"
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/json",
        "Host": "api.partner.market.yandex.ru",
    }
    payload = {
        "page_token": page,
        "limit": 200,
    }
    url = endpoint_url + f"campaigns/{campaign_id}/offer-mapping-entries"
    response = requests.get(url, headers=headers, params=payload)
    response.raise_for_status()
    response_object = response.json()
    return response_object.get("result")


def update_stocks(stocks, campaign_id, access_token):
    """Обновить остатки.

        Аргументы:
            campaign_id (str): идентификатор кампании
            stocks (list): информация о товарах
            access_token (str): токен продавца
        Возвращает:
            dict: словарь с остатками.
        Корректное исполнение функции:
            принимает объекты, формата str с именами campaign_id, access_token,
            формата list с именем stocks
            делает put-запрос по url, обновляет список остатков.
        Неккоректное исполнение функции:
            принимает объект иного формата или неверные значения аргументов,
            при попытке сделать запрос выдаст ошибку requests.HTTPError или
            при вызове response.json() выдаст json.JSONDecodeError
    """
    endpoint_url = "https://api.partner.market.yandex.ru/"
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/json",
        "Host": "api.partner.market.yandex.ru",
    }
    payload = {"skus": stocks}
    url = endpoint_url + f"campaigns/{campaign_id}/offers/stocks"
    response = requests.put(url, headers=headers, json=payload)
    response.raise_for_status()
    response_object = response.json()
    return response_object


def update_price(prices, campaign_id, access_token):
    """Обновить цены.

            Аргументы:
                campaign_id (str): идентификатор кампании
                prices (list): список цен
                access_token (str): токен продавца
            Возвращает:
                dict: словарь с остатками.
            Корректное исполнение функции:
                принимает объекты, формата str с именами campaign_id, access_token,
                формата list с именем prices
                делает post-запрос по url, отправляет список цен.
            Неккоректное исполнение функции:
                принимает объект иного формата или неверные значения аргументов,
                при попытке сделать запрос выдаст ошибку requests.HTTPError или
                при вызове response.json() выдаст json.JSONDecodeError
        """
    endpoint_url = "https://api.partner.market.yandex.ru/"
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/json",
        "Host": "api.partner.market.yandex.ru",
    }
    payload = {"offers": prices}
    url = endpoint_url + f"campaigns/{campaign_id}/offer-prices/updates"
    response = requests.post(url, headers=headers, json=payload)
    response.raise_for_status()
    response_object = response.json()
    return response_object


def get_offer_ids(campaign_id, market_token):
    """Получить артикулы товаров Яндекс маркета

            Аргументы:
                campaign_id (str): идентификатор кампании
                market_token (str): токен магазина
            Возвращает:
                offer_ids (list) : список с артикулами
            Корректное исполнение функции:
                принимает объекты, формата str с именами campaign_id, market_token,
                получает список артикулов offer_ids.
            Неккоректное исполнение функции:
                принимает объект иного формата или неверные значения аргументов в функции get_product_list,
                при попытке сделать запрос выдаст ошибку requests.HTTPError или
                при вызове response.json() выдаст json.JSONDecodeError
        """
    page = ""
    product_list = []
    while True:
        some_prod = get_product_list(page, campaign_id, market_token)
        product_list.extend(some_prod.get("offerMappingEntries"))
        page = some_prod.get("paging").get("nextPageToken")
        if not page:
            break
    offer_ids = []
    for product in product_list:
        offer_ids.append(product.get("offer").get("shopSku"))
    return offer_ids


def create_stocks(watch_remnants, offer_ids, warehouse_id):
    """Получить остатки

        Аргументы:
            watch_remnants (dict) : словарь, с остатками часов;
            offer_ids (list) : список артикулов;
            warehouse_id (str) : идентификатор
        Возвращает:
            stocks (list) : список с остатками.
        Корректное исполнение функции:
            принимает объекты, формата dict с именем watch_remnants, формата list с именем offer_ids,
            формата str с именем warehouse_id
            сортирует остатки,
            получает stocks с остатками.
        Неккоректное исполнение функции:
            принимает объект иного формата или неверные
            значения аргументов в функции get_product_list,
            при попытке сделать запрос выдаст ошибку requests.HTTPError или
            при вызове response.json() выдаст json.JSONDecodeError
    """
    # Уберем то, что не загружено в market
    stocks = list()
    date = str(datetime.datetime.utcnow().replace(microsecond=0).isoformat() + "Z")
    for watch in watch_remnants:
        if str(watch.get("Код")) in offer_ids:
            count = str(watch.get("Количество"))
            if count == ">10":
                stock = 100
            elif count == "1":
                stock = 0
            else:
                stock = int(watch.get("Количество"))
            stocks.append(
                {
                    "sku": str(watch.get("Код")),
                    "warehouseId": warehouse_id,
                    "items": [
                        {
                            "count": stock,
                            "type": "FIT",
                            "updatedAt": date,
                        }
                    ],
                }
            )
            offer_ids.remove(str(watch.get("Код")))
    # Добавим недостающее из загруженного:
    for offer_id in offer_ids:
        stocks.append(
            {
                "sku": offer_id,
                "warehouseId": warehouse_id,
                "items": [
                    {
                        "count": 0,
                        "type": "FIT",
                        "updatedAt": date,
                    }
                ],
            }
        )
    return stocks


def create_prices(watch_remnants, offer_ids):
    """Составление цен на товары.

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
            значения аргументов в функциях get_product_list,
            при попытке сделать запрос выдаст ошибку requests.HTTPError или
            при вызове response.json() выдаст json.JSONDecodeError
    """
    prices = []
    for watch in watch_remnants:
        if str(watch.get("Код")) in offer_ids:
            price = {
                "id": str(watch.get("Код")),
                # "feed": {"id": 0},
                "price": {
                    "value": int(price_conversion(watch.get("Цена"))),
                    # "discountBase": 0,
                    "currencyId": "RUR",
                    # "vat": 0,
                },
                # "marketSku": 0,
                # "shopSku": "string",
            }
            prices.append(price)
    return prices


async def upload_prices(watch_remnants, campaign_id, market_token):
    """Выгрузка цен товаров.

        Аргументы:
            watch_remnants (dict): остатки часов
            campaign_id (str): идентификатор
            market_token (str): токен магазина
        Возвращает:
            prices (list): cписок цен на товары
        Корректное исполнение функции:
            принимает объекты, формата str с именами campaign_id, market_token, формата dict с именем watch_remnants,
            делает post-запрос по url функции update_price, получает list с ценами.
        Неккоректное исполнение функции:
            принимает объект иного формата или неверные значения аргументов в функции update_price,
            при попытке сделать запрос выдаст ошибку requests.HTTPError
        """
    offer_ids = get_offer_ids(campaign_id, market_token)
    prices = create_prices(watch_remnants, offer_ids)
    for some_prices in list(divide(prices, 500)):
        update_price(some_prices, campaign_id, market_token)
    return prices


async def upload_stocks(watch_remnants, campaign_id, market_token, warehouse_id):
    """Выгрузка остатков товаров.

        Аргументы:
            watch_remnants (dict): остатки часов
            campaign_id (str): идентификатор
            market_token (str): токен магазина
            warehouse_id (str): идентификатор
        Возвращает:
            stocks (list): cписок остатков
            not_empty (list) : список ?
        Корректное исполнение функции:
            принимает объекты, формата str с именами campaign_id, market_token, warehouse_id,
            формата dict с именем watch_remnants,
            делает post-запрос по url функции update_stocks, получает list с остатками.
        Неккоректное исполнение функции:
            принимает объект иного формата или неверные значения аргументов в функции update_stocks,
            при попытке сделать запрос выдаст ошибку requests.HTTPError или
            при вызове response.json() выдаст json.JSONDecodeError
    """
    offer_ids = get_offer_ids(campaign_id, market_token)
    stocks = create_stocks(watch_remnants, offer_ids, warehouse_id)
    for some_stock in list(divide(stocks, 2000)):
        update_stocks(some_stock, campaign_id, market_token)
    not_empty = list(
        filter(lambda stock: (stock.get("items")[0].get("count") != 0), stocks)
    )
    return not_empty, stocks


def main():
    env = Env()
    market_token = env.str("MARKET_TOKEN")
    campaign_fbs_id = env.str("FBS_ID")
    campaign_dbs_id = env.str("DBS_ID")
    warehouse_fbs_id = env.str("WAREHOUSE_FBS_ID")
    warehouse_dbs_id = env.str("WAREHOUSE_DBS_ID")

    watch_remnants = download_stock()
    try:
        # FBS
        offer_ids = get_offer_ids(campaign_fbs_id, market_token)
        # Обновить остатки FBS
        stocks = create_stocks(watch_remnants, offer_ids, warehouse_fbs_id)
        for some_stock in list(divide(stocks, 2000)):
            update_stocks(some_stock, campaign_fbs_id, market_token)
        # Поменять цены FBS
        upload_prices(watch_remnants, campaign_fbs_id, market_token)

        # DBS
        offer_ids = get_offer_ids(campaign_dbs_id, market_token)
        # Обновить остатки DBS
        stocks = create_stocks(watch_remnants, offer_ids, warehouse_dbs_id)
        for some_stock in list(divide(stocks, 2000)):
            update_stocks(some_stock, campaign_dbs_id, market_token)
        # Поменять цены DBS
        upload_prices(watch_remnants, campaign_dbs_id, market_token)
    except requests.exceptions.ReadTimeout:
        print("Превышено время ожидания...")
    except requests.exceptions.ConnectionError as error:
        print(error, "Ошибка соединения")
    except Exception as error:
        print(error, "ERROR_2")


if __name__ == "__main__":
    main()
