from logger                        import new_data_add, json_load
from db_connector                  import connect_db, get_artikul_price, update_db,do_commit,db_insert
from multiprocessing               import Pool, Process
from functools                     import partial
from concurrent.futures            import as_completed
from concurrent.futures            import ThreadPoolExecutor
from requests_futures.sessions     import FuturesSession
from parser_for_db                 import same_avg_price
import time
import requests
import datetime
import re
import os

'''Добавление в корзину'''
#------ artikul - артикул товара------#  
#------ balance - баланс на карте------#
def busket_add(session, artikul, spp, balance=5000):
    wb_wh_list                  = [1733,507,3158,686,2737,117986,130744,122495,117393,1193,120762,121709]
    
    url_for_artikul_info        = 'https://wbxcatalog-ru.wildberries.ru/nm-2-card/catalog?spp={}&regions=64,75,4,38,30,33,70,68,22,31,66,40,71,82,1,80,69,48&stores=119261,122252,122256,117673,122258,122259,121631,122466,122467,122495,122496,122498,122590,122591,122592,123816,123817,123818,123820,123821,123822,124093,124094,124095,124096,124097,124098,124099,124100,124101,124583,124584,125238,125239,125240,143772,6159,507,3158,117501,120602,6158,120762,121709,124731,2737,130744,117986,1733,686,132043&pricemarginCoeff=1.0&reg=1&appType=1&offlineBonus=0&onlineBonus=0&emp=0&locale=ru&lang=ru&curr=rub&couponsGeo=12,3,18,15,21&dest=-1292731,-226149,-102269,-1029256&nm={}'
    artikul_info                = session.get(url_for_artikul_info.format(spp,artikul)).json()
    cod1S                       = str(artikul)
    characteristicId            = str(artikul_info['data']['products'][0]['sizes'][0]['optionId']) 
    priceWithCouponAndDiscount  = str(int(artikul_info['data']['products'][0]['salePriceU']/100))
    
    # Вычисляем доступное количество товара и сверяем с нашим лимитом на карте
    quantity_for_by = 0
    total_quantity  = 0
    buyer_quantity  = int(balance/int(priceWithCouponAndDiscount))
    if len(artikul_info['data']['products'][0]['sizes']) > 0: 
        for size in artikul_info['data']['products'][0]['sizes']:
            for qty in size['stocks']:
                if qty['wh'] in wb_wh_list:
                    total_quantity += qty['qty']
            if total_quantity != 0:
                characteristicId = size['optionId']
                total_quantity   = 1
                break
    else:
        for qty in artikul_info['data']['products'][0]['sizes'][0]['stocks']:
            if qty['wh'] in wb_wh_list:
                total_quantity += qty['qty']
            
    # вдруг на фс и на складах вб поэтому сначала считает сумму и решаем
    if total_quantity == 0:
        return False
    
    if buyer_quantity < 1:
        quantity_for_by = '1'
        
    elif total_quantity >= buyer_quantity:
        quantity_for_by = str(buyer_quantity)
    else:
        quantity_for_by = str(total_quantity)
    
    total_price = int(quantity_for_by) * int(priceWithCouponAndDiscount)
    params = {'cod1S':cod1S,
              'characteristicId':characteristicId,
              'priceWithCouponAndDiscount':priceWithCouponAndDiscount,
              'quantity':quantity_for_by,
              'lw':'CT',
              'targetUrl':'XS',
              'l':'SNT',
              's':'PU',
              'iid':'3',
              't':''}

    resp = session.post("https://www.wildberries.ru/product/addtobasket", data=params)
    return [characteristicId,total_price]

def buy_product(session,files, characteristicId, totalPrice):
     
    url_for_buy                                               = 'https://www.wildberries.ru/lk/basket/spa/submitorder'
    files['orderDetails.TotalPrice']                          = (None, totalPrice)
    files['orderDetails.UserBasketItems[0].CharacteristicId'] = (None, characteristicId)
    files['orderDetails.IncludeInOrder[0]']                   = (None, characteristicId)
    rep                                                       = session.post(url_for_buy,files=files)

def get_basket_info(artikul, spp, headers):
    # Добавить, получить инфу и удалить
    sess = requests.Session()
    sess.headers.update(headers)
    characteristicId = busket_add(sess,artikul, spp)
    
    
    busket_info_url  = 'https://ru-basket-api.wildberries.ru/lk/basket/data'
    rep              = sess.post(busket_info_url)
    busket_info      = rep.json()
    
    DeliveryPointId  = busket_info['value']['data']['basket']['deliveryPoint']['addressId']
    DeliveryWay      = busket_info['value']['data']['basket']['deliveryPoint']['deliveryType']
    PaymentTypeId    = busket_info['value']['data']['basket']['paymentType']['id']
    MaskedCardId     = busket_info['value']['data']['basket']['paymentType']['bankCardId']
    
    files={'orderDetails.DeliveryPointId': (None, str(DeliveryPointId)),
               'orderDetails.DeliveryWay': (None, str(DeliveryWay)), 
               'orderDetails.DeliveryPrice': (None, ''),
               'orderDetails.PaymentType.Id': (None, str(PaymentTypeId)),
               'orderDetails.MaskedCardId': (None, str(MaskedCardId)),
               'orderDetails.SberPayPhone': (None, ''),
               'orderDetails.AgreePublicOffert': (None, 'true')
               }
    
    
    delete = sess.post('https://www.wildberries.ru/lk/basket/spa/delete',data={'chrtIds':characteristicId})
    return [files, sess]
    
        
def send_info(artikul, new_price, old_price,art_info, end_time):
    TOKEN        = '2102852967:AAFx7gVnIpOuoxBPJH4DoAORcD7d0xvtcQM'
    URL          = 'https://api.telegram.org/bot'
    tg_ids       = [ 1868789819]
    URL          += TOKEN
    method       = URL + "/sendMessage"
    price_info   = "Куплен по *{}* руб. Старая цена *{}* руб. \n".format(new_price,old_price)
    product_info = "Ссылка - https://www.wildberries.ru/catalog/{}/detail.aspx?targetUrl=EX \n".format(artikul)
    artikul_info = "Артикул: *{}* \n".format(artikul)
    diff_price   = "Доп инфа для статы {}".format(art_info)
    text         = price_info + product_info + artikul_info + diff_price + ':' + end_time
    
    for use_id in tg_ids:
        r = requests.post(method, data={
             "chat_id": use_id,
             "text": text,
             "parse_mode":'markdown'
              })
        
def send_error(error):
    TOKEN        = '5001179266:AAFH1NQ3S2Xn4B0z0XeI3S9UEUIe7zpnGe4'
    URL          = 'https://api.telegram.org/bot'
    tg_ids       = [1868789819]
    URL          += TOKEN
    method       = URL + "/sendMessage"

    for use_id in tg_ids:
        r = requests.post(method, data={
             "chat_id": use_id,
             "text": error,
             "parse_mode":'markdown'
              })        

def start_artikuls_info(url, spp, count_of_pages=20):
    page_number     = 0
    artikul_list    = []
    product_dict    = {}
    diff_price_list = {}
    sort_type       = re.findall("sort=(.+)&", url)[0]
    session         = FuturesSession(executor = ThreadPoolExecutor(max_workers = 10))
    
    futures=[session.get(url.format(spp, page)) for page in range(1,count_of_pages + 1)]
    
    for future in as_completed(futures):
        resp = future.result()
        data = resp.json()
        page_number += 1
        for product in data['data']['products']:
            artikul_list.append(product['id'])
            product_dict[product['id']]    = int(product['salePriceU']/100)
            diff_price_list[product['id']] = str(product['diffPrice']) + ':' + str(page_number) + ':' + sort_type 
            
    return [artikul_list, product_dict, diff_price_list]  

def test_buy(session, files, artikul, spp, headers, balance=5000):
    wb_wh_list                  = [1733,507,3158,686,2737,117986,130744,122495,117393,1193,120762,121709]
    
    url_for_artikul_info        = 'https://wbxcatalog-ru.wildberries.ru/nm-2-card/catalog?spp={}&regions=64,75,4,38,30,33,70,68,22,31,66,40,71,82,1,80,69,48&stores=119261,122252,122256,117673,122258,122259,121631,122466,122467,122495,122496,122498,122590,122591,122592,123816,123817,123818,123820,123821,123822,124093,124094,124095,124096,124097,124098,124099,124100,124101,124583,124584,125238,125239,125240,143772,6159,507,3158,117501,120602,6158,120762,121709,124731,2737,130744,117986,1733,686,132043&pricemarginCoeff=1.0&reg=1&appType=1&offlineBonus=0&onlineBonus=0&emp=0&locale=ru&lang=ru&curr=rub&couponsGeo=12,3,18,15,21&dest=-1292731,-226149,-102269,-1029256&nm={}'
    artikul_info                = session.get(url_for_artikul_info.format(spp,artikul)).json()
    cod1S                       = str(artikul)
    characteristicId            = str(artikul_info['data']['products'][0]['sizes'][0]['optionId']) 
    priceWithCouponAndDiscount  = str(int(artikul_info['data']['products'][0]['salePriceU']/100))
    
    # Вычисляем доступное количество товара и сверяем с нашим лимитом на карте
    quantity_for_by = 0
    total_quantity  = 0
    buyer_quantity  = int(balance/int(priceWithCouponAndDiscount))
    for qty in artikul_info['data']['products'][0]['sizes'][0]['stocks']:
        if qty['wh'] in wb_wh_list:
            total_quantity += qty['qty']
            
    # вдруг на фс и на складах вб поэтому сначала считает сумму и решаем
    if total_quantity == 0:
        return False
    
    if buyer_quantity < 1:
        quantity_for_by = 1
        
    elif total_quantity >= buyer_quantity:
        quantity_for_by = buyer_quantity
    else:
        quantity_for_by = total_quantity
    
    # total_price = int(quantity_for_by) * int(priceWithCouponAndDiscount)
    
    # //// для теста тестового ( покупка одной единицы)
    if quantity_for_by == 1:
        quantity_for_by = 1
    else:
        quantity_for_by = 2
    total_price = int(quantity_for_by) * int(priceWithCouponAndDiscount)
    # 
    
    params = {'cod1S':cod1S,
              'characteristicId':characteristicId,
              'priceWithCouponAndDiscount':priceWithCouponAndDiscount,
              'quantity':quantity_for_by,
              'lw':'CT',
              'targetUrl':'XS',
              'l':'SNT',
              's':'PU',
              'iid':'3',
              't':''}

    
    url_for_buy                                               = 'https://www.wildberries.ru/lk/basket/spa/submitorder'
    files['orderDetails.TotalPrice']                          = (None, total_price)
    files['orderDetails.UserBasketItems[0].CharacteristicId'] = (None, characteristicId)
    files['orderDetails.IncludeInOrder[0]']                   = (None, characteristicId)
    session         = FuturesSession(executor = ThreadPoolExecutor(max_workers = 10))
    session.headers.update(headers)
    future1=session.post("https://www.wildberries.ru/product/addtobasket", data=params)
    time.sleep(0.03)
    future2=session.post(url_for_buy,files=files)

    
def get_data_request(db_name, url):
    config         = json_load(r'../json/config.json')
    spp_config     = json_load(r'../json/spp.json')
    spp            = spp_config['spp']
    balance        = config['balance']
    
    pages_config   = json_load(r'../json/pages.json')
    count_of_pages = pages_config['count_of_pages']
    
    min_sale       = config['min_sale']
    db_name        = config['db_name']
    min_price      = config['min_price']
    headers        = json_load(r'../json/headers.json')
    reboot_time    = time.time()
    
    count_of_errors     = 0
    connect             = connect_db(db_name)
    start_products_info = start_artikuls_info(url,spp, count_of_pages)
    start_artikuls      = set(start_products_info[0])
    start_basket_info   = get_basket_info(list(start_artikuls)[0],spp, headers)[0]
    session             = get_basket_info(list(start_artikuls)[0],spp, headers)[1]
    session_futures     = FuturesSession(executor = ThreadPoolExecutor(max_workers = 10))
    while True:
        if (time.time() - reboot_time) > 2400:
            os.system('reboot now')
        try:
            start_time  = time.time()
            page_number = 0
            sort_type   = re.findall("sort=(.+)&", url)[0]
    
            futures     = [session_futures.get(url.format(spp, page)) for page in range(1,count_of_pages + 1)]
            
            for future in as_completed(futures):
                artikul_list    = []
                products_info   = {}
                diff_price_info = {}
                resp            = future.result()
                data            = resp.json()
                page_number     += 1
                for product in data['data']['products']:
                    artikul_list.append(product['id'])
                    products_info[product['id']]    = int(product['salePriceU']/100)
                    diff_price_info[product['id']] = str(product['diffPrice']) + ':' + str(page_number) + ':' + sort_type 
                    
                    
                new_artikuls      = set(artikul_list)
    
                diff              = list(new_artikuls - start_artikuls)
                if len(diff) > 0:
                    
                    # TEST
                    # url_for_same_artikuls = 'https://www.wildberries.ru/recommendation/catalog/data?desktop=2&type=visuallysimilar&forproduct={}'
                    # session_futures       = FuturesSession(executor = ThreadPoolExecutor(max_workers = 10))
                    # session_futures.headers.update(headers)
                    # same_artikuls_futures = [session_futures.get(url_for_same_artikuls.format(product)) for product in diff]
                    # TEST
                    for product_artikul in diff:
                        
                        end_detect_time = time.time() - start_time
                        # чекаем наличие артикула в базе, если есть, то сравниваем цену и покупаем
                        artikul_price_info = get_artikul_price(connect,db_name,product_artikul)
                        
                        new_price          = products_info[product_artikul]
                        art_info           = diff_price_info[product_artikul]
                        #если 0, то артикула нет в базе
                        if artikul_price_info != 0:
                            old_price          = int(artikul_price_info[0])
                            avg_same_price     = int(artikul_price_info[1])
                            histrory_price     = int(artikul_price_info[2])
                            coeff = 1
                            if (1 - new_price/old_price) > min_sale:
                                if histrory_price != None and histrory_price != 0:
                                    coeff = new_price /  histrory_price
                                elif avg_same_price != 0:
                                    coeff = new_price / avg_same_price
                                else:
                                    coeff = new_price/old_price
                                
                            # Решение о покупке
                            if (1 -  coeff) > min_sale:
                                
                                start_buy_time = time.time()
                                order_info = test_buy(session, start_basket_info,product_artikul, spp, headers, balance)
                                if order_info != False:
                                    end_buy_time = time.time() - start_buy_time
                                    time_to_send = str(end_detect_time) + ':' + str(end_buy_time)
                                send_info(product_artikul, new_price, old_price,art_info,time_to_send)
                                # send_info(product_artikul, new_price, old_price,art_info,'test')
                            if new_price < old_price:
                                update_db(connect, db_name, product_artikul, new_price)
                                # тип логгируем для наглядности
                            new_data_add(r'../json/result.json', [str(product_artikul) + ':' + str(old_price) + ':' + str(new_price)])
                                # print(str(product_artikul) + ':' + str(old_price) + ':' + str(new_price))
                        # else:
                        #     if new_price >= min_price:
                        #         avg_same_price = same_avg_price(same_artikuls_futures[diff.index(product_artikul)], session_futures)
                        #         coeff = new_price / avg_same_price
                        #         if (1 -  coeff) > min_sale:
                                    
                        #             start_buy_time = time.time()
                        #             order_info = test_buy(session, start_basket_info,product_artikul, spp, headers, balance)
                        #             if order_info != False:
                        #                 end_buy_time = time.time() - start_buy_time
                        #                 time_to_send = str(end_detect_time) + ':' + str(end_buy_time) + ':' +' NEW PRODUCT'
                        #                 send_info(product_artikul, new_price, old_price,art_info,time_to_send)
                                    
                        #             try:
                        #                 db_insert(connect,db_name,product_artikul,new_price, avg_same_price, 0)
                        #             except:
                        #                 continue
                        #             # тип логгируем для наглядности
                        #             new_data_add(r'../json/result.json', ['NEW' + ':' + str(product_artikul) + ':' + str(old_price) + ':' + str(new_price)])
                        #             print(str(product_artikul) + ':' + str(old_price) + ':' + str(new_price))
                            
                         # добавили в стартовые артикулы
                        start_artikuls.add(product_artikul)
                        do_commit(connect)  
        except Exception as e:
            count_of_errors += 1
            text_to_tg = db_name + ":" + str(e)
            if count_of_errors % 10 == 0:
                send_error(text_to_tg)
            time.sleep(5)
            continue
        
        print("--- %s seconds ---" % (time.time() - start_time))
        
def multi_get_data():
    config          = json_load(r'../json/config.json')
    db_name         = config['db_name']  
    urls_info       = json_load(r'../json/urls.json')[db_name]
    urls_for_detect = urls_info['urls_for_detect']
    p               = Pool(processes=2)
    func            = partial(get_data_request, db_name)
    p.map(func, urls_for_detect)
