from logger                        import json_load, json_dump
from db_connector                  import create_db, db_insert, do_commit, get_artikul_price, update_db, connect_db, update_db_price_history, update_db_same_price
from concurrent.futures            import as_completed
from concurrent.futures            import ThreadPoolExecutor
from requests_futures.sessions     import FuturesSession
import requests


def same_avg_price(same_future, session_futures):
    
    url_for_artikuls      = 'https://card.wb.ru/cards/list?spp=19&regions=75,64,4,38,30,33,70,68,71,22,31,66,40,82,1,80,69,48&stores=117673,122258,122259,125238,125239,125240,6159,507,3158,117501,120602,6158,120762,121709,124731,2737,130744,117986,1733,686,132043&pricemarginCoeff=1.0&reg=1&appType=1&offlineBonus=0&onlineBonus=0&emp=0&locale=ru&lang=ru&curr=rub&couponsGeo=12,3,18,15,21&dest=-1029256,-102269,-226149,-1292731&nm={}'

    
    try:
        rep                   = same_future.result()
        
        same_info             = rep.json()
        count_of_artikuls     = 0
        artikuls_for_query    = []
        for same_artikul in same_info:
         
            if count_of_artikuls == 250:
                break 
            artikuls_for_query.append(str(same_artikul))
            count_of_artikuls += 1
        
        # rep          = session_futures.get(url_for_artikuls.format(";".join(artikuls_for_query)))
        # nm_info      = rep.result().json()
        rep          = requests.get(url_for_artikuls.format(";".join(artikuls_for_query)))
        nm_info      = rep.json()
    
        total_price = 0
        for product in nm_info['data']['products']:
            try:
                total_price +=  int(product['salePriceU']/100)
            except:
                continue
            
        avg_price = total_price/count_of_artikuls
        return int(avg_price)
    except:
        return 0

def history_avg_price(history_future):

    price_history      = 0
    price_history_resp = history_future.result()
    # print(price_history_resp.text)
    try:
        history = price_history_resp.json()
        
        for date in history:
            price_history += date['price']['RUB']/100    
        price_history =  int(price_history/len(history))
        return price_history
    except:
        return 0
    
    # print(price_history_resp.text)
    # if len(price_history_resp.text) != 0:
    #     history = price_history_resp.json()
    #     for date in history:
    #         price_history += date['price']['RUB']/100    
    #     price_history =  int(price_history/len(history))
    #     return price_history
    # else:
    #     return 0
    
'''Парсер карточек на странице и добавление артикула и цены в базу'''
#------ driver               -  selenium driver                                                                           ------#
#------ db_connect           -  подключение к базе                                                                        ------#
#------ min_price            -  минимальная цена                                                                          ------#    
#------ current_page_number  -  номер текущей страницы, если 1000-ая стр то возвращаем флаг и последнюю цену на странице  ------#        
def page_parser_for_db(url,connect, min_price, current_page_number, db_name, headers):
    
    price_flag    = False
    artikuls_list = []
    price         = 0
    html          = requests.get(url)
    
    same_headers = {'Host': 'in-visual-similar.wildberries.ru',
                           'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:94.0) Gecko/20100101 Firefox/94.0',
                           'Accept': '*/*',
                           'Accept-Language': 'ru-RU,ru;q=0.8,en-US;q=0.5,en;q=0.3',
                           'Accept-Encoding': 'gzip, deflate, br',
                           'Referer':'https://www.wildberries.ru/lk/basket',
                           'x-requested-with': 'XMLHttpRequest',
                           'x-spa-version': '9.3.17',
                           'Connection': 'keep-alive',
                           'Cookie': 'BasketUID=b35d466a-57e4-4bb8-a969-b7b3c4ae4a96; ___wbu=019f94a6-6568-4c62-ab1c-712dab6ccfe0.1630958013; _gcl_au=1.1.1088428129.1630958015; _wbauid=9566196431630958015; _ga=GA1.2.218985208.1630958019; _pk_id.1.034e=49d7ead5a0366f72.1630958081.105.1636670843.1636669887.; _pk_ref.1.034e=%5B%22%22%2C%22%22%2C1636669887%2C%22https%3A%2F%2Fimages.wbstatic.net%2F%22%5D; __ac=true; _ga_0QDZ2491FF=GS1.1.1636124957.2.1.1636124976.0; _gid=GA1.2.1596438743.1635622834; _hjid=df8bf581-56b4-4c3b-9456-006af9d4e4fe; SL_C_23361dd035530_VID=gCxX3zyGTq; SL_C_23361dd035530_KEY=2aae3a30b09b0e4f50c63218dddf51890470a1d3; WILDAUTHNEW_V3=22228FDF0D4B7E34E995822D17856CEED95845559B22FD66B6970AADA87A2D7AFD0DB8444AA9E31089D8E3C6D6FEFDB97616AF1236EEBEBDCF6BE93BE2E6317328BF1B20AA2CE006F1781C655DCBB1E9F4E6C4AB87062D0C652AE9040E7EF9EBBA25267CC53D9BB12953297539088B2710DC7584D307DBFC259A3465101C8C05D00F1DD9EC3A6FA8C6516320B5E3783EA5897C7A72C3946E1EFF91DF539946E9C0CB1FC55E7FBE9D698D345C0A168C360917D43671B29933A8D74B35C391AFF4F5D8740A1FB84AFA77CA661DC85659914E8B1A35914F8A47CF81F0EA4ED6B75BB206E191C1B921CBC0945C0BDD46B95F5F0964E93320CEC655B286AF1679EEE20E29454BA24D33D75E23859A8D4E39C19BB25E475A732E677232B6512C8E976A922981F4; __wbl=cityId%3D0%26regionId%3D0%26city%3D%D0%B3%20%D0%9C%D0%BE%D1%81%D0%BA%D0%B2%D0%B0%2C%20%D0%9D%D0%B8%D0%BA%D1%83%D0%BB%D0%B8%D0%BD%D1%81%D0%BA%D0%B0%D1%8F%20%D0%A3%D0%BB%D0%B8%D1%86%D0%B0%2015%D0%BA1%26phone%3D88001007505%26latitude%3D55%2C66804%26longitude%3D37%2C456122%26src%3D1; route=1e8664b7b52b2ff970d291c1112a8610ab665110; __bsa=basket-ru-36; __catalogOptions=Sort%3APriceup%26CardSize%3Ac516x688; um=uid%3Dw7TDssOkw7PCu8K0wrbCuMKzwrbCtMK3wrU%253d%3Aproc%3D0%3Aehash%3Dd41d8cd98f00b204e9800998ecf8427e; ncache=119261_122252_122256_117673_122258_122259_121631_122466_122467_122495_122496_122498_122590_122591_122592_123816_123817_123818_123820_123821_123822_124093_124094_124095_124096_124097_124098_124099_124100_124101_124583_124584_125238_125239_125240_143772_6159_507_3158_117501_120602_6158_120762_121709_124731_2737_130744_117986_1733_686_132043%3B75_64_4_38_30_33_70_68_71_22_31_66_40_82_1_80_69_48%3B1.0--%3B12_3_18_15_21%3B%3BSort%3APriceup%26CardSize%3Ac516x688%3Btrue%3B-1292731_-226149_-102269_-1029256; access_token=eyJhbGciOiJSUzI1NiIsImtpZCI6IlpkZUJNOG5xb0RCd3N4RkdnMjM5a1N4N1pZY2xncTZNWjVPSXVVRGdiSXciLCJ0eXAiOiJKV1QifQ.eyJleHAiOjE2MzY3NDkxOTAsImlhdCI6MTYzNjY2Mjc5MCwicm9sZXMiOlsidXNlciJdLCJ1c2VyX2lkIjo1NzkyNzU2NH0.XPpdfpj5IksA5xldvnWq0-ruu7fK94F1yT8RIIvaAXlQuBss13BILSrY7YavVW-t0JyodeamlNTnAimS3yEcx0wA6J_trQIKJEngVhWgIV9IWKOu6mDNCFRT06My-76V3sc3pW0u302lBG3S4YleSLEXgwsH6K5TzF8VnAOPJPaL84OQOWG73DcJkTikntE3nNFeJA6GtgxwZTYGjv-Nabpee3ne-MUUwymQSGCy5y6S1-ZwhKU3KZPEXA3XBUTb23BaqV0ZnjmZTjHqt8pXpOvBrRGN8rxsaB90uK6zmT_PoY3GHyQahRddDbAHjtUXIPjZFyObZM1OOMC7sd_LLw; __store=119261_122252_122256_117673_122258_122259_121631_122466_122467_122495_122496_122498_122590_122591_122592_123816_123817_123818_123820_123821_123822_124093_124094_124095_124096_124097_124098_124099_124100_124101_124583_124584_125238_125239_125240_143772_6159_507_3158_117501_120602_6158_120762_121709_124731_2737_130744_117986_1733_686_132043; __region=75_64_4_38_30_33_70_68_71_22_31_66_40_82_1_80_69_48; __pricemargin=1.0--; __cpns=12_3_18_15_21; __sppfix=; __dst=-1292731_-226149_-102269_-1029256; ___wbs=f4c13883-cff1-49d6-af2b-b7799d432c1d.1636669859; _pk_ses.1.034e=*',
                           'Sec-Fetch-Dest': 'empty',
                           'Sec-Fetch-Mode': 'cors',
                           'Sec-Fetch-Site': 'same-origin',
                           'TE': 'trailers'}
    try:
        data      = html.json()
    except:
        return 'end_of_artikuls'
    if len(data['data']['products']) == 0:
        return 'end_of_artikuls'
    
    
    for product in data['data']['products']:
        
       
        
        price          = int(product['salePriceU']/100)
        artikul        = product['id']
        # print(str(artikul)+':'+str(price)) 
        artikuls_list.append([artikul , price])
        if price < min_price:
            price_flag = True
            break
        
    if price_flag == False:
        url_for_price_history = 'https://wbx-content-v2.wbstatic.net/price-history/{}.json?locale=ru'
        url_for_same_artikuls = 'https://in-visual-similar.wildberries.ru/?nm={}'
        session_futures_history = FuturesSession(executor = ThreadPoolExecutor(max_workers = 10))
        session_futures_same    = FuturesSession(executor = ThreadPoolExecutor(max_workers = 10))
        price_history_futures = [session_futures_history.get(url_for_price_history.format(product[0])) for product in artikuls_list]
        session_futures_same.headers.update(same_headers)
        same_artikuls_futures = [session_futures_same.get(url_for_same_artikuls.format(product[0])) for product in artikuls_list]
            
        for product in artikuls_list:
            history_price  = 0
            avg_same_price = 0
            
            # добавляем в базу, если есть такой артикул, то обновляем цену
            old_price = get_artikul_price(connect, db_name, product[0])
            
            if old_price == 0:
                
                history_price = history_avg_price(price_history_futures[artikuls_list.index(product)])
                if history_price == 0:
                    try:
                        avg_same_price = same_avg_price(same_artikuls_futures[artikuls_list.index(product)], session_futures_same)
                    except:
                        avg_same_price = 0
                        
                        
                if (history_price != 0 and history_price < min_price) or (avg_same_price != 0 and avg_same_price < min_price):
                    continue
                
                db_insert(connect,db_name,product[0],product[1], avg_same_price, history_price)
                print(str(product[0]) + ':' + str(product[1])+ ':' + str(avg_same_price)+ ':' + str(history_price))
            else:
                print(str(product[0]) + ':' + str(old_price[0])+ ':' + str(old_price[1])+ ':' + str(old_price[2]))
                if old_price[0] > product[1]:
                    update_db(connect, db_name, product[0], product[1])
                if old_price[2] == 0:
                    
                    history_price = history_avg_price(price_history_futures[artikuls_list.index(product)])
                    print(history_price)
                    if history_price > 0 :
                        update_db_price_history(connect, db_name, product[0], history_price)
                if history_price == 0 and old_price[1] == 0:
                    avg_same_price = same_avg_price(same_artikuls_futures[artikuls_list.index(product)], same_artikuls_futures)
                    if avg_same_price > 0 :
                        update_db_same_price(connect, db_name, product[0], avg_same_price)
    if current_page_number % 30== 0:
        return [price_flag, price]
    else:
        return price_flag
    
        
def wb_parser():    
    config         = json_load(r'../json/config.json')
    spp_config     = json_load(r'../json/spp.json')
    spp            = spp_config['spp']
    db_name        = config['db_name']
    min_price      = config['min_price']
    
    
    urls_info      = json_load(r'../json/urls.json')[db_name]
    subject_list   = urls_info['subjects_list']
    url_for_pars   = urls_info['url_for_pars']
    url_for_brands = urls_info['url_for_brands']
    
    headers        = json_load(r'../json/headers.json')
    
    last_pars_info = json_load(r'../json/last_save_info.json')
    if len(last_pars_info['subjects']) != 0:
        subjects = last_pars_info['subjects']
    else:
        subjects = subject_list
    
    url_for_pars += '&page={}&sort=pricedown&priceU={}00;{}00&fbrand={}'
    

    for subject in subjects:
        connect   = create_db(db_name) 
        #  для каждого сабджекта получаем все бренды
        html      = requests.get(url_for_brands.format(subject, spp))
        data      = html.json()
        brand_ids = []
        
        if len(last_pars_info['brands']) != 0 and subjects.index(subject) == 0:
            brand_ids = last_pars_info['brands']
        else:
            for brand in data['data']['filters'][0]['items']:
                brand_ids.append(brand['id'])
        
                 
        for brand_id in brand_ids:
            print(str(brand_ids.index(brand_id) + 1) + '/' +str(len(brand_ids)) + '('+ str(subjects.index(subject)) + ':' + str(len(subjects)) +'), ' + str(brand_id)+ ':' + str(subject))
            
            # max_price         =  int(data['data']['filters'][1]['maxPriceU']/100)
            max_price = 1000000
            start_page_number = 1
            
            if max_price < min_price:
                continue
            
            while True:
                if start_page_number  > 30: 
                    if start_page_number % 30 == 0:
                        new_page_number = 30
                    else:
                        coef = int(start_page_number/30)
                        new_page_number = start_page_number - coef*30
            
                    url_for_request = url_for_pars.format(spp, subject, str(new_page_number), min_price, max_price,brand_id)
                else:
                    url_for_request = url_for_pars.format(spp, subject, str(start_page_number), min_price, max_price,brand_id)
                                 
                price_info = page_parser_for_db(url_for_request, connect, min_price,start_page_number, db_name, headers)
                
                    
                if price_info == 'end_of_artikuls':
                    break
                        
                        #если 1000ая страница, то возвращаем минимальную цену с нее и флаг значений меньше минимально установленной цены
                if start_page_number % 30 == 0:
                    if max_price == price_info[1] or (max_price - price_info[1]) < 100:
                        price_check = price_info[0]
                        max_price   = price_info[1] - 100
                    else:
                        price_check = price_info[0]
                        max_price   = price_info[1]
                else:
                    #
                    price_check = price_info
                    
                #если цена ниже указанной, то прекращаем парсить раздел и переходим к следующему
                if price_check == True:
                    break
                      
                start_page_number += 1
                if subjects.index(subject) % 100 == 0:
                    json_dump(r'../json/last_save_info.json', {'subjects': subjects[subjects.index(subject):], 'brands':brand_ids[brand_ids.index(brand_id):]})
            
        # добавить инфу о успешное завершение категории
        # конечный коммит
        do_commit(connect)  
        #каждые 25 subjects 
        if subjects.index(subject) % 2 == 0:
            json_dump(r'../json/last_save_info.json', {'subjects': subjects[subjects.index(subject):], 'brands':[]})
    json_dump(r'../json/last_save_info.json', {'subjects': [], 'brands':[]})  


def subject_analytiks():
    url_for_artikuls  = 'https://wbxcatalog-ru.wildberries.ru/nm-2-card/catalog?spp={}&regions=75,64,4,38,30,33,70,68,71,22,31,66,40,82,1,80,69,48&stores=117673,122258,122259,125238,125239,125240,6159,507,3158,117501,120602,6158,120762,121709,124731,2737,130744,117986,1733,686,132043&pricemarginCoeff=1.0&reg=1&appType=1&offlineBonus=0&onlineBonus=0&emp=0&locale=ru&lang=ru&curr=rub&couponsGeo=12,3,18,15,21&dest=-1029256,-102269,-226149,-1292731&nm={}'
    config            = json_load(r'../json/config.json')
    db_name           = config['db_name']
    min_price         = config['min_price']
    spp               = config['spp']

    urls_info         = json_load(r'../json/urls.json')[db_name]
    url_for_pars      = urls_info['url_for_pars']
    url_for_brands    = urls_info['url_for_brands']
    
    connect           = connect_db(db_name)
    cursor            = connect.cursor()
    cursor.execute("select artikul from {};".format(db_name)) 
    products          = cursor.fetchall()
    count_of_artikuls = 0
    artikuls_list     = []
    subjects_list     = []
    result_list       = []
    count = 0
    for product in products:
        count_of_artikuls += 1
        artikuls_list.append(str(product[0]))
        
        if count_of_artikuls == 250 or (products.index(product) + 1) == len(products):
            count += 1

            url               = url_for_artikuls.format(spp, ";".join(artikuls_list))
            count_of_artikuls = 0
            artikuls_list     =[]
            html              = requests.get(url)
            data              = html.json()
            for product in data['data']['products']:
                subjects_list.append(product['subjectId'])
    
    subjects_list = list(set(subjects_list))

    
    url_for_pars  += '&page={}&sort=pricedown&priceU={}00;{}00&fbrand={}'
    
    
    for subject in subjects_list:
        count_of_artikuls = 0
        price_count       = 0
        subject_topic     = ''
        #  для каждого сабджекта получаем все бренды
        html      = requests.get(url_for_brands.format(subject, spp))
        data      = html.json()
        
        brand_ids = []
        for brand in data['data']['filters'][0]['items']:
            brand_ids.append(brand['id'])
        
        for brand_id in brand_ids:
            print(str(brand_ids.index(brand_id) + 1) + '/' +str(len(brand_ids))+ ':' + str(subjects_list.index(subject) + 1) + '/' + str(len(subjects_list)))
            max_price         =  int(data['data']['filters'][1]['maxPriceU']/100)
            start_page_number = 1
            while True:
                if start_page_number  > 30: 
                    if start_page_number % 30 == 0:
                        new_page_number = 30
                    else:
                        coef = int(start_page_number/30)
                        new_page_number = start_page_number - coef*30
            
                    url_for_request = url_for_pars.format(spp, subject, str(new_page_number), 0, max_price,brand_id)
                else:
                    url_for_request = url_for_pars.format(spp, subject, str(start_page_number), 0, max_price,brand_id)
                    
                html          = requests.get(url_for_request)
                products_info = html.json()
                if len(products_info['data']['products']) == 0:
                    break
    
                for product in products_info['data']['products']:
                    if product['subjectId'] == subject:
                        count_of_artikuls += 1
                        subject_topic      = product['name']
                        price              = int(product['salePriceU']/100)
                        if price >= min_price:
                            price_count += 1
             
                if start_page_number % 30== 0:
                    if max_price == price or (max_price - price) < 100:
                        max_price   = price- 100
                    else:
                        max_price   = price
            
                start_page_number += 1
                    
        if count_of_artikuls > 0:
            result_list.append({"subjectID":subject,"all":count_of_artikuls,"NormalPrice":price_count, "percent":round(price_count/count_of_artikuls * 100), "topic":subject_topic})
       
        print(str(count_of_artikuls) + ':' + str(price_count))
    json_dump(r'../json/analytics.json', result_list)
    print(result_list)
