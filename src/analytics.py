from logger                        import json_load, json_dump
from db_connector                  import create_db, db_insert, do_commit, get_artikul_price, update_db, connect_db, update_db_price_history, update_db_same_price
from concurrent.futures            import as_completed
from concurrent.futures            import ThreadPoolExecutor
from requests_futures.sessions     import FuturesSession
import requests


def subject_analytiks():
    url_for_artikuls  = 'https://wbxcatalog-ru.wildberries.ru/nm-2-card/catalog?spp={}&regions=75,64,4,38,30,33,70,68,71,22,31,66,40,82,1,80,69,48&stores=117673,122258,122259,125238,125239,125240,6159,507,3158,117501,120602,6158,120762,121709,124731,2737,130744,117986,1733,686,132043&pricemarginCoeff=1.0&reg=1&appType=1&offlineBonus=0&onlineBonus=0&emp=0&locale=ru&lang=ru&curr=rub&couponsGeo=12,3,18,15,21&dest=-1029256,-102269,-226149,-1292731&nm={}'
    config            = json_load(r'../json/config.json')
    db_name           = config['db_name'] + '_analytics'
    min_price         = config['min_price']
    spp               = config['spp']

    urls_info         = json_load(r'../json/urls_analytics.json')[config['db_name']]
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
        print(products.index(product))
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


'''Парсер карточек на странице и добавление артикула и цены в базу'''
#------ driver               -  selenium driver                                                                           ------#
#------ db_connect           -  подключение к базе                                                                        ------#
#------ min_price            -  минимальная цена                                                                          ------#    
#------ current_page_number  -  номер текущей страницы, если 1000-ая стр то возвращаем флаг и последнюю цену на странице  ------#        
def page_parser_for_db(url,connect, min_price, current_page_number, db_name):
    
    price_flag    = False
    artikuls_list = []
    price         = 0
    html          = requests.get(url)
    try:
        data      = html.json()
    except:
        return 'end_of_artikuls'
    if len(data['data']['products']) == 0:
        return 'end_of_artikuls'
    
    
    for product in data['data']['products']:
        price          = int(product['salePriceU']/100)
        artikul        = product['id']
        print(str(artikul)+':'+str(price))
        
        if price < min_price:
            price_flag = True
            break
        try:
            db_insert(connect,db_name,artikul,price, 0, 0)
        except:
            continue  
    if current_page_number % 30== 0:
        return [price_flag, price]
    else:
        return price_flag
    
        
def analytics():    
    config         = json_load(r'../json/config.json')
    db_name        = config['db_name'] + '_analytics'
    min_price      = config['min_price']
    spp            = config['spp']
    
    urls_info      = json_load(r'../json/urls_analytics.json')[config['db_name']]
    subject_list   = urls_info['subjects_list']
    url_for_pars   = urls_info['url_for_pars']
    url_for_brands = urls_info['url_for_brands']
    
    
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
        if len(last_pars_info['brands']) != 0:
            brand_ids = last_pars_info['brands']
        else:
            for brand in data['data']['filters'][0]['items']:
                brand_ids.append(brand['id'])
        
                 
        for brand_id in brand_ids:
            print(str(brand_ids.index(brand_id) + 1) + '/' +str(len(brand_ids)) + '('+ str(subjects.index(subject)) + ':' + str(len(subjects)) +')')
            max_price         =  int(data['data']['filters'][1]['maxPriceU']/100)
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
                    
                price_info = page_parser_for_db(url_for_request, connect, min_price,start_page_number, db_name)
                
                    
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


