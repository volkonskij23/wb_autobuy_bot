from wb_detector     import multi_get_data, send_error
from parser_for_db   import wb_parser,subject_analytiks
from logger          import json_load, json_dump
from analytics       import subject_analytiks,analytics
import os
import time
import datetime
import requests

def time_in_range():

    start = datetime.time(1, 0, 0)
    end = datetime.time(2, 0, 0)
    hours   = (int(time.strftime("%H", time.gmtime(time.time()))) + 3) % 24
    minutes = int(time.strftime("%M", time.gmtime(time.time())))
    x       = datetime.time(hours, minutes, 0)

    if start <= end:
        return start <= x <= end
    else:
        return start <= x or x <= end
    
if __name__ == '__main__':
    config           = json_load(r'../json/config.json')
    last_pars_info   = json_load(r'../json/last_save_info.json')
    vm_name          = config['db_name']
    count_of_errors  = 0
    to_do            = 1
    time_flag        = time_in_range()
    last_pars_for_db = os.stat(r'../json/last_save_info.json')
    
    headers          = json_load(r'../json/headers.json')
    resp             = requests.post('https://www.wildberries.ru/webapi/personalinfo', headers = headers)
    spp              = resp.json()['value']['personalDiscount']
    json_dump(r'../json/spp.json', {'spp': spp}) 
    
    if time_flag or len(last_pars_info['subjects']) != 0:
        to_do = 3
    else:
        to_do = 2
    multi_get_data()
    to_do           = int(input('Введите:\n 1 - для парсинга категории;\n 2 - для детекта на 2 ссылках;\n 3 - парсинг и детект;\n 4 - аналитика \n :'))
    
    if to_do == 1:
        while True:
            if count_of_errors > 10:
                break
            try:
                wb_parser()
                break
            except Exception as e:
                count_of_errors += 1
                text_to_tg = vm_name + ":" + str(e)
                send_error(text_to_tg)
                continue
                
    elif to_do == 2:
        while True:
            if count_of_errors > 10:
                break
            try:
                multi_get_data()
            except Exception as e:
                count_of_errors += 1
                text_to_tg = vm_name + ":" + str(e)
                send_error(text_to_tg)
                os.system('reboot now')
                continue
        
    elif to_do == 3:
        while True:
            if count_of_errors > 10:
                break
            try:
                wb_parser()
                break
            except Exception as e:
                count_of_errors += 1
                text_to_tg = vm_name + ":" + str(e)
                send_error(text_to_tg)
                continue
        while True:
            if count_of_errors > 10:
                break
            try:
                multi_get_data()
                break
            except Exception as e:
                count_of_errors += 1
                text_to_tg = vm_name + ":" + str(e)
                send_error(text_to_tg)
                continue
    else:
        subject_analytiks()
