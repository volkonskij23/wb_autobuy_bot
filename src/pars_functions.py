from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
import re
import time

  
# Критерий загрузки обновленного ассортимента - исключение StaleElementReferenceException
""" Если взяли элемент, и страница обновилась, то при обращении к нему будет исключение"""
def load_checker(search_item):
    while True:
        try:
            search_item.find_element_by_class_name("lower-price").text
        except:
            return True
        
'''Артикул товара'''
#------ search_item -  карточка товара на странице ------# 
def get_artikul(search_item):
    item_url     = search_item.find_element_by_tag_name('a').get_attribute('href')
    item_artikul =  re.sub('\D', '', item_url)
    return item_artikul

'''Цена товара'''
#------ search_item -  карточка товара на странице ------# 
def get_price(search_item):
    try:
        price = search_item.find_element_by_class_name("lower-price").text
    except:
        price = search_item.find_element_by_class_name("price-commission__current-price").text
    price = int(re.sub('\D', '', price))
    return price

'''Установка фильтра цены'''
#------ driver    -  selenium driver  ------#
#------ min_price -  минимальная цена ------#
def set_price(driver, min_price):
    # Настройка фильтров поиска
    try:
        price_filter  = WebDriverWait(driver, 15).until(
                EC.presence_of_element_located((By.CLASS_NAME, "j-price.c-input-base-sm"))
            )  
    finally:
        # Установка цены
        price_filter = driver.find_element_by_class_name("j-price.c-input-base-sm")
        driver.execute_script("arguments[0].value = ''", price_filter)
        price_filter.send_keys(str(min_price))
        first_product_on_page = driver.find_elements_by_class_name("product-card.j-card-item")[-1]  # для проверки обновления ассортимента 
        price_filter.send_keys(Keys.RETURN)
        
        # Проверка загрузуки страницы 
        load_checker(first_product_on_page)
        time.sleep(5)
        
'''Получение разделов каталога'''
#------ driver      -  selenium driver    ------#
#------ catalog_url -  ссылка на каталог  ------#
  
def get_catalog_links(driver,catalog_url):
    result = []
    filtrs = '?sort=pricedown&page={}'
    driver.get(catalog_url)
    #  ждем прогрузку страницы
    try:
        catalog_item_urls  = WebDriverWait(driver, 15).until(
                EC.presence_of_element_located((By.CLASS_NAME, "j-menu-item"))
            )  
    finally:
        catalog_item_urls =  driver.find_elements_by_class_name("j-menu-item")
        for item in catalog_item_urls:
            item_url = item.get_attribute('href')
            item_url = item_url + filtrs
            result.append(item_url)
        return result