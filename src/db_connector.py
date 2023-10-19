import sqlite3

'''Подключение к базе'''
def connect_db(db_name):
    sqlite_connection = sqlite3.connect(r'../db/' + db_name + '.db')
    return sqlite_connection

'''Получение цены по артикулу'''
def get_artikul_price(connect, db_name, product_artikul):
    cursor = connect.cursor()
    cursor.execute("select price,avg_same_price,price_history from {} where artikul={};".format(db_name,product_artikul)) 
    old_price = cursor.fetchone()
    if old_price is not None:
        return old_price
    else:
        return 0
    
'''Добавление артикулов и цены в базу'''
def db_insert(connect, db_name, artikul, price, avg_same_price, price_history):
    cursor = connect.cursor()
    cursor.execute("INSERT INTO {} VALUES(?,?,?,?);".format(db_name), [artikul,price,avg_same_price, price_history])

def update_db_price_history(connect, db_name, artikul, price):
    cursor = connect.cursor()
    cursor.execute("UPDATE {} SET price_history={} WHERE artikul={};".format(db_name,price,artikul))
    
def update_db_same_price(connect, db_name, artikul, price):
    cursor = connect.cursor()
    cursor.execute("UPDATE {} SET avg_same_price={} WHERE artikul={};".format(db_name,price,artikul))
    
'''Коммит'''
def do_commit(connect):
    connect.commit()
'''Создание базы'''
#------ db_name - имя базы данных (пример - avtotovary, т.е. без .db)------#
def create_db(db_name):    
    connect = sqlite3.connect(r'../db/' + db_name + '.db')
    cursor = connect.cursor()
    cursor.execute("""CREATE TABLE IF NOT EXISTS {}(
        artikul INTEGER PRIMARY KEY,
        price INTEGER,
        avg_same_price INTEGER,
        price_history INTEGER
        )""".format(db_name))
    return connect

def update_db(connect, db_name, artikul, price):
    cursor = connect.cursor()
    cursor.execute("UPDATE {} SET price={} WHERE artikul={};".format(db_name,price,artikul))
