import psycopg2
from psycopg2 import OperationalError
import string
#--------------------------------------------------------------------------------------------------------------
def create_connection(db_host, db_port, db_user, db_password, db_name):
    ''' Ф-ция соединения с БД на сервере PostgreSQL'''
    connection = None
    try:
        connection = psycopg2.connect(
            host=db_host,
            port=db_port,
            user=db_user,
            password=db_password,
            database=db_name
        )
        print("Connection to PostgreSQL DB successful!!")
    except OperationalError as e:
        print(f"The error '{e}' occurred...")
    return connection
#---------------------------------------------------------------------------------------------------------------
connection = create_connection("192.168.51.120", "5432", "billadmin", "billadminpwd", "billdb")

print('Модуль поиска кода')

with connection:
    with connection.cursor() as cur:
        
        cur.execute("SELECT id, dst, client_id, prefix_id FROM paidcalls WHERE status = 'prefix defined'")
        call = cur.fetchone()
        
        if call:  # если есть хотя бы одна новая запись....

            # для удобства и наглядности заносим полученный список параметров вызова в переменные
            call_id, dst, client_id, prefix_id = (el for el in call)
            print('вызов: ', call_id, dst, client_id, prefix_id)

            # отделяем префикс от набранных цифр:
            cur.execute("SELECT prefix FROM prefix WHERE id = %s", [prefix_id])
            prefix = cur.fetchone()
            wopref = dst.split(prefix[0], 1)[1]
            print('префикс: ', prefix[0], wopref)

            # выбираем все записи из таблицы тарифов с тарифом клиента и допустимым префиксом:
            cur.execute('''SELECT id, code, price 
                            FROM tarif 
                            WHERE tarifnum = (SELECT tarifnum FROM client WHERE id = %s) 
                            AND  %s = ANY (prefixes)''', [client_id, prefix_id])
            codes = cur.fetchall()
            
            if codes:  # если есть хотя бы одна такая запись...
                code_lengt = 0
                code_id = 0
                
                for code in codes: # ищем вхождение кода с начала, наибольшей длины
                    
                    print('коды: ', code[1], wopref.startswith(code[1]))
                    
                    if wopref.startswith(code[1]):
                        if len(code[1]) > code_lengt:
                            code_id = code[0]
                            code_def = code[1]
                            code_lengt = len(code_def)
                            code_price = code[2]
                            print(code_id, code_def)
                
                if code_id != 0:
                    cur.execute('''
                                UPDATE paidcalls 
                                SET code_id = %s, 
                                    code = %s, 
                                    number = %s, 
                                    price = %s, 
                                    total = billmin * price, 
                                    status = 'tarif defined' 
                                WHERE id = %s''', [code_id, code_def, wopref.split(code_def, 1)[1], code_price, call_id])
                    print("код найден ид ", code_id)
                else:
                    cur.execute("UPDATE paidcalls SET status = 'tarif undefined' WHERE id = %s", [call_id])
                    print("код НЕ найден")

 #print('поиск кода и тарифа - конец')