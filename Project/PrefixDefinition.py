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

print('Модуль поиска префикса')
# открываем транзакцию
with connection:
    with connection.cursor() as cur:
        
        cur.execute("SELECT id, dst FROM paidcalls WHERE status = 'client defined'")
        call = cur.fetchone()
        
        if call:  # если есть хотя бы одна новая запись....
            # для удобства и наглядности заносим полученный список параметров вызова в переменные
            call_id = call[0]   
            dst = call[1]
            print('вызов: ', call_id, dst)
            
            cur.execute("SELECT id, prefix FROM prefix")
            prefix = cur.fetchall()
            
            pref_lengt = 0
            pref_id = 0
            
            for pref in prefix:
                if dst.startswith(pref[1]):
                    if len(pref[1]) > pref_lengt:
                        pref_lengt = len(pref[1])
                        pref_id = pref[0]
                #print(pref[0], pref[1])
            
            if pref_lengt > 0:
                cur.execute('''
                            UPDATE paidcalls 
                            SET prefix_id = %s, 
                                status = 'prefix defined' 
                            WHERE id = %s''', [pref_id, call_id])
                print("найден префикс с ид ", pref_id)
            else:
                cur.execute("UPDATE paidcalls SET status = 'local call' WHERE id = %s", [call_id])                
                print("префикс не найден")

print('поиск префикса - конец')
