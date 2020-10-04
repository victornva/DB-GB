import psycopg2
from psycopg2 import OperationalError
#import string, datetime

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

# открываем транзакцию
with connection:
    with connection.cursor() as cur:
        cur.execute("SELECT id, src, dst FROM paidcalls WHERE status = 'new record'")
        call = cur.fetchone()
 
        if call:                  # если есть хотя бы одна новая запись....
            call_id, src, dst = (el for el in call)
            print('вызов: ', call_id, src, dst)
            # входящий?
            cur.execute("SELECT client_id FROM callerid WHERE callerid = %s", [dst])
            incoming_cli_id = cur.fetchone()

            if incoming_cli_id:      # входящий
                client_id = incoming_cli_id[0]
                print(f'входящий на номер {dst} client_id = {client_id}') 
                cur.execute('''
                            UPDATE paidcalls 
                            SET client_id = %s, 
                                client = (SELECT client.name FROM client WHERE client.id = %s), 
                                status = 'incoming' 
                            WHERE id = %s''', [client_id, client_id, call_id])
            else:
                # исходящий?
                cur.execute("SELECT callerid.client_id FROM callerid WHERE callerid.callerid = %s", [src])
                outgoing_cli_id = cur.fetchone()

                if outgoing_cli_id:  # исходящий
                    client_id = outgoing_cli_id[0]
                    cur.execute('''
                                    UPDATE paidcalls 
                                    SET client_id = %s, 
                                        client = (SELECT client.name FROM client WHERE client.id = %s), 
                                        status = 'client defined' 
                                    WHERE id = %s''', [client_id, client_id, call_id])
                    print(f'исходящий с номера {src} client_id = {client_id}')
                else:
                    print('клиент неизвестен')
                    cur.execute("UPDATE paidcalls SET status = 'client undefined' WHERE id = %s", [call_id])

print('Модуль определение клиента - конец')
