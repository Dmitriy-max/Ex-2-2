#  ---------------------------------------------------------------------------
# Установим необходимые Packages для Python
# Для этого в теринале выполним команду(ы):
# pip install pandas
# pip install sqlalchemy
# pip install Flask
# pip install Flask-RESTful
#  ---------------------------------------------------------------------------
#
import array
# ---------------------------------------------------------------------------
#                       импортируем необходимые библиотеки
# ----------------------------------------------------------------------------
import os                           # для раболты с файлами, операционной системой
import datetime                     # для работы с датой и временем
import sqlite3                      # для работы с sqlite
import argparse                     # для работы с параметрами
import json                         # для работы с json

from flask import Flask, request
from flask_restful import Api, Resource

# ----------------------------------------------------------------------------
# ----------------------------- Тело скрипта ---------------------------------
# ----------------------------------------------------------------------------

def query(date_start, date_end, users):

    data_base_name = os.getcwd() + "\\database.db"  # путь к БД database.db


    if os.path.exists(data_base_name):  # проверим несть ли в каталоге уже созданная БД
        with sqlite3.connect(data_base_name) as db:  # работаем с БД
            cursor = db.cursor()  # создадим курсор

    # Создам строку для зпроса
    q = ("""SELECT DISTINCT
        user_id AS user_id,
        begin_time AS begin_time,
        end_time AS end_time,
        sessions.start_session AS start_session,
        sessions.stop_session AS stop_session,
        COUNT (users.user_id) AS sessions_count,
        SUM (
            CASE
                WHEN DATE(start_session) = DATE(stop_session) 
                    AND (TIME(start_session) BETWEEN TIME(begin_time) AND TIME(end_time)) 
                    AND (TIME(stop_session) BETWEEN TIME(begin_time) AND TIME(end_time)) 
                    AND (TIME(stop_session) >= TIME(start_session)) 
                THEN  ROUND((JULIANDAY(stop_session) - JULIANDAY(start_session)) * 86400)

                WHEN DATE(start_session) = DATE(stop_session) 
                    AND TIME(start_session) < TIME(begin_time)
                    AND TIME(stop_session) < TIME(begin_time)                                                                                                
                THEN 0

                WHEN DATE(start_session) = DATE(stop_session) 
                    AND TIME(start_session) > TIME(end_time)          
                    AND TIME(stop_session) > TIME(end_time)                                                                                                  
                THEN 0

                WHEN DATE(start_session) = DATE(stop_session) 
                    AND TIME(start_session) < TIME(begin_time)        
                    AND TIME(stop_session) > TIME(end_time)                                                                                                  
                THEN 0

                WHEN TIME(start_session) < TIME(begin_time)   
                    AND ( TIME(stop_session) BETWEEN TIME(begin_time) AND TIME(end_time) )                                                                                                                     
                THEN ROUND((JULIANDAY(TIME(stop_session)) - JULIANDAY(TIME(begin_time))) * 86400)

                WHEN (TIME(start_session) BETWEEN TIME(begin_time) AND TIME(end_time)) 
                    AND TIME((stop_session) > TIME(end_time))                              
                THEN ROUND((JULIANDAY(TIME(end_time)) - JULIANDAY(TIME(start_session))) * 86400)
            END) session_time
        FROM
            users
        INNER JOIN
            sessions
            ON  users.user_id = sessions.id_user
    
        WHERE 
            {where}
        GROUP BY
                users.user_id
    
        """)

    # Условие запроса для выборки всех пользователей за интервал
    where_all_users = (f"""
        DATE(stop_session) BETWEEN '{date_start}' AND '{date_end}'
        AND  DATE(stop_session) BETWEEN '{date_start}' AND '{date_end}'
    """)

    # Условие запроса для выборки нескольких конкретных пользователей за интервал
    where_one_user = (f"""
        DATE(stop_session) BETWEEN '{date_start}' AND '{date_end}'
        AND  DATE(stop_session) BETWEEN '{date_start}' AND '{date_end}'
        AND users.user_id = {users}
    """)

    # Условие запроса для выборки нескольких конкретных пользователей за интервал
    where_several_users = (f"""
        DATE(stop_session) BETWEEN '{date_start}' AND '{date_end}'
        AND  DATE(stop_session) BETWEEN '{date_start}' AND '{date_end}'
        AND users.user_id IN {users}
    """)

    # если колчество пользователей == 0 то выбираем , где все пользователи, иначе выбираем только по указанным
    if len(users) == 0:  # будем выбирать всех пользователей, иначе только указанных
        where = where_all_users.format(date_start=date_start, date_end=date_end)
    else:
        if len(users) == 1:
            where = where_one_user.format(date_start=date_start, date_end=date_end, users=users)
        else:
            where = where_several_users.format(date_start=date_start, date_end=date_end, users=users)

    query_str = q.format(where=where)  # получим итоговый запрос со всеми необходимыми параметрами

    session_list = []

    for i in cursor.execute(query_str).fetchall():
        # Преобразуем секунды в часы и минуты, с учетом возможной ошибки Nan времени
        try:
            s_time = str(datetime.timedelta(seconds=int(i[6])))
        except:
            s_time = '0:00:00'
        session_list.append({'user_id': i[0], 'session_time': s_time})      # для вывода в json

    db.close()  # Завершим работу с БД

    return session_list

app = Flask(__name__)
api = Api()

date_start = '2024-05-27'  # Дата начала выбоки
date_end = '2024-05-27'  # Дата конца выбоки


@app.route('/users', methods=['GET'])
def get ():
        date_start = request.args.get('DateStart')              # получим параметр DateStart
        date_end = request.args.get('DateEnd')                  # получим параметр DateEnd
        users_id = request.args.get('Users_ID', default='All')  # получим параметр Users_ID
        #users = tuple(users_id)

        if users_id == 'All':
            users_id = ''
            res = query(date_start, date_end, users_id)
        else:
            users_id = users_id.strip(',')                      # удалим лишние запятые в ачале и конце строки (если они там есть)
            users_id = users_id.replace(' ', '')    # удалим все пробелы в строке
            users_id = users_id.split(',')                      # разобьем строку на части

            if len(users_id) == 1:                              # какие-то преобразованияб чтобы работало
                s = str(users_id)[2:-2], ''                     #
                res = query(date_start, date_end,s)             #
            else:
                res = query(date_start, date_end,tuple(users_id))
        return res

#api.init_app(app)

if __name__ == "__main__":
    app.run(debug = False, port = 3000, host='127.0.0.1')


# ----------------------------------------------------------------------------
# ------------------------------Конеец тела скрипта --------------------------
# ----------------------------------------------------------------------------