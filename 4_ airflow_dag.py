#!/usr/bin/env python
# coding: utf-8

from airflow import DAG
from airflow.operators import PythonOperator
from datetime import datetime


default_args = {
    'owner': 'igubanov',  # отобразится в интерфейсе airflow
    'depends_on_past': False,  # зависит ли результат исполнения DAG'a от его предыдущей истории 
    'start_date': datetime(2020, 1, 1),   # когда мы хотим начать отрабатывать задачу
    'retries': 0    # сколько раз пытаться запустить DAG
}

dag = DAG('igubanov_miniproj_dag',   # имя DAG'a, будет отображаться в интерфейсе airflow
          default_args=default_args,   # передаем словарик с аргументами, записанный выше
          catchup=False, 
          schedule_interval='00 12 * * 1')   # запускать нашу задачу в 12:00 каждый понедельник


# функция, которая считает метрики и отправляет их в vk используя api
def send_report_to_vk():
    import pandas as pd
    import numpy as np
    import seaborn as sns
    import matplotlib.pyplot as plt
    import os
    from pathlib import Path
    import vk_api
    import random

    path = 'https://docs.google.com/spreadsheets/d/e/2PACX-1vR-ti6Su94955DZ4Tky8EbwifpgZf_dTjpBdiVH0Ukhsq94jZdqoHuUytZsFZKfwpXEUCKRFteJRc9P/pub?gid=889004448&single=true&output=csv'
    ads = pd.read_csv(path, parse_dates=[0])
    
    # считаем метрики - показы, клики, ctr, траты
    metrics = []
    for d in ads.date.unique():
        part_df = ads[ads.date == d].reset_index()
        views = part_df[part_df.event == 'view'].shape[0]
        clicks = part_df[part_df.event == 'click'].shape[0]
        ctr = clicks / views * 100
        sums = part_df.ad_cost[0] / 1000 * views
        metrics.append([views, clicks, ctr, sums])

    metrics_df = pd.DataFrame(metrics, index=ads.date.unique(), columns=['views', 'clicks', 'ctr', 'sums'])
    
    # разница за две даты
    difference = round(metrics_df.loc['2019-04-02'] / metrics_df.loc['2019-04-01'] * 100 - 100, 1)
    metrics_df.loc['2019-04-01'] / metrics_df.loc['2019-04-02']
    
    # сообщение, которое будет приходить в вк
    message = f' Отчет по объявлению {ads.ad_id[0]} oт 2019-04-02 \n Показы: {metrics_df.loc["2019-04-02"].views} ({difference.views}%) \n Клики: {metrics_df.loc["2019-04-02"].clicks} ({difference.clicks}%) \n CTR: {metrics_df.loc["2019-04-02"].ctr} ({difference.ctr}%) \n Траты:  {metrics_df.loc["2019-04-02"].sums} рублей ({difference.sums}%)'
    print(message)
    
    # записываем сообщение в файл
    with open(f'report_2019-04-02.txt', 'w') as fwr:
        fwr.write(message)

    # Токен vk
    app_token = ''                                
    
    # id чата
    chat_id = 1

    # id получателя
    my_id =                                      

    # инициализация сессии
    vk_session = vk_api.VkApi(token=app_token)

    vk = vk_session.get_api()

    vk.messages.send(
        chat_id=chat_id,
        random_id=random.randint(1, 2 ** 31),
        message=message)


t1 = PythonOperator(task_id='ad_report',    # создаем таску
                    python_callable=send_report_to_vk,   # передаем нашу функцию в первую таску для нашего DAG'a
                    dag=dag)     # передаем dag



# Вариант с открытием файла и отправкой текста из него (а не просто отправкой сообщения)

# авторизуемся в VK через сервисный ключ
#     token = ''
#     vk_session = vk_api.VkApi(app_id = 454383, token = token);
#     vk = vk_session.get_api()

#     # получаем урл для загрузки файла в сообщение
#     upload_url = vk.docs.getMessagesUploadServer(peer_id = 17828802)

#     # загружаем файл
#     files = {'file': open("ad_analytics.txt","rb")}
#     r = requests.post(upload_url['upload_url'], files=files)
#     saved_f = vk.docs.save(file = json.loads(r.text)['file'], title = 'ad_analytics.txt')

#     # отправляем загруженный файл в сообщении
#     vk.messages.send(
#         chat_id = 1,
#         random_id = random.randint(0, 9999),
#         message = F"This is today's analytics",
#         attachment = F"{saved_f['type']}{saved_f['doc']['owner_id']}_{saved_f['doc']['id']}"
#     )