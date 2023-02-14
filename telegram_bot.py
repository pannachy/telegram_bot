from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from datetime import datetime, timedelta

import io
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import pandahouse as ph
import seaborn as sns
import telegram


# данные для отправки в чат
MY_TOKEN = token
CHAT_ID = XXXXX

# запросы к БД
q = """
        SELECT 
            toDate(time) as day,
            formatDateTime(time, '%R') as hm,
            *
        FROM (
            SELECT 
                toStartOfFifteenMinutes(time) as time, 
                uniq(user_id) as qty_users_feed,
                countIf(action='like') as likes,
                countIf(action='view') as views,
                ROUND(likes/views, 3) as ctr
            FROM simulator_20230120.feed_actions 
            WHERE time >= subtractHours(now(), 25) AND time < toStartOfFifteenMinutes(now())
            GROUP BY time) as t1
        LEFT JOIN (
            SELECT 
                toStartOfFifteenMinutes(time) as time, 
                uniq(user_id) as qty_users_messenger,
                count(reciever_id) as messages
            FROM simulator_20230120.message_actions 
            WHERE time >= subtractHours(now(), 25) AND time < toStartOfFifteenMinutes(now())
            GROUP BY time) as t2
        USING time
        ORDER BY time
    """
q2 = """     
        WITH tmp AS (
                SELECT GREATEST(message.ts, feed.ts) as time, 
                        GREATEST(message.user_id, feed.user_id) as user_id_both,
                        MAX(GREATEST(message.os, feed.os)) as os,
                        MAX(GREATEST(message.source, feed.source)) as source
                FROM (
                        SELECT user_id, 
                                toStartOfFifteenMinutes(time) as ts,
                                os,
                                source
                        FROM simulator_20230120.message_actions
                        WHERE ts >= subtractHours(now(), 25) AND ts < toStartOfFifteenMinutes(now())
                        ) as message
                FULL JOIN (
                        SELECT user_id, 
                                toStartOfFifteenMinutes(time) as ts,
                                os,
                                source
                        FROM simulator_20230120.feed_actions
                        WHERE ts >= subtractHours(now(), 25) AND ts < toStartOfFifteenMinutes(now())
                        ) as feed 
                USING user_id, ts
                GROUP BY time, user_id_both
                )
        SELECT  toDate(time) as day,
                formatDateTime(time, '%R') as hm, 
                countIf(os = 'Android') as qty_android, 
                countIf(os = 'iOS') as qty_iOS,
                countIf(source = 'ads') as qty_ads,
                countIf(source = 'organic') as qty_organic
        FROM tmp
        GROUP BY day, hm
        ORDER BY day, hm
        """


def check_anomaly(df, metric, a=3, n=5):
    # алгоритм поиска аномалий в данных (3 сигмы)
    
    df['std'] = df[metric].shift(1).rolling(n).std()
    df['moving_mean'] = df[metric].shift(1).rolling(n).mean()
    df['up_std'] = df['moving_mean'] + a * df['std']
    df['low_std'] = df['moving_mean'] - a * df['std']
    
    df['up_std'] = df['up_std'].rolling(n + 4, center=True, min_periods=1).mean()
    df['low_std'] = df['low_std'].rolling(n + 4, center=True, min_periods=1).mean()
    
    if (df[metric].iloc[-1] < df['low_std'].iloc[-1] or df[metric].iloc[-1] > df['up_std'].iloc[-1])\
        and  abs(df[metric].iloc[-1] / df[metric].iloc[-2] - 1) > 0.15:
        is_alert = 1
    else:
        is_alert = 0

    return is_alert, df

links = {'qty_users_feed':'http://superset.lab.karpov.courses/r/3055',  
        'qty_users_messenger':'http://superset.lab.karpov.courses/r/3056',
         'likes': 'http://superset.lab.karpov.courses/r/3058', 
         'views': 'http://superset.lab.karpov.courses/r/3057', 
         'ctr': 'http://superset.lab.karpov.courses/r/3051', 
         'messages': 'http://superset.lab.karpov.courses/r/3052',
         'qty_android': 'http://superset.lab.karpov.courses/r/3053', 
         'qty_iOS': 'http://superset.lab.karpov.courses/r/3053', 
         'qty_ads': 'http://superset.lab.karpov.courses/r/3054', 
         'qty_organic': 'http://superset.lab.karpov.courses/r/3054'
        }

def run_alerts(data, chat_id=CHAT_ID):
    # система алертов
    
    metric_list = ['qty_users_feed', 'likes', 'views', 'ctr', 'messages',
                   'qty_users_messenger', 'qty_android', 'qty_iOS', 'qty_ads', 'qty_organic']
       
    for metric in metric_list:
        
        df = data[['time', 'day', 'hm', metric]].copy()
        is_alert, df = check_anomaly(df, metric)

        if is_alert == 1:
            current_val = df[metric].iloc[-1]
            deviation = ((df[metric].iloc[-1] / df[metric].iloc[-2]) - 1) * 100
            time_now = df['time'].iloc[-1]
            
            if deviation > 0:
                msg = f' *Метрика {metric}* \n{time_now}: \n🟢 current value: {current_val} ( +{deviation:.0f}%)'
            else: 
                msg = f' *Метрика {metric}* \n{time_now}: \n🔴 current value: {current_val} ( {deviation:.0f}%)'
            msg += f'\n[Ссылка на чарт]({links[metric]})'

            sns.set(rc={'figure.figsize': (11, 8)})
            sns.set_style('darkgrid')
            ax = sns.lineplot(data=df, x='time', y='low_std', linewidth=0.5, color='darkgreen', label='upper bound')
            ax = sns.lineplot(data=df, x='time', y=metric, color='darkblue', label=metric)
            ax = sns.lineplot(data=df, x='time', y='up_std', linewidth=0.5, color='red', label='lower bound')
            
            ax.fill_between(data=df, x='time', y1='low_std', y2='up_std', color='green', alpha=0.02)
                    
            ax.set(ylabel=metric)
            ax.set(xlabel=None)
            ax.set_title(f'Dinamic of metric: {metric}', fontweight='bold', fontsize=14)

            plot_object = io.BytesIO()
            plt.savefig(plot_object)
            plot_object.seek(0)
            plot_object.name = 'anomaly_metrics.png'
            plt.close()

            bot = telegram.Bot(token=MY_TOKEN)
            bot.sendMessage(chat_id=chat_id, text=msg, parse_mode='Markdown') 
            bot.sendPhoto(chat_id=chat_id, photo=plot_object)

# настройки соединения
connection = {
        'host': host,
        'password': password,
        'user': user,
        'database': database,
}

# дефолтные параметры
default_args = {
    'owner': 'a-ponomareva',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2023, 2, 7)
    
}

# интервал для запуска - каждые 15 минут
schedule_interval = '*/15 * * * *'
    
@dag(default_args=default_args, schedule_interval=schedule_interval, tags=['a-pono'], catchup=False)
def pono_alert_dag():
    @task()
    def load_feed_messages():
        data_fm = ph.read_clickhouse(query=q, connection=connection)

        return data_fm
    
    @task()
    def load_feed_messages_os_source():
        data_fm_os_source = ph.read_clickhouse(query=q2, connection=connection)

        return data_fm_os_source
    
    @task()
    def transform_data(data_fm, data_fm_os_source):
        full_data = pd.merge(data_fm, data_fm_os_source, on = ['day', 'hm'])
            
        return full_data
    
    @task()
    def alert_sсript(full_data):
        run_alerts(full_data)
        
        
    # последовательность операций
    data_fm = load_feed_messages()
    data_fm_os_source = load_feed_messages_os_source()
    full_data = transform_data(data_fm, data_fm_os_source)
    alert_sсript(full_data)
    
pono_alert_dag = pono_alert_dag()