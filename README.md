# telegram_bot

### Задача
Телеграм-бот должен каждые 15 минут отслеживать ключевые метрики приложения социальной сети. В случае аномалий должно приходит сообщение в телеграм с величиной отклонения, графиком и ссылкой на чарт.

### Реализация
Метод поиска аномаолий основан на првиле "трех сигм". Каждые 15 минут считается сглаженное среднеквадратичное отклонение мертрики за последние 5 периодов (период=15 минут).

Чтобы организвать подсчет метрик организован ETL-пайплайн, в котором происходит загрузка, трансформация данных и отпарвка сообщений в чат в случае необходимости.

В качестве метрик выбраны: количество польователей ленты новостей, количество лайков/просмотров, количество польователей мессенджера, CTR, количество сообщений и др.

### Используемые инструменты

Python, Seaborn, telegrammAPI, AirFlow, Superset

### Файлы
__telegram_bot.py__ - скрипт дага с решением задачи

__alert_dashboard.jpeg__ - скрин дашборда с отслеживаемыми метриками

__phone_screen.jpeg__ - скрин телефона с оповещением



