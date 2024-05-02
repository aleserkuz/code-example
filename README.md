# Проект пайплайна инкрементальной загрузки

### Описание
В источнике (API в облаке) находятся данные о клиентах и их заказах. Архивные данные выгружаются инициализирующим процессом. Каждые сутки выгружаются заказы за новый день. Рассчитывается метрика удержания клиентов (Customer Retention Rate).

### Структура репозитория
1. Папка `migrations` хранит файлы миграции.
2. В папке `src` хранятся все необходимые исходники: 
    * Папка `dags` содержит DAG's Airflow.


