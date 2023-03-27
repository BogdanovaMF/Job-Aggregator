# Job-Aggregator
 ***Идея : проект упрощает поиск, анализ и исследование вакансий на [hh.ru](http://hh.ru). Информация сохраняется в базу данных.***

### Общая информация

Title           | Job aggregator
----------------|----------------------
Author          | Marina Bogdanova
Language        | Python(3+)
Release         | 2023

### Run
Для запуска скрипта необходимо задать обязательный параметр ключевого запроса поиска. 
В системах Windows ключевой запрос в двойных кавычках.
Например: data-engineer. Скрипт запускается из командной строки:
```
python3 hh_parser.py --name data-engineer
```
### Docker container run example
Для запуска Docker-контейнера в командную строку вводится следующая команда:
```
docker run -it -d \
    -e POSTGRES_USER=<name_user> \
    -e POSTGRES_PASSWORD=<password> \
    -e POSTGRES_DB=<name_db> \
    -v ~/Job-Aggregator/postgres:/var/lib/postgresql/data \
    -p <port>:5432 \
    --name hh_db_container \
    postgres:13
```

### Processing
#### 1. Получение данных

На базе словаря входных данных формируется URL для get-запроса к [hh.ru](http://hh.ru).

С помощью библиотеки parser производится парсинг данных html-страниц.

Функция ```get_links``` возвращает ссылки на все страницы, которые необходимо обработать.

Функция ```get_vacancy_data``` возвращает кортеж с информацией о вакансии.

Функция ```save_data``` преобразовывает все полученные данные по вакансиям в DataFrame для дальнейшего анализа. Результат сохраняется на диск в формате csv файла.

#### Колонки фрейма:
Парамет         | Тип              |Описание
----------------|------------------|-------------------------
pub_date        | datetime(date)   | дата публикации вакансии
vacancy         | str              | наименование вакансии
experience      | str              | требуемый опыт
company         | str              | название компании
link            | str              | ссылка на вакансию
salary          | str              | заработная плата
skills          | str              | требуемые навыки
description     | str              | текст вакансии

#### Output data
Выходные данные - таблица в формате csv, созданная с помощью фреймворка pandas

#### 2. Запись полученных данных в базу postgresql

В уже существующей базе данных создается таблица ```vacancies```со следующими колонками:
Парамет         | Тип                     |Описание
----------------|-------------------------|-------------------------
id              | SERIAL PRIMARY KEY      | индивидуальный id для каждой вакансии
pub_date        | DATE                    | дата публикации вакансии
update_ts       | TIMESTAMP WITH TIME ZONE| дата вставки данных в таблицу vacancies
source_upload_dt| DATE                    | дата создания csv-файла, из которого выгружается информация
source_type     | CHARACTER VARYING (20)  | источник полученной инфомарции о вакансиях
vacancy         | CHARACTER VARYING (150) | наименование вакансии
experience      | CHARACTER VARYING (50)  | требуемый опыт
company         | CHARACTER VARYING (128) | название компании
link            | CHARACTER VARYING (10)  | ссылка на вакансию
salary          | CHARACTER VARYING (50)  | заработная плата
skills          | CHARACTER VARYING (300) | требуемые навыки
description     | VARCHAR                 | описание вакансии

В созданную таблицу с помощью модуля ```csv``` вносятся данные.
