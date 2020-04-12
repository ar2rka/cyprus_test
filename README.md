# cyprus_test
## Задание 1
####  Подготовка
```docker run -p 5433:5433 -e DATABASE_NAME='notdocker' -e DATABASE_PASSWORD='foo123' jbfavre/vertica```
#### Задача 1-1. Создание и наполнение таблиц
Скрипт создания схем -- ```SCHEMA_DDL.sql```, скрипты создания таблиц в директории DDL/  
Скрипты для наполнения таблиц тестовыми данными в директории DML/  
#### Задача 1-2
Скрипт с решением - ```task1-2.sql```
#### Задача 1-3
Скрипт с решением - ```task1-3.sql```
#### Задача 1-4
Скрипт с решением - ```task1-4.sql```  

## Вторая часть реализована с помощью pyspark

Установка:
* pip3 install --upgrade pip
* python3 -m venv .venv
* source ./.venv/bin/activate
* pip3 install -r ./requirements.txt
