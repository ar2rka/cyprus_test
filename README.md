# cyprus_test
## Задание 1
####  Подготовка. Задание выполнено в Vertica.
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

## Задание 2
#### Подготовка. Задание выполено на python с pyspark.
Установка:
* pip3 install --upgrade pip
* python3 -m venv .venv
* source ./.venv/bin/activate
* pip3 install -r ./requirements.txt

Первый источник -- таблица ```DDL/bank/tb_transactions.sql``` с тестовыми данными ```DML/bank/tb_transactions.sql```  
В качестве второго источника выбран файл формата parquet ```reconciliation/second_source```, для удобства изучения содержимое файла продублировано в формате CSV ```reconciliation/second_source.csv```  

#### Задача 2-1
Скрипт для запуска - ```reconciliation/task21.py```
Настройка толеранса для реконсиляции - ```reconciliation/config.ini```
#### Задача 2-2
Скрипт для запуска - ```reconciliation/task22.py```

