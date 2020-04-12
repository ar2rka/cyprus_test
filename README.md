# cyprus_test
## Первая часть выполнена с бд vertica в докере
```docker run -p 5433:5433 -e DATABASE_NAME='notdocker' -e DATABASE_PASSWORD='foo123' jbfavre/vertica```


## Вторая часть реализована с помощью pyspark

Установка:
* pip3 install --upgrade pip
* python3 -m venv .venv
* source ./.venv/bin/activate
* pip3 install -r ./requirements.txt
