# Лабораторная работа №3: Знакомство с Apache Spark

- [Лабораторная работа №3: Знакомство с Apache Spark](#лабораторная-работа-3-знакомство-с-apache-spark)
  - [Создание standalone кластера Spark](#создание-standalone-кластера-spark)
  - [Датасет для заданий](#датасет-для-заданий)
  - [Задачи](#задачи)
    - [Задание 1 – Количество и сумма транзакций по каждому клиенту](#задание-1--количество-и-сумма-транзакций-по-каждому-клиенту)
    - [Задание 2 – Средняя сумма транзакций по каждой карте](#задание-2--средняя-сумма-транзакций-по-каждой-карте)
    - [Задание 3 – Анализ транзакций по бренду карты](#задание-3--анализ-транзакций-по-бренду-карты)
    - [Задание 4 – Средняя сумма транзакций по возрастным группам клиентов](#задание-4--средняя-сумма-транзакций-по-возрастным-группам-клиентов)
    - [Задание 5 – Доля способов оплаты](#задание-5--доля-способов-оплаты)
    - [Задание 6 – Определение топ-3 транзакций по сумме для каждого клиента](#задание-6--определение-топ-3-транзакций-по-сумме-для-каждого-клиента)


## Создание standalone кластера Spark

**Шаги**:
1. Заходим на [яндекс облако](https://console.yandex.cloud)
2. Заказываем две виртуальные машины, конфигурация следующая:
   * *OS* &mdash; любой дистрибутив Linux, который вам удобен (личная рекомендация Ubuntu 22.04)
   * *Диск* &mdash; HDD, не более 40 гб
   * *Выч. ресурсы* &mdash; 4 vCPU, 8 гб RAM
   * Создаем ssh ключ для подключения к машинам по ssh, [инструкция](https://yandex.cloud/ru/docs/compute/operations/vm-connect/ssh)
   * Для машин задаем имена sparkmaster и sparkworker
3. Подключаемся к sparkmaster
4. Обновляем список пакетов командой `sudo apt update` и устанавливаем java 11 версии с помощью команды `sudo apt install openjdk-11-jdk -y`.
5. Далее редактируем файл `~/.bashrc`, длбавляем в конец строку:
   ```bash
   export JAVA_HOME= # здесь указываем домашнюю директорию Java, например /usr/lib/jvm/java-11-openjdk-amd64/
   export PATH=$PATH:$JAVA_HOME:$JAVA_HOME/bin
   ```
   Директорию с Java можно узнать с помощью команды `update-alternatives --config java`
6. Скачиваем spark с помощью команды `wget https://dlcdn.apache.org/spark/spark-3.5.6/spark-3.5.6-bin-hadoop3.tgz` и распаковываем полученный архив
7. Переименовываем его в папку `/opt/spark`
8. В каталоге `/opt/spark/conf` находим шаблоны для файлов конфигураций spark `spark-defaults.conf.template`, `spark-env.sh.template`, `workers.template`. Убираем постфикс `.template` для каждого из них
9. Прежде чем добавить worker в файл с конфигурацией `workers` необходимо добавить его в `/etc/hosts`. Пример:
    ```bash
    # ...
    123.123.23.23 worker
    ```
10. Теперь записываем хост `worker` в файл `workers` конфигурации Spark
11. Далее обновить конфигурации `spark-defaults.conf` и `spark-env.sh`, чтобы можно было подключаться к webui `spark мастера`. Примеры конфигураций есть в репозитории.
12. Обновляем файл `~/.bashrc`, добавляем в конец файла:
    ```bash
    export SPARK_HOME=/opt/spark
    export PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin
    ```
    После этого выполняем команду `source ~/.bashrc`
13. Далее повторяем шаги 4-8, 11-12 на второй, worker-машине.
14. После этого на sparkmaster можно запускать master командой `start-master.sh --host 0.0.0.0`.
15. На той же машине мы можем также запустить и `worker` &mdash; командой `start-worker.sh spark://<spark-master-host>:<spark-master-port>`
16. После этого запускаем worker и на второй, worker-машине.
17. После проделанных шагов можно запускать программы с кодом spark командой `spark-submit` или же писать запросы в интерактивной среде `spark-shell`.

## Датасет для заданий

Датасет для выполнения заданий лабораторной работы используется [такой](https://www.kaggle.com/datasets/computingvictor/transactions-fraud-datasets). Из него нужны три файла &mdash; `cards_data.csv`, `transactions_data.csv`, `users_data.csv`. 
Далее для того, чтобы каждый worker мог обрабатывать файлы их можно загрузить в Yandex Object Storage. Сделать это можно прямо из веб-консоли Yandex Cloud. Там мы можем создать bucket, в который загрузим все файлы. Также нам понадобятся *access key* и *secret key* для взаимодействия с хранилищем в `main.py` и при запуске программы на кластере. [Документация как их получить](https://yandex.cloud/ru/docs/iam/operations/authentication/manage-access-keys#create-access-key).

## Задачи

Для выполнения задач необходимо написать на python скрипт, который поднимает spark-сессию и затем выполняет необходимые запросы. Шаблон скрипта есть в репозитории, необходимо вывести результат для каждой задачи в `stdout`. Для решения можно использовать как Spark SQL, так и Spark Dataframe API. Команда для запуска скрипта на кластере:
```bash
spark-submit \
  --master spark://<spark-master-host>:<spark-master-port> \
  --conf spark.executor.memory=2g \
  --conf spark.hadoop.fs.s3a.access.key=<access-key> \
  --conf spark.hadoop.fs.s3a.secret.key=<secret-key> \
  --conf spark.hadoop.fs.s3a.endpoint=https://storage.yandexcloud.net \
  --conf spark.hadoop.fs.s3a.path.style.access=true \
  --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
  --conf spark.driver.bindAddress=0.0.0.0 \
  --conf spark.driver.host=<master-public-ip> \
  --packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.691 \
  main.py
```

[Cheat-sheet по PySpark](https://www.globalsqa.com/pyspark-cheat-sheet/)

### Задание 1 – Количество и сумма транзакций по каждому клиенту
Для каждого `client_id` из `transactions_data.csv` вычислить:
  * количество транзакций (`txn_count`)
  * сумму всех транзакций (`total_amount`)
Отсортировать по убыванию `total_amount`.

### Задание 2 – Средняя сумма транзакций по каждой карте

Для каждой `card_id` из `transactions_data.csv` вычислить среднюю сумму транзакций (`avg_amount`).
Отсортировать по убыванию средней суммы.

### Задание 3 – Анализ транзакций по бренду карты

Соединить `transactions_data.csv` с `cards_data.csv` по `card_id` -> `cards.id`.
Для каждого `card_brand` вычислить:
  * количество транзакций (`txn_count`)
  * среднюю сумму транзакций (`avg_amount`)
Отсортировать по убыванию `txn_count`.

### Задание 4 – Средняя сумма транзакций по возрастным группам клиентов

Соединить `transactions_data.csv` с `users_data.csv` по `client_id` -> `users.id`.
Для каждого `current_age` вычислить среднюю сумму транзакций (`avg_amount`).
Отсортировать по возрастанию возраста.

### Задание 5 – Доля способов оплаты

Для столбца `use_chip` из `transactions_data.csv` вычислить:
  * количество транзакций (`txn_count`) для каждого способа оплаты
  * долю в процентах (`percent`) от общего числа транзакций.

### Задание 6 – Определение топ-3 транзакций по сумме для каждого клиента

Используя `transactions_data.csv`, нужно для каждого `client_id` определить **три самые крупные транзакции** по полю `amount`.
