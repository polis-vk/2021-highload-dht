# 2021-highload-dht

Курсовой проект в рамках обучающей программы "[Технополис](https://polis.mail.ru)" по дисциплине [Высоконагруженные вычисления](https://polis.mail.ru/curriculum/program/discipline/1257/).

## Этап 5. Репликация

После завершения написания кода для данного этапа, при анализе предварительных
результатов профилирования, стало понятно, что значения, используемые для нагрузки с помощью
wrk2 ранее, слишком высоки для текущей реализации: появились огромные скачки между
перцентилями (вероятно, сборщик мусора не поспевал), число нет отвергнутых запросов
было велико.

Ранее использовались следующие параметры wrk2:
```bash
wrk2 -c 128 -t 8 -d 125s -R 70000 -L -s ./scripts/put.lua http://localhost:8080/ 
wrk2 -c 128 -t 8 -d 125s -R 70000 -L -s ./scripts/get.lua http://localhost:8080/ 
```

Теперь используются следующие:
```bash
wrk2 -c 128 -t 8 -d 125s -R 20000 -L -s ./scripts/put.lua http://localhost:8080/ 
wrk2 -c 128 -t 8 -d 125s -R 20000 -L -s ./scripts/get.lua http://localhost:8080/ 
```

Конфигурация оборудования:
 - AMD Ryzen 7 5800H 3.2GHz 8 cores / 16 threads
 - RAM 16Gb
 - SSD NVMe
 - Arch Linux x64

Скрипт запуска можно увидеть тут: [wrk2.sh](../../profiling/wrk2.sh).
Все тесты проводились на одних и тех же параметрах. Для реализации предыдущего этапа
замеры были проведены повторно с учетом изменений параметров wrk2.

wrk2 запускался одновременно для PUT и GET запросов. Запуск GET был отложенным
на 10 секунд, чтобы данные уже были записаны PUT.

Для кластерной реализации wrk2 работал только по одному узлу, используя набор ключей
всех нод.

### [Реализация без репликации](https://github.com/CRaFT4ik/2021-highload-dht/blob/stage_4/)

Возьмем сервер, имеющийся на конец четвертого этапа, и произведем его профилирование.

- [wrk2 PUT](profiling/prev_stage/wrk2_stage4_put.txt)
- [wrk2 GET](profiling/prev_stage/wrk2_stage4_get.txt)
- [async-profiler CPU](profiling/prev_stage/profiler_cpu_stage4.html)
- [async-profiler ALLOC](profiling/prev_stage/profiler_alloc_stage4.html)
- [async-profiler LOCK](profiling/prev_stage/profiler_lock_stage4.html)

Видим довольно низкие задержки.
Однако сейчас кластер работает без репликации, и сбой на одном узле приведет к недоступности части данных. 

### [Реализация с репликацией](https://github.com/CRaFT4ik/2021-highload-dht/blob/stage_5/)

В соответствии с заданием, реализовано реплицирование нод.
Выбор кандидатов в реплики производится детерминированно в установленном порядке,
с использованием реализованного на предыдущем этапе consistent hash router
с многочисленными virtual nodes, что предполагает равномерность распределения.

Путь запроса, который приходит на ноду, построен следующим образом:
- Текущая нода определяет, предназначен ли запрос ей: она должна быть в числе реплик,
которое задается параметром `from`;
- Если запрос предназначен ей, нода берется за обработку запроса в своем рабочем 
пуле потоков;
- Если запрос не предназначен текущей ноде, она проксирует его на ноду-владельца
ключа запроса.

#### Про опрос реплик
Рабочий пул потоков построен на `ForkJoinPool`. Возникла идея выполнять опрос реплик
асинхронно с использованием преимуществ `ForkJoinTask`:

Создается master-задача на обработку запроса, которая при исполнении делит себя на более
мелкие slave-задачи, каждая из которых занимается опросом конкретной ноды.
master-задача не ждет завершения опроса всех нод `from`, а только части `ask`. Как только
информация получена, она сразу же синхронизируется и отсылается в сокет. Но
запрос на количестве `from - ask` нод тоже выполняется, в фоновом режиме и без ожидания.

#### Про значения параметров `from` и `ask` по-умолчанию
Значение по-умолчанию для `from` равняется общему количеству узлов в кластере.
Параметр `ask` выбирается как кворум от значения `from`.
Для текущих тестов ask/from = 2/3.

#### Про синхронизацию нод
В текущей реализации не сделана починка и синхронизация нод в случае, если они
присылают различные ответы.\
Стоит отметить, что для учебного проекта это не критично.\
Также стоит отметить, что в классе `TwoNodeTest` реализован тест (ради интереса) для проверки
работоспособности этой возможности.

#### Про выбор наиболее свежего ответа
В реализацию `LsmDAO` для каждого ключа введен параметр `timestamp`. Параметр хранится
рядом с ключом и значением и изменяется в момент перезаписи данных.

Ноды передают `timestamp` друг-другу через специальный HTTP-заголовок,
не записывая его в body.

#### Профилирование

 - [wrk2 PUT](profiling/replication/wrk2_replication_put.txt)
 - [wrk2 GET](profiling/replication/wrk2_replication_get.txt)
 - [async-profiler CPU](profiling/replication/profiler_cpu_replication.html)
 - [async-profiler ALLOC](profiling/replication/profiler_alloc_replication.html)
 - [async-profiler LOCK](profiling/replication/profiler_lock_replication.html)

Из результатов wrk2 видим увеличение как количества не 200-x и 300-x ответов,
так и среднего времени ответа. При этом всем был снижен rate для get и put методов
с 70000 до 20000.

Значительное количество времени (17%) CPU проводит на опросе реплик (метод `handleRemotely`).
До этого в предыдущих реализациях самым проблемным местом в профиле являлась запись в сокет.
Сейчас количество таких записей резко увеличилось в связи с появлением реплик. Ухудшения
ожидаемы.

alloc-профайл стал лучше в том плане, что теперь там совсем нет проксирования. Все запросы
попадают на ноду, которая входит в число реплик всех других нод. Поэтому обработка начинается
сразу же (нет смысла перенаправлять запрос, он в любом случае придет сюда же).

lock-профайл стал хуже в том плане, что теперь много локов происходит на ожидании
ответа реплик посредством HttpClient. Наверное, это основополагающий фактор тормозов.

Возможно улучшить текущую реализацию, если освобождать рабочие потоки путем введения
неблокирующего HttpClient. Это даст возможность не использовать `ForkJoinTask`'и, которые
забивают рабочий пул потоков, даст возможность опрашивать сразу несколько `HttpClient`'ов
в одном текущем рабочем потоке, должно улучшить текущую ситуацию с производительностью.