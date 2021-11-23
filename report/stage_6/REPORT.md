# 2021-highload-dht

Курсовой проект в рамках обучающей программы "[Технополис](https://polis.mail.ru)" по дисциплине [Высоконагруженные вычисления](https://polis.mail.ru/curriculum/program/discipline/1257/).

## Этап 6. Асинхронный клиент

Этот этап казался самым легким из всех. Однако сложности возникли в процессе.

После написания кода и тестирования, wrk2 не справлялся со сниженным на предыдущем этапе
rate'ом в 20000 з/с, задержки оказались слишком велики (все персентили были 
примерно равны 11 с).\
*Note:* По факту нагрузку надо умножать в 2 раза, потому что одновременно работает 
wrk2 на GET и PUT.

Проблема снижением rate до 15000 не решилась: предварительно пришлось заменить Executor
для рабочего пула с `ForkJoinPool` на `ThreadPoolExecutor` (поскольку когда он
передается в somethingAsync для `CompletableFuture`, сложно реджектить избыток запросов).

Для пула потоков `java.net.http.HttpClient` был выбран `workStealingPool`, поскольку он
оказался наиболее быстрым в замерах с `fixedThreadPool` и `cachedThreadPool`
у `Executors`.

Конфигурация оборудования:
 - AMD Ryzen 7 5800H 3.2GHz 8 cores / 16 threads
 - RAM 16Gb
 - SSD NVMe
 - Arch Linux x64

Скрипт запуска можно увидеть тут: [wrk2.sh](../../profiling/wrk2.sh).
Все тесты проводились на одних и тех же параметрах.

wrk2 запускался одновременно для PUT и GET запросов. Запуск GET был отложенным
на 10 секунд, чтобы данные уже были записаны PUT.

### [Реализация с блокирующим HttpClient](https://github.com/CRaFT4ik/2021-highload-dht/blob/stage_5/)

Замеры производительности с предыдущего этапа:

- [wrk2 PUT](profiling/prev_stage/wrk2_replication_put.txt)
- [wrk2 GET](profiling/prev_stage/wrk2_replication_get.txt)
- [async-profiler CPU](profiling/prev_stage/profiler_cpu_replication.html)
- [async-profiler ALLOC](profiling/prev_stage/profiler_alloc_replication.html)
- [async-profiler LOCK](profiling/prev_stage/profiler_lock_replication.html) 

### [Реализация с асинхронным HttpClient](https://github.com/CRaFT4ik/2021-highload-dht/blob/stage_6/)

Привожу замеры с rate в 20000 з/с, чтобы увидеть, насколько все плохо. Замеры приведены
после оптимизаций параметров и пересмотра выбора Executor'ов:
- [wrk2 PUT](profiling/delays_20k/wrk2_async_put.txt)
- [wrk2 GET](profiling/delays_20k/wrk2_async_get.txt)
- [async-profiler CPU](profiling/delays_20k/profiler_cpu_async.html)
- [async-profiler ALLOC](profiling/delays_20k/profiler_alloc_async.html)
- [async-profiler LOCK](profiling/delays_20k/profiler_lock_async.html)

Возможно это связано с невидимыми (лично мне) проблемами в реализации. В таком случае
буду рад помощи.

Снижаем rate до 15000 з/с:
- [wrk2 PUT](profiling/async/wrk2_async_put.txt)
- [wrk2 GET](profiling/async/wrk2_async_get.txt)
- [async-profiler CPU](profiling/async/profiler_cpu_async.html)
- [async-profiler ALLOC](profiling/async/profiler_alloc_async.html)
- [async-profiler LOCK](profiling/async/profiler_lock_async.html)

cpu-профайл выглядит нормально. По количеству all-samples даже есть улучшение на 27%
(88к всех вызовов в общей сложности против 65к). Обработка запросов и запись в сокет
занимает большую часть времени, как и положено.

lock-профайл, казалось бы, должен быть почище, чем на предыдущем этапе. Однако видим
много `ForkJoinWorkerThread.run` (3 миллиона совпадений, причем это со сниженным rate).
На предыдущем этапе было всего 2 миллиона совпадений на абсолютно все.
Даже если убрать из кода все `ForkJoinPool` исполнители,
профиль не изменится. Значит есть какая-то ошибка в реализации (я её не вижу).

alloc-профайл тоже стал хуже. Число аллокаций возросло. В частности за счет активного
использования Future (на него приходиться 22% профиля).
Сам `HttpClient` стал упоминаться в профайле в 2 раза реже (13% против 31%).
Также теперь приходится маппить запросы one-nio в HttpClient. Это занимает 7% профайла.