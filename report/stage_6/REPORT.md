# 2021-highload-dht

Курсовой проект в рамках обучающей программы "[Технополис](https://polis.mail.ru)" по дисциплине [Высоконагруженные вычисления](https://polis.mail.ru/curriculum/program/discipline/1257/).

## Этап 6. Асинхронный клиент

Этот этап казался самым легким из всех. Однако сложности возникли в процессе.

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

В первой реализации кластер не выдерживал нагрузку в 20000 з/с. Насколько все плохо,
показано на следующих замерах. Замеры приведены после оптимизаций параметров и
пересмотра выбора Executor'ов:
- [wrk2 PUT](profiling/delays_20k/wrk2_async_put.txt)
- [wrk2 GET](profiling/delays_20k/wrk2_async_get.txt)
- [async-profiler CPU](profiling/delays_20k/profiler_cpu_async.html)
- [async-profiler ALLOC](profiling/delays_20k/profiler_alloc_async.html)
- [async-profiler LOCK](profiling/delays_20k/profiler_lock_async.html)

Я догадывался в чем проблема, и мне позже подтвердили догадки на лекции. Забегая вперед,
сервер не реджектит лишнюю нагрузку. Но пока попробуем снизить rate.

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
профиль не изменится. Значит есть какая-то ошибка в реализации. *Вероятно, очередь на
executor переполняется.*

alloc-профайл тоже стал хуже. Число аллокаций возросло. В частности за счет активного
использования Future (на него приходиться 22% профиля).
Сам `HttpClient` стал упоминаться в профайле в 2 раза реже (13% против 31%).
Также теперь приходится маппить запросы one-nio в HttpClient. Это занимает 7% профайла.

#### Пересмотр регулирования очереди на исполнение

С введением в код `CompletableFuture` регулировать очередь на исполнение переданного
ему executor'а стало сложнее: все что он делает, использует для запуска интерфейс
`Executor`, в котором лишь один метод. Но нам не подходит, чтобы этот метод отвергал
запросы на исполнение при своем вызове, поскольку если мы уже решили опрашивать реплики,
то нам нужно, чтобы на каждую из них ушел запрос.

Поэтому в `LimitedExecutorService` была введена внешняя проверка на заполненность очереди.
Непосредственно перед опросом реплик происходит:
 - Если пул потоков, отвечающий за опрос реплик, не может обработать N запросов прямо 
сейчас (очередь не позволяет), значит запрос должен быть отвергнут;
 - Если все же может, то в очереди пула резервируется место, а когда обработка завершится
(неважно как), место в очереди освободиться.

То есть регулировать очередь executor'а теперь берется внешний компонент, который обязан
освободить очередь, когда его задачи будут исполнены на данном executor. \
*Мне эта идея не нравится, но как ещё регулировать очередь, придумать не удалось :(*

Это позволило улучшить производительность и **вновь поднять rate до 20к з/с**:
- [wrk2 PUT](profiling/async_limited/wrk2_async_fixed_put.txt)
- [wrk2 GET](profiling/async_limited/wrk2_async_fixed_get.txt)
- [async-profiler CPU](profiling/async_limited/profiler_cpu_async_fixed.html)
- [async-profiler ALLOC](profiling/async_limited/profiler_alloc_async_fixed.html)
- [async-profiler LOCK](profiling/async_limited/profiler_lock_async_fixed.html)

Скачок в измерениях wrk2 вызван чрезмерной нагрузкой на процессор (у нас теперь много
потоков). Если запускать wrk2 без параллельно запущенного async-profiler, скачок пропадает.

Однако проведенные действия не решили проблему с lock-профайлом. Там очень много
занимает `ForkJoinWorkerThread.run`, которого быть не должно. Теперь точно
лишние запросы отбрасываются, и в очереди на исполнение никто не стоит 
(превышение очереди логируется), однако lock-профайл все равно выглядит плохо.
**С этим вопросом прошу помочь разобраться.**

#### Реализация с несколькими HttpClient

После ревью получен совет попробовать использовать не один HttpClient
на все реплики, а создавать экземпляр для каждой. Данная модификация привела к устранению
скачков wrk2, а также к снижению количества отвергнутых запросов на 1/3 от предыдущей 
реализации:

- [wrk2 PUT](profiling/review_multihttp/wrk2_multihttp_put.txt)
- [wrk2 GET](profiling/review_multihttp/wrk2_multihttp_get.txt)
- [async-profiler CPU](profiling/review_multihttp/profiler_cpu_multihttp.html)
- [async-profiler ALLOC](profiling/review_multihttp/profiler_alloc_multihttp.html)
- [async-profiler LOCK](profiling/review_multihttp/profiler_lock_multihttp.html)