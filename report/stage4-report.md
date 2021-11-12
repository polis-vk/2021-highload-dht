## Отчет №4 "Шардирование"
## Автор: Гаспарян Сократ

<b>Запуск wrk PUT запросы на 64 соединений в 4 потока с rate 10000 запросов в секундну - 1 минута:</b>
<b>Узел с портом 8080</b>
```
Running 1m test @ http://localhost:8080
  4 threads and 64 connections
  Thread calibration: mean lat.: 2.691ms, rate sampling interval: 12ms
  Thread calibration: mean lat.: 2.728ms, rate sampling interval: 13ms
  Thread calibration: mean lat.: 2.666ms, rate sampling interval: 12ms
  Thread calibration: mean lat.: 2.665ms, rate sampling interval: 12ms
  Thread Stats   Avg      Stdev     Max   +/- Stdev
    Latency     2.71ms    4.39ms  60.99ms   92.18%
    Req/Sec     2.62k   691.03    10.91k    85.78%
  Latency Distribution (HdrHistogram - Recorded Latency)
 50.000%    1.41ms
 75.000%    2.32ms
 90.000%    5.68ms
 99.000%   24.03ms
 99.900%   41.98ms
 99.990%   53.31ms
 99.999%   58.27ms
100.000%   61.02ms
...
#[Mean    =        2.709, StdDeviation   =        4.386]
#[Max     =       60.992, Total count    =       499201]
#[Buckets =           27, SubBuckets     =         2048]
----------------------------------------------------------
  599646 requests in 1.00m, 42.91MB read
Requests/sec:   9994.02
Transfer/sec:    732.27KB
```

<b>Узел с портом 8081</b>
```
Running 1m test @ http://localhost:8081
  4 threads and 64 connections
  Thread calibration: mean lat.: 4.260ms, rate sampling interval: 23ms
  Thread calibration: mean lat.: 4.193ms, rate sampling interval: 23ms
  Thread calibration: mean lat.: 4.205ms, rate sampling interval: 23ms
  Thread calibration: mean lat.: 4.301ms, rate sampling interval: 23ms
  Thread Stats   Avg      Stdev     Max   +/- Stdev
    Latency     5.17ms    6.46ms  50.24ms   85.16%
    Req/Sec     2.56k   673.89     6.43k    77.38%
  Latency Distribution (HdrHistogram - Recorded Latency)
 50.000%    1.75ms
 75.000%    7.74ms
 90.000%   14.41ms
 99.000%   28.30ms
 99.900%   38.94ms
 99.990%   44.67ms
 99.999%   48.99ms
100.000%   50.27ms
...
#[Mean    =        5.166, StdDeviation   =        6.464]
#[Max     =       50.240, Total count    =       499068]
#[Buckets =           27, SubBuckets     =         2048]
----------------------------------------------------------
  599516 requests in 1.00m, 49.73MB read
Requests/sec:   9991.87
Transfer/sec:    848.65KB
```

<b>Узел с портом 8082</b>
```
Running 1m test @ http://localhost:8082
  4 threads and 64 connections
  Thread calibration: mean lat.: 5.950ms, rate sampling interval: 34ms
  Thread calibration: mean lat.: 6.539ms, rate sampling interval: 36ms
  Thread calibration: mean lat.: 5.788ms, rate sampling interval: 33ms
  Thread calibration: mean lat.: 5.622ms, rate sampling interval: 32ms
  Thread Stats   Avg      Stdev     Max   +/- Stdev
    Latency     5.64ms    7.24ms  64.48ms   86.56%
    Req/Sec     2.54k   610.93     6.88k    75.68%
  Latency Distribution (HdrHistogram - Recorded Latency)
 50.000%    1.90ms
 75.000%    8.40ms
 90.000%   14.97ms
 99.000%   33.57ms
 99.900%   50.49ms
 99.990%   57.89ms
 99.999%   61.28ms
100.000%   64.51ms
...
#[Mean    =        5.637, StdDeviation   =        7.240]
#[Max     =       64.480, Total count    =       499177]
#[Buckets =           27, SubBuckets     =         2048]
----------------------------------------------------------
  597219 requests in 1.00m, 49.55MB read
Requests/sec:   9954.08
Transfer/sec:    845.70KB
```

<b>Профилирование с помощью async-profiler в течение 15 секунд. FlameGraph для cpu, alloc:</b>

<a href=./resource/profile-html/stage4/stage4-put-cpu-con64-8080.html>PUT запросы CPU для порта 8080</a>

<a href=./resource/profile-html/stage4/stage4-put-mem-con64-8080.html>PUT запросы память для порта 8080</a>

<a href=./resource/profile-html/stage4/stage4-put-cpu-con64-8082.html>PUT запросы CPU для порта 8082</a>

<a href=./resource/profile-html/stage4/stage4-put-mem-con64-8082.html>PUT запросы память для порта 8082</a>

<b>Вывод по результатам:</b>
Как видно из отчетов wrk для PUT запросов использование кластеров снизило производительность по сравнению с одним рабочим сервером, причём для каждого узла. Как видно из отчёта async-profiler часть запросов имеет промах по локальному хранилищу и отправляет запрос на другой сервер из топологии.


<b>Запуск wrk GET запросы на 64 соединений в 4 потока с rate 10000 запросов в секундну - 1 минута:</b>
<b>Узел с портом 8080</b>
```
Running 1m test @ http://localhost:8080
  4 threads and 64 connections
  Thread calibration: mean lat.: 8.240ms, rate sampling interval: 48ms
  Thread calibration: mean lat.: 8.338ms, rate sampling interval: 49ms
  Thread calibration: mean lat.: 8.347ms, rate sampling interval: 49ms
  Thread calibration: mean lat.: 8.086ms, rate sampling interval: 48ms
  Thread Stats   Avg      Stdev     Max   +/- Stdev
    Latency     8.82ms    9.46ms  54.08ms   80.52%
    Req/Sec     2.53k   684.62     4.85k    62.27%
  Latency Distribution (HdrHistogram - Recorded Latency)
 50.000%    3.75ms
 75.000%   15.52ms
 90.000%   23.82ms
 99.000%   33.12ms
 99.900%   38.81ms
 99.990%   45.66ms
 99.999%   50.01ms
100.000%   54.11ms
...
#[Mean    =        8.817, StdDeviation   =        9.462]
#[Max     =       54.080, Total count    =       499153]
#[Buckets =           27, SubBuckets     =         2048]
----------------------------------------------------------
  599605 requests in 1.00m, 46.44MB read
Requests/sec:   9992.99
Transfer/sec:    792.62KB
```

<b>Узел с портом 8081</b>
```
Running 1m test @ http://localhost:8081
  4 threads and 64 connections
  Thread calibration: mean lat.: 10.416ms, rate sampling interval: 63ms
  Thread calibration: mean lat.: 10.391ms, rate sampling interval: 63ms
  Thread calibration: mean lat.: 10.360ms, rate sampling interval: 63ms
  Thread calibration: mean lat.: 10.263ms, rate sampling interval: 62ms
  Thread Stats   Avg      Stdev     Max   +/- Stdev
    Latency    10.95ms   12.71ms  63.52ms   80.19%
    Req/Sec     2.52k   723.92     4.57k    63.93%
  Latency Distribution (HdrHistogram - Recorded Latency)
 50.000%    3.10ms
 75.000%   19.77ms
 90.000%   31.69ms
 99.000%   44.03ms
 99.900%   53.92ms
 99.990%   59.52ms
 99.999%   62.24ms
100.000%   63.55ms
...
#[Mean    =       10.946, StdDeviation   =       12.715]
#[Max     =       63.520, Total count    =       498792]
#[Buckets =           27, SubBuckets     =         2048]
----------------------------------------------------------
  599240 requests in 1.00m, 53.24MB read
Requests/sec:   9987.75
Transfer/sec:      0.89MB
```

<b>Узел с портом 8082</b>
```
Running 1m test @ http://localhost:8082
  4 threads and 64 connections
  Thread calibration: mean lat.: 11.319ms, rate sampling interval: 68ms
  Thread calibration: mean lat.: 11.338ms, rate sampling interval: 68ms
  Thread calibration: mean lat.: 11.506ms, rate sampling interval: 68ms
  Thread calibration: mean lat.: 11.425ms, rate sampling interval: 68ms
  Thread Stats   Avg      Stdev     Max   +/- Stdev
    Latency    11.26ms   12.35ms  56.61ms   80.37%
    Req/Sec     2.52k   680.40     4.30k    62.62%
  Latency Distribution (HdrHistogram - Recorded Latency)
 50.000%    5.10ms
 75.000%   19.95ms
 90.000%   31.23ms
 99.000%   42.59ms
 99.900%   49.44ms
 99.990%   53.41ms
 99.999%   55.78ms
100.000%   56.64ms
...
#[Mean    =       11.257, StdDeviation   =       12.353]
#[Max     =       56.608, Total count    =       499598]
#[Buckets =           27, SubBuckets     =         2048]
----------------------------------------------------------
  599636 requests in 1.00m, 53.29MB read
Requests/sec:   9993.95
Transfer/sec:      0.89MB
```

<b>Профилирование с помощью async-profiler в течение 15 секунд. FlameGraph для cpu, alloc:</b>

<a href=./resource/profile-html/stage4/stage4-get-cpu-con64-8080.html>GET запросы CPU для порта 8080</a>

<a href=./resource/profile-html/stage4/stage4-get-mem-con64-8080.html>GET запросы память для порта 8080</a>

<a href=./resource/profile-html/stage4/stage4-get-cpu-con64-8082.html>GET запросы CPU для порта 8082</a>

<a href=./resource/profile-html/stage4/stage4-get-mem-con64-8082.html>GET запросы память для порта 8082</a>

<b>Вывод по результатам:</b>
Для GET запросов ситуация такая же, как и для PUT - производительность для каждого узла снизилась из-за промахов запросов к локальному хранилищу.




<h4>Общий вывод и оптимизации:</h4>
Нужно выбрать определенную хэш функцию от которой зависит разрешимость коллизии при запросах на сервер. Ещё одним способом оптимизации является использование другого протокола передачи данных между узлами, например, можно использовать просто сокетное подключение и определить несколько команд для обмена данными между узлами.
 