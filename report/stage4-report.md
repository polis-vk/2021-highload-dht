## Отчет №4 "Шардирование"
## Автор: Гаспарян Сократ

### Тест №1 Размер интервала - 32768
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



### Тест №2 Размер интервала - 32768
<b>Запуск wrk PUT запросы на 128 соединений в 4 потока с rate 10000 запросов в секундну - 3 минуты:</b>
<b>Узел с портом 8080</b>
```
Running 3m test @ http://localhost:8080
  4 threads and 128 connections
  Thread calibration: mean lat.: 1.775ms, rate sampling interval: 10ms
  Thread calibration: mean lat.: 1.811ms, rate sampling interval: 10ms
  Thread calibration: mean lat.: 1.906ms, rate sampling interval: 10ms
  Thread calibration: mean lat.: 1.794ms, rate sampling interval: 10ms
  Thread Stats   Avg      Stdev     Max   +/- Stdev
    Latency     2.08ms    2.76ms  54.98ms   92.90%
    Req/Sec     2.65k   630.03    12.67k    85.56%
  Latency Distribution (HdrHistogram - Recorded Latency)
 50.000%    1.38ms
 75.000%    2.08ms
 90.000%    3.85ms
 99.000%   14.96ms
 99.900%   30.85ms
 99.990%   41.95ms
 99.999%   48.03ms
100.000%   55.01ms
...
#[Mean    =        2.082, StdDeviation   =        2.760]
#[Max     =       54.976, Total count    =      1698406]
#[Buckets =           27, SubBuckets     =         2048]
----------------------------------------------------------
  1799274 requests in 3.00m, 128.71MB read
Requests/sec:   9995.91
Transfer/sec:    732.24KB
```

<b>Узел с портом 8081</b>
```
Running 3m test @ http://localhost:8081
  4 threads and 128 connections
  Thread calibration: mean lat.: 4.220ms, rate sampling interval: 26ms
  Thread calibration: mean lat.: 4.679ms, rate sampling interval: 28ms
  Thread calibration: mean lat.: 4.346ms, rate sampling interval: 27ms
  Thread calibration: mean lat.: 4.865ms, rate sampling interval: 26ms
  Thread Stats   Avg      Stdev     Max   +/- Stdev
    Latency     5.99ms    7.28ms  60.83ms   83.61%
    Req/Sec     2.56k   733.38     6.19k    72.81%
  Latency Distribution (HdrHistogram - Recorded Latency)
 50.000%    2.02ms
 75.000%    8.95ms
 90.000%   17.22ms
 99.000%   30.03ms
 99.900%   39.17ms
 99.990%   47.46ms
 99.999%   54.65ms
100.000%   60.86ms
...
#[Mean    =        5.987, StdDeviation   =        7.281]
#[Max     =       60.832, Total count    =      1698120]
#[Buckets =           27, SubBuckets     =         2048]
----------------------------------------------------------
  1796691 requests in 3.00m, 149.04MB read
Requests/sec:   9981.57
Transfer/sec:    847.89KB
```

<b>Узел с портом 8082</b>
```
Running 3m test @ http://localhost:8082
  4 threads and 128 connections
  Thread calibration: mean lat.: 6.775ms, rate sampling interval: 38ms
  Thread calibration: mean lat.: 5.522ms, rate sampling interval: 32ms
  Thread calibration: mean lat.: 6.978ms, rate sampling interval: 37ms
  Thread calibration: mean lat.: 5.214ms, rate sampling interval: 30ms
  Thread Stats   Avg      Stdev     Max   +/- Stdev
    Latency     7.15ms    7.75ms  72.13ms   84.14%
    Req/Sec     2.55k   710.10     7.45k    68.24%
  Latency Distribution (HdrHistogram - Recorded Latency)
 50.000%    3.49ms
 75.000%   11.11ms
 90.000%   17.92ms
 99.000%   33.02ms
 99.900%   53.15ms
 99.990%   61.06ms
 99.999%   66.56ms
100.000%   72.19ms
...
#[Mean    =        7.146, StdDeviation   =        7.748]
#[Max     =       72.128, Total count    =      1698346]
#[Buckets =           27, SubBuckets     =         2048]
----------------------------------------------------------
  1794612 requests in 3.00m, 148.91MB read
Requests/sec:   9970.18
Transfer/sec:    847.12KB
```

<b>Профилирование с помощью async-profiler в течение 45 секунд. FlameGraph для cpu, alloc:</b>

<a href=./resource/profile-html/stage4/stage4-put-cpu-con128-8080.html>PUT запросы CPU для порта 8080</a>

<a href=./resource/profile-html/stage4/stage4-put-mem-con128-8080.html>PUT запросы память для порта 8080</a>

<a href=./resource/profile-html/stage4/stage4-put-cpu-con128-8081.html>PUT запросы CPU для порта 8081</a>

<a href=./resource/profile-html/stage4/stage4-put-mem-con128-8081.html>PUT запросы память для порта 8081</a>

<a href=./resource/profile-html/stage4/stage4-put-cpu-con128-8082.html>PUT запросы CPU для порта 8082</a>

<a href=./resource/profile-html/stage4/stage4-put-mem-con128-8082.html>PUT запросы память для порта 8082</a>

<b>Вывод по результатам:</b>

Результат wrk для PUT запросов при более длительном нагрузочном тестировании показывает почти такой же результат, как и при менее длительном. При равномерном распределений интервалов для ConsistentHash при таких тестовых запросах результат, вероятно, будет одинаковый для любого времени. 


<b>Запуск wrk GET запросы на 128 соединений в 4 потока с rate 15000 запросов в секундну - 3 минуты:</b>
<b>Узел с портом 8080</b>
```
Running 3m test @ http://localhost:8080
  4 threads and 128 connections
  Thread calibration: mean lat.: 5.975ms, rate sampling interval: 33ms
  Thread calibration: mean lat.: 5.758ms, rate sampling interval: 31ms
  Thread calibration: mean lat.: 5.782ms, rate sampling interval: 32ms
  Thread calibration: mean lat.: 5.801ms, rate sampling interval: 32ms
  Thread Stats   Avg      Stdev     Max   +/- Stdev
    Latency     6.44ms    7.19ms  43.97ms   79.75%
    Req/Sec     2.54k   671.93     5.45k    69.07%
  Latency Distribution (HdrHistogram - Recorded Latency)
 50.000%    1.95ms
 75.000%   11.57ms
 90.000%   17.98ms
 99.000%   25.77ms
 99.900%   31.02ms
 99.990%   35.78ms
 99.999%   39.94ms
100.000%   44.00ms
...
#[Mean    =        6.443, StdDeviation   =        7.193]
#[Max     =       43.968, Total count    =      1698494]
#[Buckets =           27, SubBuckets     =         2048]
----------------------------------------------------------
  1799264 requests in 3.00m, 140.26MB read
Requests/sec:   9995.81
Transfer/sec:    797.93KB
```

<b>Узел с портом 8081</b>
```
Running 3m test @ http://localhost:8081
  4 threads and 128 connections
  Thread calibration: mean lat.: 11.659ms, rate sampling interval: 60ms
  Thread calibration: mean lat.: 11.562ms, rate sampling interval: 59ms
  Thread calibration: mean lat.: 13.347ms, rate sampling interval: 64ms
  Thread calibration: mean lat.: 11.653ms, rate sampling interval: 60ms
  Thread Stats   Avg      Stdev     Max   +/- Stdev
    Latency    12.08ms   11.87ms  55.46ms   79.86%
    Req/Sec     2.52k   759.63     4.53k    60.83%
  Latency Distribution (HdrHistogram - Recorded Latency)
 50.000%    7.72ms
 75.000%   21.10ms
 90.000%   30.27ms
 99.000%   41.44ms
 99.900%   46.88ms
 99.990%   50.43ms
 99.999%   53.06ms
100.000%   55.49ms
...
#[Mean    =       12.075, StdDeviation   =       11.875]
#[Max     =       55.456, Total count    =      1698325]
#[Buckets =           27, SubBuckets     =         2048]
----------------------------------------------------------
  1796605 requests in 3.00m, 160.57MB read
Requests/sec:   9981.31
Transfer/sec:      0.89MB
```

<b>Узел с портом 8082</b>
```
Running 3m test @ http://localhost:8082
  4 threads and 128 connections
  Thread calibration: mean lat.: 11.769ms, rate sampling interval: 60ms
  Thread calibration: mean lat.: 10.203ms, rate sampling interval: 56ms
  Thread calibration: mean lat.: 10.138ms, rate sampling interval: 56ms
  Thread calibration: mean lat.: 10.357ms, rate sampling interval: 57ms
  Thread Stats   Avg      Stdev     Max   +/- Stdev
    Latency    11.51ms   11.58ms  56.67ms   80.14%
    Req/Sec     2.52k   765.36     4.78k    61.12%
  Latency Distribution (HdrHistogram - Recorded Latency)
 50.000%    6.84ms
 75.000%   20.19ms
 90.000%   29.39ms
 99.000%   40.48ms
 99.900%   46.40ms
 99.990%   49.89ms
 99.999%   53.15ms
100.000%   56.70ms
...
#[Mean    =       11.511, StdDeviation   =       11.579]
#[Max     =       56.672, Total count    =      1698394]
#[Buckets =           27, SubBuckets     =         2048]
----------------------------------------------------------
  1796961 requests in 3.00m, 160.63MB read
Requests/sec:   9983.21
Transfer/sec:      0.89MB
```

<b>Профилирование с помощью async-profiler в течение 45 секунд. FlameGraph для cpu, alloc:</b>

<a href=./resource/profile-html/stage4/stage4-get-cpu-con128-8080.html>GET запросы CPU для порта 8080</a>

<a href=./resource/profile-html/stage4/stage4-get-mem-con128-8080.html>GET запросы память для порта 8080</a>

<a href=./resource/profile-html/stage4/stage4-get-cpu-con128-8081.html>GET запросы CPU для порта 8081</a>

<a href=./resource/profile-html/stage4/stage4-get-mem-con128-8081.html>GET запросы память для порта 8081</a>

<a href=./resource/profile-html/stage4/stage4-get-cpu-con128-8082.html>GET запросы CPU для порта 8082</a>

<a href=./resource/profile-html/stage4/stage4-get-mem-con128-8082.html>GET запросы память для порта 8082</a>


<b>Вывод по результатам:</b>

Результат wrk для GET запросов при длительном тестировании не изменился.


### Тест №3 Размер интервала - 1024
<b>Запуск wrk PUT запросы на 128 соединений в 4 потока с rate 10000 запросов в секундну - 3 минуты:</b>
<b>Узел с портом 8080</b>
```
Running 3m test @ http://localhost:8080
  4 threads and 128 connections
  Thread calibration: mean lat.: 2.196ms, rate sampling interval: 10ms
  Thread calibration: mean lat.: 2.289ms, rate sampling interval: 10ms
  Thread calibration: mean lat.: 2.152ms, rate sampling interval: 10ms
  Thread calibration: mean lat.: 2.299ms, rate sampling interval: 10ms
  Thread Stats   Avg      Stdev     Max   +/- Stdev
    Latency     1.28ms    1.65ms  41.31ms   96.62%
    Req/Sec     2.64k   405.17    10.33k    91.50%
  Latency Distribution (HdrHistogram - Recorded Latency)
 50.000%    1.04ms
 75.000%    1.42ms
 90.000%    1.90ms
 99.000%    8.67ms
 99.900%   22.53ms
 99.990%   33.50ms
 99.999%   38.33ms
100.000%   41.34ms
...
#[Mean    =        1.280, StdDeviation   =        1.648]
#[Max     =       41.312, Total count    =      1698390]
#[Buckets =           27, SubBuckets     =         2048]
----------------------------------------------------------
  1799271 requests in 3.00m, 128.73MB read
Requests/sec:   9995.88
Transfer/sec:    732.30KB
```

<b>Узел с портом 8081</b>
```
Running 3m test @ http://localhost:8081
  4 threads and 128 connections
  Thread calibration: mean lat.: 1.634ms, rate sampling interval: 10ms
  Thread calibration: mean lat.: 1.572ms, rate sampling interval: 10ms
  Thread calibration: mean lat.: 2.076ms, rate sampling interval: 10ms
  Thread calibration: mean lat.: 2.009ms, rate sampling interval: 10ms
  Thread Stats   Avg      Stdev     Max   +/- Stdev
    Latency     1.67ms    1.95ms  49.34ms   93.37%
    Req/Sec     2.64k   746.27    11.44k    75.22%
  Latency Distribution (HdrHistogram - Recorded Latency)
 50.000%    1.24ms
 75.000%    1.78ms
 90.000%    2.65ms
 99.000%   10.57ms
 99.900%   21.97ms
 99.990%   33.44ms
 99.999%   44.06ms
100.000%   49.38ms
...
#[Mean    =        1.666, StdDeviation   =        1.946]
#[Max     =       49.344, Total count    =      1698375]
#[Buckets =           27, SubBuckets     =         2048]
----------------------------------------------------------
  1794644 requests in 3.00m, 148.88MB read
Requests/sec:   9970.35
Transfer/sec:    846.98KB
```

<b>Узел с портом 8082</b>
```
Running 3m test @ http://localhost:8082
  4 threads and 128 connections
  Thread calibration: mean lat.: 1.787ms, rate sampling interval: 10ms
  Thread calibration: mean lat.: 2.293ms, rate sampling interval: 10ms
  Thread calibration: mean lat.: 1.833ms, rate sampling interval: 10ms
  Thread calibration: mean lat.: 2.593ms, rate sampling interval: 10ms
  Thread Stats   Avg      Stdev     Max   +/- Stdev
    Latency     3.15ms    4.18ms  46.75ms   89.66%
    Req/Sec     2.66k     1.04k    9.78k    83.02%
  Latency Distribution (HdrHistogram - Recorded Latency)
 50.000%    1.70ms
 75.000%    2.89ms
 90.000%    7.52ms
 99.000%   21.66ms
 99.900%   30.80ms
 99.990%   37.41ms
 99.999%   43.81ms
100.000%   46.78ms
...
#[Mean    =        3.153, StdDeviation   =        4.176]
#[Max     =       46.752, Total count    =      1698373]
#[Buckets =           27, SubBuckets     =         2048]
----------------------------------------------------------
  1794641 requests in 3.00m, 148.89MB read
Requests/sec:   9970.36
Transfer/sec:    847.03KB
```

<b>Профилирование с помощью async-profiler в течение 45 секунд. FlameGraph для cpu, alloc:</b>

<a href=./resource/profile-html/stage4/stage4-put-cpu-con128-8080-int.html>PUT запросы CPU для порта 8080</a>

<a href=./resource/profile-html/stage4/stage4-put-mem-con128-8080-int.html>PUT запросы память для порта 8080</a>

<a href=./resource/profile-html/stage4/stage4-put-cpu-con128-8081-int.html>PUT запросы CPU для порта 8081</a>

<a href=./resource/profile-html/stage4/stage4-put-mem-con128-8081-int.html>PUT запросы память для порта 8081</a>

<a href=./resource/profile-html/stage4/stage4-put-cpu-con128-8082-int.html>PUT запросы CPU для порта 8082</a>

<a href=./resource/profile-html/stage4/stage4-put-mem-con128-8082-int.html>PUT запросы память для порта 8082</a>


<b>Вывод по результатам:</b>
Исходя из результатов отчета wrk, уменьшение размера интервала для ConsistentHash у PUT запросов, увеличило производительность. Так как интервал стал меньше - частота запросов в локальное хранилище могло увеличилось и следовательно уменьшелось количество промахов в локальное хранилище при запросах на сервер.


<b>Запуск wrk GET запросы на 128 соединений в 4 потока с rate 10000 запросов в секундну - 3 минуты:</b>
<b>Узел с портом 8080</b>
```
Running 3m test @ http://localhost:8080
  4 threads and 128 connections
  Thread calibration: mean lat.: 7.701ms, rate sampling interval: 56ms
  Thread calibration: mean lat.: 7.696ms, rate sampling interval: 55ms
  Thread calibration: mean lat.: 7.637ms, rate sampling interval: 55ms
  Thread calibration: mean lat.: 7.739ms, rate sampling interval: 55ms
  Thread Stats   Avg      Stdev     Max   +/- Stdev
    Latency    10.17ms   12.06ms  64.22ms   79.73%
    Req/Sec     2.52k   748.28     4.71k    63.23%
  Latency Distribution (HdrHistogram - Recorded Latency)
 50.000%    2.04ms
 75.000%   18.78ms
 90.000%   30.33ms
 99.000%   40.51ms
 99.900%   45.76ms
 99.990%   53.69ms
 99.999%   61.02ms
100.000%   64.25ms
...
#[Mean    =       10.169, StdDeviation   =       12.059]
#[Max     =       64.224, Total count    =      1698345]
#[Buckets =           27, SubBuckets     =         2048]
----------------------------------------------------------
  1799086 requests in 3.00m, 140.26MB read
Requests/sec:   9994.86
Transfer/sec:    797.91KB
```

<b>Узел с портом 8081</b>
```
Running 3m test @ http://localhost:8081
  4 threads and 128 connections
  Thread calibration: mean lat.: 7.236ms, rate sampling interval: 53ms
  Thread calibration: mean lat.: 7.317ms, rate sampling interval: 53ms
  Thread calibration: mean lat.: 7.270ms, rate sampling interval: 53ms
  Thread calibration: mean lat.: 7.260ms, rate sampling interval: 53ms
  Thread Stats   Avg      Stdev     Max   +/- Stdev
    Latency    10.66ms   13.14ms  66.62ms   80.40%
    Req/Sec     2.52k   730.10     4.58k    65.25%
  Latency Distribution (HdrHistogram - Recorded Latency)
 50.000%    2.09ms
 75.000%   19.07ms
 90.000%   32.83ms
 99.000%   45.57ms
 99.900%   54.17ms
 99.990%   58.43ms
 99.999%   61.92ms
100.000%   66.69ms
...
#[Mean    =       10.664, StdDeviation   =       13.143]
#[Max     =       66.624, Total count    =      1698271]
#[Buckets =           27, SubBuckets     =         2048]
----------------------------------------------------------
  1799161 requests in 3.00m, 160.81MB read
Requests/sec:   9995.24
Transfer/sec:      0.89MB

```

<b>Узел с портом 8082</b>
```
Running 3m test @ http://localhost:8082
  4 threads and 128 connections
  Thread calibration: mean lat.: 7.036ms, rate sampling interval: 51ms
  Thread calibration: mean lat.: 7.046ms, rate sampling interval: 51ms
  Thread calibration: mean lat.: 7.032ms, rate sampling interval: 51ms
  Thread calibration: mean lat.: 7.007ms, rate sampling interval: 50ms
  Thread Stats   Avg      Stdev     Max   +/- Stdev
    Latency    10.78ms   13.44ms  68.22ms   80.68%
    Req/Sec     2.53k   751.21     4.72k    65.47%
  Latency Distribution (HdrHistogram - Recorded Latency)
 50.000%    2.02ms
 75.000%   19.22ms
 90.000%   33.41ms
 99.000%   46.91ms
 99.900%   57.69ms
 99.990%   62.49ms
 99.999%   66.37ms
100.000%   68.29ms
...
#[Mean    =       10.782, StdDeviation   =       13.440]
#[Max     =       68.224, Total count    =      1698412]
#[Buckets =           27, SubBuckets     =         2048]
----------------------------------------------------------
  1799281 requests in 3.00m, 160.82MB read
Requests/sec:   9995.98
Transfer/sec:      0.89MB
```

<b>Профилирование с помощью async-profiler в течение 45 секунд. FlameGraph для cpu, alloc:</b>

<a href=./resource/profile-html/stage4/stage4-get-cpu-con128-8080-int.html>GET запросы CPU для порта 8080</a>

<a href=./resource/profile-html/stage4/stage4-get-mem-con128-8080-int.html>GET запросы память для порта 8080</a>

<a href=./resource/profile-html/stage4/stage4-get-cpu-con128-8081-int.html>GET запросы CPU для порта 8081</a>

<a href=./resource/profile-html/stage4/stage4-get-mem-con128-8081-int.html>GET запросы память для порта 8081</a>

<a href=./resource/profile-html/stage4/stage4-get-cpu-con128-8082-int.html>GET запросы CPU для порта 8082</a>

<a href=./resource/profile-html/stage4/stage4-get-mem-con128-8082-int.html>GET запросы память для порта 8082</a>

<b>Вывод по результатам:</b>
Для GET запросов изменение размера интервала не дало улучшений.


<h4>Общий вывод и оптимизации:</h4>
Способ разбития узлов кластера влияет на производительность. Следовательно один из способов повышения производительности - изменение алгоритма разбития нод на интервалы. Например, можно проанализировать данные и разбивать ноды на интервалы, исходя из статистических данных(т.е. на основе гистограммы определить к какому серверу по заданному ключу обращается пользователь и пересчитывать интервал) для уменьшения промахов обращения к локальному хранилищу. Ещё одним способом оптимизации является использование другого протокола передачи данных между узлами, например, можно использовать просто сокетное подключение и определить несколько команд для обмена данными между узлами.
 