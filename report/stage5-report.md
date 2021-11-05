## Отчет №5 "Репликация"
## Автор: Гаспарян Сократ

### Результаты нагрузочного тестирования с репликацией
<b>Запуск wrk PUT запросы на 128 соединений в 2 потока с rate 10000 запросов в секундну - 1 минута:</b>
<b>Узел с портом 8080</b>
```
Running 1m test @ http://localhost:8080
  2 threads and 128 connections
  Thread calibration: mean lat.: 11.073ms, rate sampling interval: 64ms
  Thread calibration: mean lat.: 11.288ms, rate sampling interval: 65ms
  Thread Stats   Avg      Stdev     Max   +/- Stdev
    Latency    10.21ms   11.11ms  93.57ms   83.23%
    Req/Sec     5.04k     1.24k    9.65k    67.75%
  Latency Distribution (HdrHistogram - Recorded Latency)
 50.000%    5.74ms
 75.000%   16.22ms
 90.000%   26.75ms
 99.000%   44.74ms
 99.900%   71.17ms
 99.990%   88.57ms
 99.999%   91.71ms
100.000%   93.63ms
...
#[Mean    =       10.213, StdDeviation   =       11.113]
#[Max     =       93.568, Total count    =       496234]
#[Buckets =           27, SubBuckets     =         2048]
----------------------------------------------------------
  597754 requests in 1.00m, 51.88MB read
Requests/sec:   9962.96
Transfer/sec:      0.86MB
```

<b>Узел с портом 8081</b>
```
Running 1m test @ http://localhost:8081
  2 threads and 128 connections
  Thread calibration: mean lat.: 11.359ms, rate sampling interval: 58ms
  Thread calibration: mean lat.: 10.388ms, rate sampling interval: 55ms
  Thread Stats   Avg      Stdev     Max   +/- Stdev
    Latency    10.92ms   11.24ms 104.83ms   82.50%
    Req/Sec     5.05k     1.37k    9.41k    67.64%
  Latency Distribution (HdrHistogram - Recorded Latency)
 50.000%    6.41ms
 75.000%   17.58ms
 90.000%   28.22ms
 99.000%   43.71ms
 99.900%   57.79ms
 99.990%   71.42ms
 99.999%   90.88ms
100.000%  104.89ms
...
#[Mean    =       10.922, StdDeviation   =       11.244]
#[Max     =      104.832, Total count    =       496822]
#[Buckets =           27, SubBuckets     =         2048]
----------------------------------------------------------
  594247 requests in 1.00m, 37.97MB read
Requests/sec:   9904.15
Transfer/sec:    648.03KB
```

<b>Узел с портом 8082</b>
```
Running 1m test @ http://localhost:8082
  2 threads and 128 connections
  Thread calibration: mean lat.: 9.655ms, rate sampling interval: 51ms
  Thread calibration: mean lat.: 11.224ms, rate sampling interval: 55ms
  Thread Stats   Avg      Stdev     Max   +/- Stdev
    Latency    11.59ms   11.38ms  97.60ms   82.35%
    Req/Sec     5.05k     1.47k    9.82k    67.31%
  Latency Distribution (HdrHistogram - Recorded Latency)
 50.000%    7.35ms
 75.000%   18.46ms
 90.000%   29.09ms
 99.000%   44.70ms
 99.900%   56.51ms
 99.990%   67.14ms
 99.999%   85.89ms
100.000%   97.66ms
...
#[Mean    =       11.585, StdDeviation   =       11.385]
#[Max     =       97.600, Total count    =       496801]
#[Buckets =           27, SubBuckets     =         2048]
----------------------------------------------------------
  594232 requests in 1.00m, 51.57MB read
Requests/sec:   9904.00
Transfer/sec:      0.86MB
```

<b>Профилирование с помощью async-profiler в течение 20 секунд. FlameGraph для cpu, alloc:</b>

<a href=./resource/profile-html/stage5/stage5-put-cpu-node8080.html>PUT запросы CPU для порта 8080</a>

<a href=./resource/profile-html/stage5/stage5-put-mem-node8080.html>PUT запросы память для порта 8080</a>

<a href=./resource/profile-html/stage5/stage5-put-cpu-node8081.html>PUT запросы CPU для порта 8081</a>

<a href=./resource/profile-html/stage5/stage5-put-mem-node8081.html>PUT запросы память для порта 8081</a>

<a href=./resource/profile-html/stage5/stage5-put-cpu-node8082.html>PUT запросы CPU для порта 8082</a>

<a href=./resource/profile-html/stage5/stage5-put-mem-node8082.html>PUT запросы память для порта 8082</a>

<b>Вывод по результатам:</b>
Как видно из резултатов wrk, каждый узел нагружается одинаково. После добавления поддержки репликации производительность для PUT запросов снизилась, если сравнить результаты отчетов по добавлению шардирования. Исходя из отчетов профилировщика понижение производительности связано с тем, что добавляется большое количество системных вызовов для записи в сокет при запросах данных у узлов в кластере.


<b>Запуск wrk GET запросы на 128 соединений в 2 потока с rate 10000 запросов в секундну - 1 минута:</b>
<b>Узел с портом 8080</b>
```
Running 1m test @ http://localhost:8080
  2 threads and 128 connections
  Thread calibration: mean lat.: 9.087ms, rate sampling interval: 46ms
  Thread calibration: mean lat.: 8.358ms, rate sampling interval: 44ms
  Thread Stats   Avg      Stdev     Max   +/- Stdev
    Latency     8.99ms    8.19ms  45.57ms   79.55%
    Req/Sec     5.06k     1.10k    7.16k    63.38%
  Latency Distribution (HdrHistogram - Recorded Latency)
 50.000%    5.57ms
 75.000%   14.33ms
 90.000%   21.74ms
 99.000%   31.28ms
 99.900%   36.32ms
 99.990%   40.99ms
 99.999%   44.51ms
100.000%   45.60ms
...
#[Mean    =        8.990, StdDeviation   =        8.187]
#[Max     =       45.568, Total count    =       496908]
#[Buckets =           27, SubBuckets     =         2048]
----------------------------------------------------------
  594229 requests in 1.00m, 41.70MB read
Requests/sec:   9903.93
Transfer/sec:    711.77KB
```

<b>Узел с портом 8081</b>
```
Running 1m test @ http://localhost:8081
  2 threads and 128 connections
  Thread calibration: mean lat.: 8.519ms, rate sampling interval: 39ms
  Thread calibration: mean lat.: 9.484ms, rate sampling interval: 43ms
  Thread Stats   Avg      Stdev     Max   +/- Stdev
    Latency     7.06ms    7.49ms  53.76ms   83.33%
    Req/Sec     5.06k   830.92     6.95k    72.54%
  Latency Distribution (HdrHistogram - Recorded Latency)
 50.000%    3.11ms
 75.000%   11.15ms
 90.000%   17.30ms
 99.000%   32.90ms
 99.900%   49.31ms
 99.990%   51.97ms
 99.999%   53.60ms
100.000%   53.79ms
...
#[Mean    =        7.060, StdDeviation   =        7.493]
#[Max     =       53.760, Total count    =       496848]
#[Buckets =           27, SubBuckets     =         2048]
----------------------------------------------------------
  594221 requests in 1.00m, 41.70MB read
Requests/sec:   9903.72
Transfer/sec:    711.76KB
```

<b>Узел с портом 8082</b>
```
Running 1m test @ http://localhost:8082
  2 threads and 128 connections
  Thread calibration: mean lat.: 4.330ms, rate sampling interval: 26ms
  Thread calibration: mean lat.: 4.063ms, rate sampling interval: 25ms
  Thread Stats   Avg      Stdev     Max   +/- Stdev
    Latency     5.24ms    6.31ms  45.31ms   83.58%
    Req/Sec     5.10k   838.82     7.20k    77.76%
  Latency Distribution (HdrHistogram - Recorded Latency)
 50.000%    2.23ms
 75.000%    6.50ms
 90.000%   15.27ms
 99.000%   26.46ms
 99.900%   35.52ms
 99.990%   40.93ms
 99.999%   43.81ms
100.000%   45.34ms
...
#[Mean    =        5.242, StdDeviation   =        6.314]
#[Max     =       45.312, Total count    =       496760]
#[Buckets =           27, SubBuckets     =         2048]
----------------------------------------------------------
  594050 requests in 1.00m, 41.69MB read
Requests/sec:   9900.98
Transfer/sec:    711.56KB
```

<b>Профилирование с помощью async-profiler в течение 20 секунд. FlameGraph для cpu, alloc:</b>

<a href=./resource/profile-html/stage5/stage5-get-cpu-node8080.html>GET запросы CPU для порта 8080</a>

<a href=./resource/profile-html/stage5/stage5-get-mem-node8080.html>GET запросы память для порта 8080</a>

<a href=./resource/profile-html/stage5/stage5-get-cpu-node8081.html>GET запросы CPU для порта 8081</a>

<a href=./resource/profile-html/stage5/stage5-get-mem-node8081.html>GET запросы память для порта 8081</a>

<a href=./resource/profile-html/stage5/stage5-get-cpu-node8082.html>GET запросы CPU для порта 8082</a>

<a href=./resource/profile-html/stage5/stage5-get-mem-node8082.html>GET запросы память для порта 8082</a>

<b>Вывод по результатам:</b>
Для GET запросов производительность не сильно изменилась. По данным профилировщика добавляется большое количество системных вызовов записи в сокет данных, но на производительность они не повлияли заметно. Также стоит отметить, что большое число вызовов уходить на поиск данных в SSTables методом range.


<h4>Общий вывод и оптимизации:</h4>
Исходя из данных профилировщика основными направлениями для оптимизации могут быть - сокращение системных вызовов обращения к кластерам, т.е. многократное использование вызовов HttpClient. Как один из вариантов, если данные небольшого размера, можно объединять несколько данных в один запрос и отправлять на узлы. Внутри узлов можно не использовать http протокол, а использовать обычное сокетное подключение определив несколько команд для обработки данных. Также большое количество вызовов приходится на метод offset класса SSTables, стоит рассматривать более эффиктивную реализацию поиска данных в буфере отображения файла. 
 