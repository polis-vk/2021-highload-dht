## Отчет №6 "Асинхронный клиент"
## Автор: Гаспарян Сократ

### Результаты нагрузочного тестирования с асинхронными узлами кластера
<b>Запуск wrk PUT запросы на 64 соединений в 2 потока с rate 5000 запросов в секундну - 1 минута:</b>
<b>Узел с портом 8080</b>
```
Running 1m test @ http://localhost:8080
  2 threads and 64 connections
  Thread calibration: mean lat.: 10.830ms, rate sampling interval: 54ms
  Thread calibration: mean lat.: 12.523ms, rate sampling interval: 56ms
  Thread Stats   Avg      Stdev     Max   +/- Stdev
    Latency    37.13ms   73.61ms 407.81ms   90.73%
    Req/Sec     2.53k   793.14     5.02k    65.89%
  Latency Distribution (HdrHistogram - Recorded Latency)
 50.000%   10.90ms
 75.000%   28.25ms
 90.000%   92.35ms
 99.000%  334.33ms
 99.900%  372.99ms
 99.990%  393.47ms
 99.999%  405.25ms
100.000%  408.06ms
...
#[Mean    =       37.128, StdDeviation   =       73.612]
#[Max     =      407.808, Total count    =       249490]
#[Buckets =           27, SubBuckets     =         2048]
----------------------------------------------------------
  297310 requests in 1.00m, 19.00MB read
Requests/sec:   4955.20
Transfer/sec:    324.22KB
```

<b>Узел с портом 8081</b>
```
Running 1m test @ http://localhost:8081
  2 threads and 64 connections
  Thread calibration: mean lat.: 11.286ms, rate sampling interval: 60ms
  Thread calibration: mean lat.: 12.889ms, rate sampling interval: 61ms
  Thread Stats   Avg      Stdev     Max   +/- Stdev
    Latency    28.14ms   55.46ms 379.90ms   92.25%
    Req/Sec     2.52k   768.24     5.80k    66.12%
  Latency Distribution (HdrHistogram - Recorded Latency)
 50.000%    9.43ms
 75.000%   26.35ms
 90.000%   47.13ms
 99.000%  280.06ms
 99.900%  348.16ms
 99.990%  364.80ms
 99.999%  374.78ms
100.000%  380.16ms
...
#[Mean    =       28.145, StdDeviation   =       55.462]
#[Max     =      379.904, Total count    =       249164]
#[Buckets =           27, SubBuckets     =         2048]
----------------------------------------------------------
  297297 requests in 1.00m, 19.00MB read
Requests/sec:   4954.28
Transfer/sec:    324.16KB
```

<b>Узел с портом 8082</b>
```
Running 1m test @ http://localhost:8082
  2 threads and 64 connections
  Thread calibration: mean lat.: 8.436ms, rate sampling interval: 53ms
  Thread calibration: mean lat.: 8.719ms, rate sampling interval: 54ms
  Thread Stats   Avg      Stdev     Max   +/- Stdev
    Latency    26.26ms   65.80ms 468.99ms   93.02%
    Req/Sec     2.53k   740.33     5.44k    67.12%
  Latency Distribution (HdrHistogram - Recorded Latency)
 50.000%    2.77ms
 75.000%   21.22ms
 90.000%   40.00ms
 99.000%  393.47ms
 99.900%  441.60ms
 99.990%  457.73ms
 99.999%  468.48ms
100.000%  469.25ms
...
#[Mean    =       26.257, StdDeviation   =       65.799]
#[Max     =      468.992, Total count    =       249275]
#[Buckets =           27, SubBuckets     =         2048]
----------------------------------------------------------
  299626 requests in 1.00m, 19.14MB read
Requests/sec:   4993.76
Transfer/sec:    326.74KB
```

<b>Профилирование с помощью async-profiler в течение 30 секунд. FlameGraph для cpu, alloc, lock:</b>

<a href=./resource/profile-html/stage6/stage6-put-cpu.html>PUT запросы CPU</a>

<a href=./resource/profile-html/stage6/stage6-put-mem.html>PUT запросы память</a>

<a href=./resource/profile-html/stage6/stage6-put-lock.html>PUT запросы блокировки</a>


<b>Вывод по результатам:</b>
Как видно из отчета wrk реализация для  PUT запросов показывает результаты значительно хуже, чем было до асинхронного опроса узлов кластера. Из данных профилировщика видно, что множество вызововов приходится на обработку потоков. Блокировки в данной реализации связаны только с работой в пуле потоков, никаких блокировок в пользовательском коде не происходит.  


<b>Запуск wrk GET запросы на 64 соединений в 2 потока с rate 5000 запросов в секундну - 1 минута:</b>
<b>Узел с портом 8080</b>
```
Running 1m test @ http://localhost:8080
  2 threads and 64 connections
  Thread calibration: mean lat.: 377.238ms, rate sampling interval: 1829ms
  Thread calibration: mean lat.: 379.367ms, rate sampling interval: 1841ms
  Thread Stats   Avg      Stdev     Max   +/- Stdev
    Latency     5.06ms    6.41ms  36.90ms   83.35%
    Req/Sec     2.50k    16.06     2.54k    72.22%
  Latency Distribution (HdrHistogram - Recorded Latency)
 50.000%    2.05ms
 75.000%    4.66ms
 90.000%   15.89ms
 99.000%   26.14ms
 99.900%   30.91ms
 99.990%   34.14ms
 99.999%   35.78ms
100.000%   36.93ms
...
#[Mean    =        5.058, StdDeviation   =        6.408]
#[Max     =       36.896, Total count    =       249265]
#[Buckets =           27, SubBuckets     =         2048]
----------------------------------------------------------
  299589 requests in 1.00m, 20.91MB read
Requests/sec:   4993.13
Transfer/sec:    356.89KB
```

<b>Узел с портом 8081</b>
```
Running 1m test @ http://localhost:8081
  2 threads and 64 connections
  Thread calibration: mean lat.: 3.222ms, rate sampling interval: 12ms
  Thread calibration: mean lat.: 3.247ms, rate sampling interval: 12ms
  Thread Stats   Avg      Stdev     Max   +/- Stdev
    Latency     4.94ms    6.44ms  41.22ms   84.01%
    Req/Sec     2.62k   827.78     7.64k    79.33%
  Latency Distribution (HdrHistogram - Recorded Latency)
 50.000%    1.97ms
 75.000%    4.13ms
 90.000%   15.81ms
 99.000%   26.33ms
 99.900%   32.24ms
 99.990%   36.38ms
 99.999%   39.04ms
100.000%   41.25ms
...
#[Mean    =        4.938, StdDeviation   =        6.438]
#[Max     =       41.216, Total count    =       249207]
#[Buckets =           27, SubBuckets     =         2048]
----------------------------------------------------------
  299631 requests in 1.00m, 20.91MB read
Requests/sec:   4993.96
Transfer/sec:    356.95KB
```

<b>Узел с портом 8082</b>
```
Running 1m test @ http://localhost:8082
  2 threads and 64 connections
  Thread calibration: mean lat.: 3.999ms, rate sampling interval: 24ms
  Thread calibration: mean lat.: 3.990ms, rate sampling interval: 25ms
  Thread Stats   Avg      Stdev     Max   +/- Stdev
    Latency     4.33ms    5.93ms  38.05ms   85.74%
    Req/Sec     2.55k   610.16     5.35k    80.52%
  Latency Distribution (HdrHistogram - Recorded Latency)
 50.000%    1.88ms
 75.000%    2.85ms
 90.000%   14.57ms
 99.000%   25.25ms
 99.900%   29.92ms
 99.990%   33.69ms
 99.999%   37.38ms
100.000%   38.08ms
...
#[Mean    =        4.332, StdDeviation   =        5.932]
#[Max     =       38.048, Total count    =       249133]
#[Buckets =           27, SubBuckets     =         2048]
----------------------------------------------------------
  299562 requests in 1.00m, 20.91MB read
Requests/sec:   4992.49
Transfer/sec:    356.84KB
```

<b>Профилирование с помощью async-profiler в течение 30 секунд. FlameGraph для cpu, alloc, lock:</b>

<a href=./resource/profile-html/stage6/stage6-get-cpu.html>GET запросы CPU</a>

<a href=./resource/profile-html/stage6/stage6-get-mem.html>GET запросы память</a>

<a href=./resource/profile-html/stage6/stage6-get-lock.html>GET запросы блокировки</a>


<b>Вывод по результатам:</b>
Как видно из отчета wrk реализация для GET запросов показывает результаты такие же, как и до асинхронного опроса узлов кластера. Из данных профилировщика видно, что множество вызововов приходится на обработку поиска данных в хранилище DAO и на обработку потоков в пуле. Блокировки в данной реализации связаны только с работой в пуле потоков, никаких блокировок в пользовательском коде не происходит.  



<h4>Общий вывод и оптимизации:</h4>
Из отчетов wrk и async-profiler видно, что добавление асинхронного опроса узлов кластера, для PUT запросов сильно ухудшило производительность. Одной из причин является одновременное включение записи на HDD(вызов flush) на всех узлах. Результаты замеров времени показывают, что пиковое время обработки сетевого запроса к узлу составляет 66.4 мс, пиковое время обработки узлом самого запроса (от другого узла) составляет 49.7 мс. Скачки понижения времени выполнения запроса были замечены, именно при выполнений на всех узлах метода flush. Нагрузочное тестирование производилось на ПК с 4 потоками(2 физическими ядрами). Так как, три узла начинают выполнять запись на диск, 3 из 4 потоков заняты, и следовательно все остальные приходящие запросы просто складываются в очередь, на обработку которой уходит всего 1 поток. В итоге, происходит накопление запросов у сервера one-nio, также у пользовательской серверной реализации(метод handleRequest) и очереди с java.net.http.HttpClient для внутренних запросов. Для GET запросов результаты остались такими же, как и до асинхронных узлов. В качестве оптимизации, стоит убрать асинхронных клиентов - узлов, либо обеспечить большое количество аппаратных ресурсов, далее, если оставить асинхронных клиентов, то можно ограничить выполнение метода flush, используя 1 поток для всех узлов, как разделяемый ресурс.
