## Отчет №3 "Асинхронный сервер"
## Автор: Гаспарян Сократ

### 1. Результат ThreadPoolExecutor с 2-мя потоками и неограниченной очередью:
<b>Запуск wrk PUT запросы на 16 соединений в 2 потока с rate 10000 запросов в секундну:</b>
```
Running 1m test @ http://localhost:8080
  2 threads and 16 connections
  Thread calibration: mean lat.: 1.739ms, rate sampling interval: 10ms
  Thread calibration: mean lat.: 1.705ms, rate sampling interval: 10ms
  Thread Stats   Avg      Stdev     Max   +/- Stdev
    Latency     1.65ms    3.13ms  64.61ms   98.38%
    Req/Sec     5.28k     0.94k   24.56k    95.39%
  Latency Distribution (HdrHistogram - Recorded Latency)
 50.000%    1.25ms
 75.000%    1.78ms
 90.000%    2.28ms
 99.000%   13.70ms
 99.900%   47.46ms
 99.990%   57.15ms
 99.999%   62.17ms
100.000%   64.64ms
...
#[Mean    =        1.647, StdDeviation   =        3.131]
#[Max     =       64.608, Total count    =       499419]
#[Buckets =           27, SubBuckets     =         2048]
----------------------------------------------------------
  599635 requests in 1.00m, 38.31MB read
Requests/sec:   9992.90
Transfer/sec:    653.83KB
```
Профилирование с помощью async-profiler в течение 15 секунд. FlameGraph для cpu, alloc:

<a href=./resource/profile-html/stage3/stage3-put-cpu-thread2.html>PUT запросы CPU</a>

<a href=./resource/profile-html/stage3/stage3-put-mem-thread2.html>PUT запросы память</a>

Как видно из FlameGraph большую часть времени занимает процесс записи данных в сокет при ответе на запрос - 32.9%. Также достаточно много занимает процесс обработки запроса у selector one-nio - 24.58%. У пула потока на обработку очередной задачи значительную часть времени занимает процесс работы с блокирующей очередью на запись и обработку сессий 7-9%.


<b>Запуск wrk GET запросы на 16 соединений в 2 потока с rate 10000 запросов в секундну:</b>
```
Running 1m test @ http://localhost:8080
  2 threads and 16 connections
  Thread calibration: mean lat.: 1.991ms, rate sampling interval: 10ms
  Thread calibration: mean lat.: 1.935ms, rate sampling interval: 10ms
  Thread Stats   Avg      Stdev     Max   +/- Stdev
    Latency     1.50ms    1.20ms  25.26ms   92.88%
    Req/Sec     5.28k   556.07    13.70k    79.79%
  Latency Distribution (HdrHistogram - Recorded Latency)
 50.000%    1.33ms
 75.000%    1.82ms
 90.000%    2.33ms
 99.000%    5.91ms
 99.900%   16.50ms
 99.990%   22.96ms
 99.999%   25.01ms
100.000%   25.28ms
...
#[Mean    =        1.504, StdDeviation   =        1.195]
#[Max     =       25.264, Total count    =       499610]
#[Buckets =           27, SubBuckets     =         2048]
----------------------------------------------------------
  599826 requests in 1.00m, 42.10MB read
Requests/sec:   9996.90
Transfer/sec:    718.49KB
```

Профилирование с помощью async-profiler в течение 15 секунд. FlameGraph для cpu, alloc:

<a href=./resource/profile-html/stage3/stage3-get-cpu-thread2.html>GET запросы CPU</a>

<a href=./resource/profile-html/stage3/stage3-get-mem-thread2.html>GET запросы память</a>

Во FlameGraph при GET запросах ситуция похожа на PUT запросы, часть времени уходит на работу с блокирующей очередью для ThreadPoolExecutor. В wrk сильных скачков между перцентилями нет, так как запросы на получение данных не блокируются.


### 2. Результат ThreadPoolExecutor с 4-мя потоками и неограниченной очередью:
<b>Запуск wrk PUT запросы на 16 соединений в 2 потока с rate 10000 запросов в секундну:</b>
```
Running 1m test @ http://localhost:8080
  2 threads and 16 connections
  Thread calibration: mean lat.: 1.578ms, rate sampling interval: 10ms
  Thread calibration: mean lat.: 1.569ms, rate sampling interval: 10ms
  Thread Stats   Avg      Stdev     Max   +/- Stdev
    Latency     1.67ms    2.77ms  56.48ms   97.32%
    Req/Sec     5.28k     0.93k   24.11k    92.17%
  Latency Distribution (HdrHistogram - Recorded Latency)
 50.000%    1.27ms
 75.000%    1.79ms
 90.000%    2.33ms
 99.000%   13.15ms
 99.900%   42.37ms
 99.990%   52.90ms
 99.999%   54.97ms
100.000%   56.51ms
...
#[Mean    =        1.670, StdDeviation   =        2.769]
#[Max     =       56.480, Total count    =       499602]
#[Buckets =           27, SubBuckets     =         2048]
----------------------------------------------------------
  599825 requests in 1.00m, 38.33MB read
Requests/sec:   9997.12
Transfer/sec:    654.11KB
```
Профилирование с помощью async-profiler в течение 15 секунд. FlameGraph для cpu, alloc:

<a href=./resource/profile-html/stage3/stage3-put-cpu-thread4.html>PUT запросы CPU</a>

<a href=./resource/profile-html/stage3/stage3-put-mem-thread4.html>PUT запросы память</a>

Разница у wrk по сравнению с 2-х поточной реализацией практически отсутствует. Разница у FlameGraph состоит в затрачиваемых ресурсах для блокирующей очереди в ThreadPoolExecutor - увеличилось количество вызовов метода getTask из очереди, возможно, это связано с тем, что появилось больше потоков на обработку запросов. Значительно увеличилось количество вызовов связанных с исполнением методов в потоке Thread::call_run() - в 2 раза. 

<b>Запуск wrk GET запросы на 16 соединений в 2 потока с rate 10000 запросов в секундну:</b>
```
Running 1m test @ http://localhost:8080
  2 threads and 16 connections
  Thread calibration: mean lat.: 1.438ms, rate sampling interval: 10ms
  Thread calibration: mean lat.: 1.430ms, rate sampling interval: 10ms
  Thread Stats   Avg      Stdev     Max   +/- Stdev
    Latency     1.53ms    1.32ms  35.07ms   93.98%
    Req/Sec     5.27k   657.06    13.20k    82.82%
  Latency Distribution (HdrHistogram - Recorded Latency)
 50.000%    1.31ms
 75.000%    1.85ms
 90.000%    2.42ms
 99.000%    6.81ms
 99.900%   17.39ms
 99.990%   24.03ms
 99.999%   28.62ms
100.000%   35.10ms
...
#[Mean    =        1.531, StdDeviation   =        1.315]
#[Max     =       35.072, Total count    =       499606]
#[Buckets =           27, SubBuckets     =         2048]
----------------------------------------------------------
  599822 requests in 1.00m, 42.10MB read
Requests/sec:   9996.82
Transfer/sec:    718.48KB
```

Профилирование с помощью async-profiler в течение 15 секунд. FlameGraph для cpu, alloc:

<a href=./resource/profile-html/stage3/stage3-get-cpu-thread4.html>GET запросы CPU</a>

<a href=./resource/profile-html/stage3/stage3-get-mem-thread4.html>GET запросы память</a>

Разницы по wrk почти нет. Разница по FlameGraph состоит в том, что увилечилось количество вызовов метода get(range для LsmDAO соответственно), это связано с увеличением количества потоков, сервер обработал большее количество запросов на получение данных.


### 3. Результат ThreadPoolExecutor с 4-мя потоками и ограниченной очередью размером 2:
<b>Запуск wrk PUT запросы на 16 соединений в 2 потока с rate 10000 запросов в секундну:</b>
```
Running 1m test @ http://localhost:8080
  2 threads and 16 connections
  Thread calibration: mean lat.: 0.735ms, rate sampling interval: 10ms
  Thread calibration: mean lat.: 6.867ms, rate sampling interval: 27ms
  Thread Stats   Avg      Stdev     Max   +/- Stdev
    Latency   657.97us    1.04ms  48.83ms   99.45%
    Req/Sec     0.96k   597.66     7.22k    72.67%
  Latency Distribution (HdrHistogram - Recorded Latency)
 50.000%  660.00us
 75.000%    0.87ms
 90.000%    1.05ms
 99.000%    1.12ms
 99.900%   14.14ms
 99.990%   44.54ms
 99.999%   47.58ms
100.000%   48.86ms
...
#[Mean    =        0.658, StdDeviation   =        1.036]
#[Max     =       48.832, Total count    =        62451]
#[Buckets =           27, SubBuckets     =         2048]
----------------------------------------------------------
  75025 requests in 1.00m, 4.79MB read
  Socket errors: connect 0, read 0, write 0, timeout 410
Requests/sec:   1249.60
Transfer/sec:     81.76KB
```

Профилирование с помощью async-profiler в течение 15 секунд. FlameGraph для cpu, alloc:

<a href=./resource/profile-html/stage3/stage3-put-cpu-thread4-queue2.html>PUT запросы CPU</a>

<a href=./resource/profile-html/stage3/stage3-put-mem-thread4-queue2.html>PUT запросы память</a>

Как видно из отчета wrk заданный размер очереди слишком мал, большая часть соединений не была обработана. В ошибке сокетов указано 410 таймаутов, запросы на которые не ответили. Из FlameGraph видно, что количетсво вызовов getTask значительно уменьшилось из-за такого маленького размера очереди - 18 раз и это 17.3% от всех методов.


<b>Запуск wrk GET запросы на 16 соединений в 2 потока с rate 10000 запросов в секундну:</b>
```
Running 1m test @ http://localhost:8080
  2 threads and 16 connections
  Thread calibration: mean lat.: 0.767ms, rate sampling interval: 10ms
  Thread calibration: mean lat.: 0.829ms, rate sampling interval: 10ms
  Thread Stats   Avg      Stdev     Max   +/- Stdev
    Latency   626.26us  591.07us  29.34ms   99.31%
    Req/Sec   659.35     69.61     1.89k    83.21%
  Latency Distribution (HdrHistogram - Recorded Latency)
 50.000%  602.00us
 75.000%  808.00us
 90.000%    1.00ms
 99.000%    1.07ms
 99.900%    7.61ms
 99.990%   23.53ms
 99.999%   27.97ms
100.000%   29.36ms
...
#[Mean    =        0.626, StdDeviation   =        0.591]
#[Max     =       29.344, Total count    =        62450]
#[Buckets =           27, SubBuckets     =         2048]
----------------------------------------------------------
  75098 requests in 1.00m, 5.19MB read
  Socket errors: connect 0, read 0, write 0, timeout 403
Requests/sec:   1251.63
Transfer/sec:     88.54KB
```

Профилирование с помощью async-profiler в течение 15 секунд. FlameGraph для cpu, alloc:

<a href=./resource/profile-html/stage3/stage3-get-cpu-thread4-queue2.html>GET запросы CPU</a>

<a href=./resource/profile-html/stage3/stage3-get-mem-thread4-queue2.html>GET запросы память</a>

Ситуция с wrk аналогичная, как и при PUT запросах. 


### 4. Результат ThreadPoolExecutor с 4-мя потоками и ограниченной очередью размером 8:
<b>Запуск wrk PUT запросы на 16 соединений в 2 потока с rate 10000 запросов в секундну:</b>
```
Running 1m test @ http://localhost:8080
  2 threads and 16 connections
  Thread calibration: mean lat.: 1.103ms, rate sampling interval: 10ms
  Thread calibration: mean lat.: 1.058ms, rate sampling interval: 10ms
  Thread Stats   Avg      Stdev     Max   +/- Stdev
    Latency   758.43us    1.68ms  49.25ms   99.00%
    Req/Sec     2.64k   784.04    18.00k    61.00%
  Latency Distribution (HdrHistogram - Recorded Latency)
 50.000%  627.00us
 75.000%    0.88ms
 90.000%    1.03ms
 99.000%    2.43ms
 99.900%   29.33ms
 99.990%   41.34ms
 99.999%   46.46ms
100.000%   49.28ms
...
#[Mean    =        0.758, StdDeviation   =        1.682]
#[Max     =       49.248, Total count    =       249804]
#[Buckets =           27, SubBuckets     =         2048]
----------------------------------------------------------
  299960 requests in 1.00m, 19.17MB read
  Socket errors: connect 0, read 0, write 0, timeout 226
Requests/sec:   4999.29
Transfer/sec:    327.10KB
```

Профилирование с помощью async-profiler в течение 15 секунд. FlameGraph для cpu, alloc:

<a href=./resource/profile-html/stage3/stage3-put-cpu-thread4-queue8.html>PUT запросы CPU</a>

<a href=./resource/profile-html/stage3/stage3-put-mem-thread4-queue8.html>PUT запросы память</a>

В отчете wrk видно, что, увеличив размер очереди, сервер всё ещё не обрабатывает все запросы - 226 таймаутов, но количество обработанных запросов увеличилось. На FlameGraph видно, что увеличилось количество вызовов метода getTask - доставания из очереди задач в 2.5 раза.

<b>Запуск wrk GET запросы на 16 соединений в 2 потока с rate 10000 запросов в секундну:</b>
```
Running 1m test @ http://localhost:8080
  2 threads and 16 connections
  Thread calibration: mean lat.: 0.932ms, rate sampling interval: 10ms
  Thread calibration: mean lat.: 0.785ms, rate sampling interval: 10ms
  Thread Stats   Avg      Stdev     Max   +/- Stdev
    Latency   738.35us  696.13us  27.31ms   98.11%
    Req/Sec     2.65k     1.36k    9.89k    59.66%
  Latency Distribution (HdrHistogram - Recorded Latency)
 50.000%  703.00us
 75.000%    0.95ms
 90.000%    1.09ms
 99.000%    2.38ms
 99.900%    9.81ms
 99.990%   23.53ms
 99.999%   26.90ms
100.000%   27.33ms
...
#[Mean    =        0.738, StdDeviation   =        0.696]
#[Max     =       27.312, Total count    =       249800]
#[Buckets =           27, SubBuckets     =         2048]
----------------------------------------------------------
  300686 requests in 1.00m, 20.83MB read
  Socket errors: connect 0, read 0, write 0, timeout 229
Requests/sec:   5011.44
Transfer/sec:    355.56KB
```
Профилирование с помощью async-profiler в течение 15 секунд. FlameGraph для cpu, alloc:

<a href=./resource/profile-html/stage3/stage3-get-cpu-thread4-queue8.html>GET запросы CPU</a>

<a href=./resource/profile-html/stage3/stage3-get-mem-thread4-queue8.html>GET запросы память</a>

Ситуция с wrk аналогичная, как и при PUT запросах. 


### 5. Результат ThreadPoolExecutor с 4-мя потоками и ограниченной очередью размером 16:
<b>Запуск wrk PUT запросы на 16 соединений в 2 потока с rate 10000 запросов в секундну:</b>
```
Running 1m test @ http://localhost:8080
  2 threads and 16 connections
  Thread calibration: mean lat.: 1.789ms, rate sampling interval: 10ms
  Thread calibration: mean lat.: 1.676ms, rate sampling interval: 10ms
  Thread Stats   Avg      Stdev     Max   +/- Stdev
    Latency     1.72ms    3.18ms  57.60ms   97.83%
    Req/Sec     5.28k     0.92k   22.44k    93.09%
  Latency Distribution (HdrHistogram - Recorded Latency)
 50.000%    1.27ms
 75.000%    1.79ms
 90.000%    2.31ms
 99.000%   17.09ms
 99.900%   43.42ms
 99.990%   51.52ms
 99.999%   54.81ms
100.000%   57.63ms
...
#[Mean    =        1.721, StdDeviation   =        3.177]
#[Max     =       57.600, Total count    =       499598]
#[Buckets =           27, SubBuckets     =         2048]
----------------------------------------------------------
  599821 requests in 1.00m, 38.33MB read
Requests/sec:   9996.99
Transfer/sec:    654.10KB
```
Профилирование с помощью async-profiler в течение 15 секунд. FlameGraph для cpu, alloc:

<a href=./resource/profile-html/stage3/stage3-put-cpu-thread4-queue16.html>PUT запросы CPU</a>

<a href=./resource/profile-html/stage3/stage3-put-mem-thread4-queue16.html>PUT запросы память</a>

Отчет wrk показывает, что очереди размером 16 достаточно, чтобы обработать такое количество запросов. Результат равен результату при неограниченной очереди. На FlameGraph видно, что количество вызовов метода получения новой задачи из пула потока сравнилось с количеством вызвовов при неограниченной очереди. 

<b>Запуск wrk GET запросы на 16 соединений в 2 потока с rate 10000 запросов в секундну:</b>
```
Running 1m test @ http://localhost:8080
  2 threads and 16 connections
  Thread calibration: mean lat.: 1.718ms, rate sampling interval: 10ms
  Thread calibration: mean lat.: 1.744ms, rate sampling interval: 10ms
  Thread Stats   Avg      Stdev     Max   +/- Stdev
    Latency     1.52ms    1.32ms  32.51ms   95.07%
    Req/Sec     5.27k   564.57    16.00k    79.95%
  Latency Distribution (HdrHistogram - Recorded Latency)
 50.000%    1.33ms
 75.000%    1.83ms
 90.000%    2.35ms
 99.000%    6.45ms
 99.900%   17.69ms
 99.990%   26.43ms
 99.999%   30.99ms
100.000%   32.53ms
...
#[Mean    =        1.522, StdDeviation   =        1.320]
#[Max     =       32.512, Total count    =       499598]
#[Buckets =           27, SubBuckets     =         2048]
----------------------------------------------------------
  599820 requests in 1.00m, 42.10MB read
Requests/sec:   9997.03
Transfer/sec:    718.50KB
```
Профилирование с помощью async-profiler в течение 15 секунд. FlameGraph для cpu, alloc:

<a href=./resource/profile-html/stage3/stage3-get-cpu-thread4-queue16.html>GET запросы CPU</a>

<a href=./resource/profile-html/stage3/stage3-get-mem-thread4-queue16.html>GET запросы память</a>

Как и в случае с PUT запросами, данный размер очереди позволяет серверу обработать заданное количество GET запросов. Результат совпадает с результатом для неограниченной очереди.


### 6. Результат ThreadPoolExecutor с 4-мя потоками и ограниченной очередью размером 128:
<b>Запуск wrk PUT запросы на 16 соединений в 2 потока с rate 10000 запросов в секундну:</b>
```
Running 1m test @ http://localhost:8080
  2 threads and 16 connections
  Thread calibration: mean lat.: 1.907ms, rate sampling interval: 10ms
  Thread calibration: mean lat.: 2.030ms, rate sampling interval: 10ms
  Thread Stats   Avg      Stdev     Max   +/- Stdev
    Latency     1.67ms    2.92ms  62.30ms   98.05%
    Req/Sec     5.29k     0.91k   23.33k    93.26%
  Latency Distribution (HdrHistogram - Recorded Latency)
 50.000%    1.28ms
 75.000%    1.81ms
 90.000%    2.33ms
 99.000%   14.34ms
 99.900%   43.81ms
 99.990%   56.86ms
 99.999%   61.25ms
100.000%   62.33ms
...
#[Mean    =        1.670, StdDeviation   =        2.918]
#[Max     =       62.304, Total count    =       499598]
#[Buckets =           27, SubBuckets     =         2048]
----------------------------------------------------------
  599822 requests in 1.00m, 38.33MB read
Requests/sec:   9996.85
Transfer/sec:    654.09KB
```
Профилирование с помощью async-profiler в течение 15 секунд. FlameGraph для cpu, alloc:

<a href=./resource/profile-html/stage3/stage3-put-cpu-thread4-queue128.html>PUT запросы CPU</a>

<a href=./resource/profile-html/stage3/stage3-put-mem-thread4-queue128.html>PUT запросы память</a>

Из отчетов wrk и FlameGraph, видно, что увеличение размера очереди никак не увеличело время ответа на запрос. Время осталось таким же, как и при очереди размером в 16.

<b>Запуск wrk GET запросы на 16 соединений в 2 потока с rate 10000 запросов в секундну:</b>
```
Running 1m test @ http://localhost:8080
  2 threads and 16 connections
  Thread calibration: mean lat.: 1.502ms, rate sampling interval: 10ms
  Thread calibration: mean lat.: 1.496ms, rate sampling interval: 10ms
  Thread Stats   Avg      Stdev     Max   +/- Stdev
    Latency     1.45ms  844.34us  19.82ms   80.34%
    Req/Sec     5.27k   499.77    11.20k    76.30%
  Latency Distribution (HdrHistogram - Recorded Latency)
 50.000%    1.33ms
 75.000%    1.83ms
 90.000%    2.32ms
 99.000%    3.76ms
 99.900%    9.90ms
 99.990%   16.30ms
 99.999%   19.07ms
100.000%   19.84ms
...
#[Mean    =        1.447, StdDeviation   =        0.844]
#[Max     =       19.824, Total count    =       499594]
#[Buckets =           27, SubBuckets     =         2048]
----------------------------------------------------------
  599813 requests in 1.00m, 42.10MB read
Requests/sec:   9996.89
Transfer/sec:    718.49KB
```

Профилирование с помощью async-profiler в течение 15 секунд. FlameGraph для cpu, alloc:

<a href=./resource/profile-html/stage3/stage3-get-cpu-thread4-queue128.html>GET запросы CPU</a>

<a href=./resource/profile-html/stage3/stage3-get-mem-thread4-queue128.html>GET запросы память</a>

Увеличение размера очереди для GET запрос не повлияло на время ответа запроса.


<h4>Общие оптимизации:</h4>
Можно попробовать улучшить результат, например, использовать распределение потоков при одновременных GET и PUT запросах, т.е. увеличить количество потоков на обработку запросов, которые требует большее время на обработку. Либо можно использовать cachedThreadPool и создавать потоки по мере необходимости, при увеличений нагрузки, тем самым можно отдавать ресурсы на другие задачи. Можно задавать параметр keepAliveTime для ThreadPoolExecutor, если есть задачи, которые занимают слишком много времени, снижая общую производительность. 