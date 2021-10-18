## Отчет №3 "Асинхронный сервер"
## Автор: Гаспарян Сократ

### 1. Результат ThreadPoolExecutor с 2-мя потоками и неограниченной очередью:
<b>Запуск wrk PUT запросы на 64 соединений в 4 потока с rate 10000 запросов в секундну:</b>
```
Running 1m test @ http://localhost:8080
  4 threads and 64 connections
  Thread calibration: mean lat.: 1.496ms, rate sampling interval: 10ms
  Thread calibration: mean lat.: 1.577ms, rate sampling interval: 10ms
  Thread calibration: mean lat.: 1.716ms, rate sampling interval: 10ms
  Thread calibration: mean lat.: 2.449ms, rate sampling interval: 10ms
  Thread Stats   Avg      Stdev     Max   +/- Stdev
    Latency     1.82ms    1.79ms  38.24ms   95.45%
    Req/Sec     2.64k   575.96    12.44k    80.55%
  Latency Distribution (HdrHistogram - Recorded Latency)
 50.000%    1.51ms
 75.000%    2.21ms
 90.000%    2.99ms
 99.000%    7.99ms
 99.900%   25.45ms
 99.990%   34.81ms
 99.999%   37.53ms
100.000%   38.27ms
...
#[Mean    =        1.816, StdDeviation   =        1.794]
#[Max     =       38.240, Total count    =       499186]
#[Buckets =           27, SubBuckets     =         2048]
----------------------------------------------------------
  597227 requests in 1.00m, 38.16MB read
Requests/sec:   9953.77
Transfer/sec:    651.27KB
```
Профилирование с помощью async-profiler в течение 15 секунд. FlameGraph для cpu, alloc:

<a href=./resource/profile-html/stage3/stage3-put-cpu-thread2.html>PUT запросы CPU</a>

<a href=./resource/profile-html/stage3/stage3-put-mem-thread2.html>PUT запросы память</a>

Как видно из FlameGraph большую часть времени занимает процесс записи данных в сокет при ответе на запрос - 32.9%. Также достаточно много занимает процесс обработки запроса у selector one-nio - 24.58%. У пула потока на обработку очередной задачи значительную часть времени занимает процесс работы с блокирующей очередью на запись и обработку сессий 7-9%.


<b>Запуск wrk GET запросы на 64 соединений в 4 потока с rate 10000 запросов в секундну:</b>
```
Running 1m test @ http://localhost:8080
  4 threads and 64 connections
  Thread calibration: mean lat.: 1.392ms, rate sampling interval: 10ms
  Thread calibration: mean lat.: 1.413ms, rate sampling interval: 10ms
  Thread calibration: mean lat.: 1.422ms, rate sampling interval: 10ms
  Thread calibration: mean lat.: 1.394ms, rate sampling interval: 10ms
  Thread Stats   Avg      Stdev     Max   +/- Stdev
    Latency     1.42ms    1.15ms  28.86ms   93.64%
    Req/Sec     2.64k   336.12     8.89k    85.09%
  Latency Distribution (HdrHistogram - Recorded Latency)
 50.000%    1.26ms
 75.000%    1.72ms
 90.000%    2.23ms
 99.000%    5.36ms
 99.900%   15.64ms
 99.990%   22.05ms
 99.999%   26.50ms
100.000%   28.88ms
...
#[Mean    =        1.421, StdDeviation   =        1.149]
#[Max     =       28.864, Total count    =       499191]
#[Buckets =           27, SubBuckets     =         2048]
----------------------------------------------------------
  599633 requests in 1.00m, 41.86MB read
Requests/sec:   9994.08
Transfer/sec:    714.34KB

```

Профилирование с помощью async-profiler в течение 15 секунд. FlameGraph для cpu, alloc:

<a href=./resource/profile-html/stage3/stage3-get-cpu-thread2.html>GET запросы CPU</a>

<a href=./resource/profile-html/stage3/stage3-get-mem-thread2.html>GET запросы память</a>

Во FlameGraph при GET запросах ситуция похожа на PUT запросы, часть времени уходит на работу с блокирующей очередью для ThreadPoolExecutor. В wrk сильных скачков между перцентилями нет, так как запросы на получение данных не блокируются.


### 2. Результат ThreadPoolExecutor с 4-мя потоками и неограниченной очередью:
<b>Запуск wrk PUT запросы на 64 соединений в 4 потока с rate 10000 запросов в секундну:</b>
```
Running 1m test @ http://localhost:8080
  4 threads and 64 connections
  Thread calibration: mean lat.: 1.573ms, rate sampling interval: 10ms
  Thread calibration: mean lat.: 1.620ms, rate sampling interval: 10ms
  Thread calibration: mean lat.: 1.615ms, rate sampling interval: 10ms
  Thread calibration: mean lat.: 1.539ms, rate sampling interval: 10ms
  Thread Stats   Avg      Stdev     Max   +/- Stdev
    Latency     1.47ms    1.70ms  43.52ms   97.32%
    Req/Sec     2.64k   410.56    12.78k    88.30%
  Latency Distribution (HdrHistogram - Recorded Latency)
 50.000%    1.24ms
 75.000%    1.70ms
 90.000%    2.23ms
 99.000%    7.53ms
 99.900%   25.42ms
 99.990%   35.78ms
 99.999%   40.48ms
100.000%   43.55ms
...
#[Mean    =        1.470, StdDeviation   =        1.703]
#[Max     =       43.520, Total count    =       499206]
#[Buckets =           27, SubBuckets     =         2048]
----------------------------------------------------------
  599647 requests in 1.00m, 38.32MB read
Requests/sec:   9994.00
Transfer/sec:    653.90KB

```
Профилирование с помощью async-profiler в течение 15 секунд. FlameGraph для cpu, alloc:

<a href=./resource/profile-html/stage3/stage3-put-cpu-thread4.html>PUT запросы CPU</a>

<a href=./resource/profile-html/stage3/stage3-put-mem-thread4.html>PUT запросы память</a>

Разница у wrk по сравнению с 2-х поточной реализацией практически отсутствует. Разница у FlameGraph состоит в затрачиваемых ресурсах для блокирующей очереди в ThreadPoolExecutor - увеличилось количество вызовов метода getTask из очереди, возможно, это связано с тем, что появилось больше потоков на обработку запросов. Значительно увеличилось количество вызовов связанных с исполнением методов в потоке Thread::call_run() - в 2 раза. 

<b>Запуск wrk GET запросы на 64 соединений в 4 потока с rate 10000 запросов в секундну:</b>
```
Running 1m test @ http://localhost:8080
  4 threads and 64 connections
  Thread calibration: mean lat.: 2.180ms, rate sampling interval: 10ms
  Thread calibration: mean lat.: 2.683ms, rate sampling interval: 10ms
  Thread calibration: mean lat.: 2.236ms, rate sampling interval: 10ms
  Thread calibration: mean lat.: 2.211ms, rate sampling interval: 10ms
  Thread Stats   Avg      Stdev     Max   +/- Stdev
    Latency     1.61ms    0.97ms  26.22ms   78.38%
    Req/Sec     2.64k   409.93     7.10k    72.59%
  Latency Distribution (HdrHistogram - Recorded Latency)
 50.000%    1.44ms
 75.000%    2.03ms
 90.000%    2.73ms
 99.000%    4.53ms
 99.900%    9.88ms
 99.990%   17.92ms
 99.999%   22.72ms
100.000%   26.24ms
...
#[Mean    =        1.614, StdDeviation   =        0.968]
#[Max     =       26.224, Total count    =       499168]
#[Buckets =           27, SubBuckets     =         2048]
----------------------------------------------------------
  597219 requests in 1.00m, 41.68MB read
Requests/sec:   9954.09
Transfer/sec:    711.45KB
```

Профилирование с помощью async-profiler в течение 15 секунд. FlameGraph для cpu, alloc:

<a href=./resource/profile-html/stage3/stage3-get-cpu-thread4.html>GET запросы CPU</a>

<a href=./resource/profile-html/stage3/stage3-get-mem-thread4.html>GET запросы память</a>

Разницы по wrk почти нет. Разница по FlameGraph состоит в том, что увилечилось количество вызовов метода get(range для LsmDAO соответственно), это связано с увеличением количества потоков, сервер обработал большее количество запросов на получение данных.


### 3. Результат ThreadPoolExecutor с 4-мя потоками и ограниченной очередью размером 2:
<b>Запуск wrk PUT запросы на 64 соединений в 4 потока с rate 10000 запросов в секундну:</b>
```
Running 1m test @ http://localhost:8080
  4 threads and 64 connections
  Thread calibration: mean lat.: 1.096ms, rate sampling interval: 10ms
  Thread calibration: mean lat.: 0.954ms, rate sampling interval: 10ms
  Thread calibration: mean lat.: 1.825ms, rate sampling interval: 10ms
  Thread calibration: mean lat.: 9223372036854776.000ms, rate sampling interval: 10ms
  Thread Stats   Avg      Stdev     Max   +/- Stdev
    Latency   665.34us  692.79us  22.59ms   99.21%
    Req/Sec   123.93    150.31     0.89k    85.70%
  Latency Distribution (HdrHistogram - Recorded Latency)
 50.000%  614.00us
 75.000%    0.86ms
 90.000%    1.02ms
 99.000%    1.14ms
 99.900%   12.31ms
 99.990%   16.88ms
 99.999%   22.61ms
100.000%   22.61ms
...
#[Mean    =        0.665, StdDeviation   =        0.693]
#[Max     =       22.592, Total count    =        23400]
#[Buckets =           27, SubBuckets     =         2048]
----------------------------------------------------------
  29240 requests in 1.00m, 1.87MB read
  Socket errors: connect 0, read 0, write 0, timeout 1761
Requests/sec:    486.61
Transfer/sec:     31.84KB
```

Профилирование с помощью async-profiler в течение 15 секунд. FlameGraph для cpu, alloc:

<a href=./resource/profile-html/stage3/stage3-put-cpu-thread4-queue2.html>PUT запросы CPU</a>

<a href=./resource/profile-html/stage3/stage3-put-mem-thread4-queue2.html>PUT запросы память</a>

Как видно из отчета wrk заданный размер очереди слишком мал, большая часть соединений не была обработана. В ошибке сокетов указано 1761 таймаут, запросы на которые не ответили. Из FlameGraph видно, что количетсво вызовов getTask значительно уменьшилось из-за такого маленького размера очереди - 18 раз и это 17.3% от всех методов.


<b>Запуск wrk GET запросы на 64 соединений в 4 потока с rate 10000 запросов в секундну:</b>
```
Running 1m test @ http://localhost:8080
  4 threads and 64 connections
  Thread calibration: mean lat.: 1.000ms, rate sampling interval: 10ms
  Thread calibration: mean lat.: 1.029ms, rate sampling interval: 10ms
  Thread calibration: mean lat.: 0.968ms, rate sampling interval: 10ms
  Thread calibration: mean lat.: 0.671ms, rate sampling interval: 10ms
  Thread Stats   Avg      Stdev     Max   +/- Stdev
    Latency     0.90ms    1.29ms  29.23ms   96.90%
    Req/Sec    82.96     92.52   666.00     71.95%
  Latency Distribution (HdrHistogram - Recorded Latency)
 50.000%  695.00us
 75.000%    0.98ms
 90.000%    1.10ms
 99.000%    7.07ms
 99.900%   16.80ms
 99.990%   23.02ms
 99.999%   29.25ms
100.000%   29.25ms
...
#[Mean    =        0.896, StdDeviation   =        1.288]
#[Max     =       29.232, Total count    =        15600]
#[Buckets =           27, SubBuckets     =         2048]
----------------------------------------------------------
  20667 requests in 1.00m, 1.40MB read
  Socket errors: connect 0, read 0, write 0, timeout 1793
  Non-2xx or 3xx responses: 9
Requests/sec:    343.99
Transfer/sec:     23.79KB

```

Профилирование с помощью async-profiler в течение 15 секунд. FlameGraph для cpu, alloc:

<a href=./resource/profile-html/stage3/stage3-get-cpu-thread4-queue2.html>GET запросы CPU</a>

<a href=./resource/profile-html/stage3/stage3-get-mem-thread4-queue2.html>GET запросы память</a>

Ситуция с wrk аналогичная, как и при PUT запросах. 


### 4. Результат ThreadPoolExecutor с 4-мя потоками и ограниченной очередью размером 8:
<b>Запуск wrk PUT запросы на 64 соединений в 4 потока с rate 10000 запросов в секундну:</b>
```
Running 1m test @ http://localhost:8080
  4 threads and 64 connections
  Thread calibration: mean lat.: 0.909ms, rate sampling interval: 10ms
  Thread calibration: mean lat.: 1.791ms, rate sampling interval: 10ms
  Thread calibration: mean lat.: 1.162ms, rate sampling interval: 10ms
  Thread calibration: mean lat.: 1.310ms, rate sampling interval: 10ms
  Thread Stats   Avg      Stdev     Max   +/- Stdev
    Latency   694.66us  638.58us  24.38ms   99.29%
    Req/Sec   330.20    277.89     1.80k    51.85%
  Latency Distribution (HdrHistogram - Recorded Latency)
 50.000%  697.00us
 75.000%    0.92ms
 90.000%    1.10ms
 99.000%    1.21ms
 99.900%   10.93ms
 99.990%   20.82ms
 99.999%   23.17ms
100.000%   24.40ms
...
#[Mean    =        0.695, StdDeviation   =        0.639]
#[Max     =       24.384, Total count    =        62401]
#[Buckets =           27, SubBuckets     =         2048]
----------------------------------------------------------
  79214 requests in 1.00m, 5.06MB read
  Socket errors: connect 0, read 0, write 0, timeout 1602
Requests/sec:   1318.30
Transfer/sec:     86.26KB

```

Профилирование с помощью async-profiler в течение 15 секунд. FlameGraph для cpu, alloc:

<a href=./resource/profile-html/stage3/stage3-put-cpu-thread4-queue8.html>PUT запросы CPU</a>

<a href=./resource/profile-html/stage3/stage3-put-mem-thread4-queue8.html>PUT запросы память</a>

В отчете wrk видно, что, увеличив размер очереди, сервер всё ещё не обрабатывает все запросы - 1602 таймаутов, но количество обработанных запросов увеличилось. На FlameGraph видно, что увеличилось количество вызовов метода getTask - доставания из очереди задач в 2.5 раза.

<b>Запуск wrk GET запросы на 64 соединений в 4 потока с rate 10000 запросов в секундну:</b>
```
Running 1m test @ http://localhost:8080
  4 threads and 64 connections
  Thread calibration: mean lat.: 1.084ms, rate sampling interval: 10ms
  Thread calibration: mean lat.: 1.327ms, rate sampling interval: 10ms
  Thread calibration: mean lat.: 1.627ms, rate sampling interval: 10ms
  Thread calibration: mean lat.: 1.384ms, rate sampling interval: 10ms
  Thread Stats   Avg      Stdev     Max   +/- Stdev
    Latency   733.81us    1.03ms  40.74ms   98.11%
    Req/Sec   376.79    181.60     1.89k    72.85%
  Latency Distribution (HdrHistogram - Recorded Latency)
 50.000%  666.00us
 75.000%    0.92ms
 90.000%    1.08ms
 99.000%    4.13ms
 99.900%   14.32ms
 99.990%   34.78ms
 99.999%   37.53ms
100.000%   40.77ms
...
#[Mean    =        0.734, StdDeviation   =        1.030]
#[Max     =       40.736, Total count    =        71093]
#[Buckets =           27, SubBuckets     =         2048]
----------------------------------------------------------
  93731 requests in 1.00m, 6.45MB read
  Socket errors: connect 0, read 0, write 0, timeout 1534
Requests/sec:   1562.09
Transfer/sec:    109.99KB

```
Профилирование с помощью async-profiler в течение 15 секунд. FlameGraph для cpu, alloc:

<a href=./resource/profile-html/stage3/stage3-get-cpu-thread4-queue8.html>GET запросы CPU</a>

<a href=./resource/profile-html/stage3/stage3-get-mem-thread4-queue8.html>GET запросы память</a>

Ситуция с wrk аналогичная, как и при PUT запросах. 


### 5. Результат ThreadPoolExecutor с 4-мя потоками и ограниченной очередью размером 64:
<b>Запуск wrk PUT запросы на 64 соединений в 4 потока с rate 10000 запросов в секундну:</b>
```
Running 1m test @ http://localhost:8080
  4 threads and 64 connections
  Thread calibration: mean lat.: 1.490ms, rate sampling interval: 10ms
  Thread calibration: mean lat.: 1.496ms, rate sampling interval: 10ms
  Thread calibration: mean lat.: 1.499ms, rate sampling interval: 10ms
  Thread calibration: mean lat.: 1.565ms, rate sampling interval: 10ms
  Thread Stats   Avg      Stdev     Max   +/- Stdev
    Latency     1.78ms    2.68ms  60.61ms   95.69%
    Req/Sec     2.65k   540.18    13.67k    89.17%
  Latency Distribution (HdrHistogram - Recorded Latency)
 50.000%    1.28ms
 75.000%    1.82ms
 90.000%    2.61ms
 99.000%   13.75ms
 99.900%   34.91ms
 99.990%   46.49ms
 99.999%   55.87ms
100.000%   60.64ms
...
#[Mean    =        1.780, StdDeviation   =        2.684]
#[Max     =       60.608, Total count    =       499199]
#[Buckets =           27, SubBuckets     =         2048]
----------------------------------------------------------
  599648 requests in 1.00m, 38.32MB read
Requests/sec:   9993.97
Transfer/sec:    653.90KB

```
Профилирование с помощью async-profiler в течение 15 секунд. FlameGraph для cpu, alloc:

<a href=./resource/profile-html/stage3/stage3-put-cpu-thread4-queue64.html>PUT запросы CPU</a>

<a href=./resource/profile-html/stage3/stage3-put-mem-thread4-queue64.html>PUT запросы память</a>

Отчет wrk показывает, что очереди размером 64 достаточно, чтобы обработать такое количество запросов. Результат почти равен результату при неограниченной очереди. На FlameGraph видно, что количество вызовов метода получения новой задачи из пула потока сравнилось с количеством вызвовов при неограниченной очереди. 

<b>Запуск wrk GET запросы на 64 соединений в 4 потока с rate 10000 запросов в секундну:</b>
```
Running 1m test @ http://localhost:8080
  4 threads and 64 connections
  Thread calibration: mean lat.: 1.696ms, rate sampling interval: 10ms
  Thread calibration: mean lat.: 2.516ms, rate sampling interval: 10ms
  Thread calibration: mean lat.: 1.794ms, rate sampling interval: 10ms
  Thread calibration: mean lat.: 1.745ms, rate sampling interval: 10ms
  Thread Stats   Avg      Stdev     Max   +/- Stdev
    Latency     1.87ms    1.42ms  29.46ms   87.46%
    Req/Sec     2.64k   514.89     8.00k    70.25%
  Latency Distribution (HdrHistogram - Recorded Latency)
 50.000%    1.59ms
 75.000%    2.31ms
 90.000%    3.24ms
 99.000%    7.41ms
 99.900%   16.05ms
 99.990%   23.12ms
 99.999%   26.94ms
100.000%   29.47ms
...
#[Mean    =        1.873, StdDeviation   =        1.423]
#[Max     =       29.456, Total count    =       499171]
#[Buckets =           27, SubBuckets     =         2048]
----------------------------------------------------------
  597214 requests in 1.00m, 41.68MB read
Requests/sec:   9953.82
Transfer/sec:    711.43KB
```
Профилирование с помощью async-profiler в течение 15 секунд. FlameGraph для cpu, alloc:

<a href=./resource/profile-html/stage3/stage3-get-cpu-thread4-queue64.html>GET запросы CPU</a>

<a href=./resource/profile-html/stage3/stage3-get-mem-thread4-queue64.html>GET запросы память</a>

Как и в случае с PUT запросами, данный размер очереди позволяет серверу обработать заданное количество GET запросов. Результат совпадает с результатом для неограниченной очереди.


### 6. Результат ThreadPoolExecutor с 4-мя потоками и ограниченной очередью размером 128:
<b>Запуск wrk PUT запросы на 64 соединений в 4 потока с rate 10000 запросов в секундну:</b>
```
Running 1m test @ http://localhost:8080
  4 threads and 64 connections
  Thread calibration: mean lat.: 1.462ms, rate sampling interval: 10ms
  Thread calibration: mean lat.: 1.443ms, rate sampling interval: 10ms
  Thread calibration: mean lat.: 1.484ms, rate sampling interval: 10ms
  Thread calibration: mean lat.: 1.442ms, rate sampling interval: 10ms
  Thread Stats   Avg      Stdev     Max   +/- Stdev
    Latency     1.64ms    2.42ms  56.10ms   96.86%
    Req/Sec     2.64k   506.71    12.22k    91.09%
  Latency Distribution (HdrHistogram - Recorded Latency)
 50.000%    1.26ms
 75.000%    1.77ms
 90.000%    2.40ms
 99.000%   11.20ms
 99.900%   34.81ms
 99.990%   49.98ms
 99.999%   54.53ms
100.000%   56.13ms
...
#[Mean    =        1.643, StdDeviation   =        2.417]
#[Max     =       56.096, Total count    =       499203]
#[Buckets =           27, SubBuckets     =         2048]
----------------------------------------------------------
  599644 requests in 1.00m, 38.31MB read
Requests/sec:   9993.77
Transfer/sec:    653.89KB

```
Профилирование с помощью async-profiler в течение 15 секунд. FlameGraph для cpu, alloc:

<a href=./resource/profile-html/stage3/stage3-put-cpu-thread4-queue128.html>PUT запросы CPU</a>

<a href=./resource/profile-html/stage3/stage3-put-mem-thread4-queue128.html>PUT запросы память</a>

Из отчетов wrk и FlameGraph, видно, что увеличение размера очереди никак не увеличело время ответа на запрос. Время осталось таким же, как и при очереди размером в 64.

<b>Запуск wrk GET запросы на 64 соединений в 4 потока с rate 10000 запросов в секундну:</b>
```
Running 1m test @ http://localhost:8080
  4 threads and 64 connections
  Thread calibration: mean lat.: 1.522ms, rate sampling interval: 10ms
  Thread calibration: mean lat.: 1.500ms, rate sampling interval: 10ms
  Thread calibration: mean lat.: 2.390ms, rate sampling interval: 10ms
  Thread calibration: mean lat.: 1.456ms, rate sampling interval: 10ms
  Thread Stats   Avg      Stdev     Max   +/- Stdev
    Latency     1.87ms    1.27ms  35.74ms   81.29%
    Req/Sec     2.64k   509.82     8.67k    70.04%
  Latency Distribution (HdrHistogram - Recorded Latency)
 50.000%    1.62ms
 75.000%    2.36ms
 90.000%    3.31ms
 99.000%    5.47ms
 99.900%   14.03ms
 99.990%   24.58ms
 99.999%   30.27ms
100.000%   35.78ms
...
#[Mean    =        1.866, StdDeviation   =        1.273]
#[Max     =       35.744, Total count    =       499148]
#[Buckets =           27, SubBuckets     =         2048]
----------------------------------------------------------
  597187 requests in 1.00m, 41.68MB read
Requests/sec:   9953.59
Transfer/sec:    711.42KB

```

Профилирование с помощью async-profiler в течение 15 секунд. FlameGraph для cpu, alloc:

<a href=./resource/profile-html/stage3/stage3-get-cpu-thread4-queue128.html>GET запросы CPU</a>

<a href=./resource/profile-html/stage3/stage3-get-mem-thread4-queue128.html>GET запросы память</a>

Увеличение размера очереди для GET запрос не повлияло на время ответа запроса.


<h4>Общие оптимизации:</h4>
Можно попробовать улучшить результат, например, использовать распределение потоков при одновременных GET и PUT запросах, т.е. увеличить количество потоков на обработку запросов, которые требует большее время на обработку. Либо можно использовать cachedThreadPool и создавать потоки по мере необходимости, при увеличений нагрузки, тем самым можно отдавать ресурсы на другие задачи. Можно задавать параметр keepAliveTime для ThreadPoolExecutor, если есть задачи, которые занимают слишком много времени, снижая общую производительность. 