## Отчет №6 "Асинхронный клиент"
## Автор: Гаспарян Сократ

### Результаты нагрузочного тестирования с асинхронными узлами кластера
<b>Запуск wrk PUT запросы на 128 соединений в 2 потока с rate 10000 запросов в секундну - 1 минута:</b>
<b>Узел с портом 8080</b>
```
Running 1m test @ http://localhost:8080
  2 threads and 128 connections
  Thread calibration: mean lat.: 14.096ms, rate sampling interval: 77ms
  Thread calibration: mean lat.: 12.402ms, rate sampling interval: 64ms
  Thread Stats   Avg      Stdev     Max   +/- Stdev
    Latency     9.85ms   11.99ms 112.19ms   83.25%
    Req/Sec     5.05k     1.14k    8.66k    68.00%
  Latency Distribution (HdrHistogram - Recorded Latency)
 50.000%    3.18ms
 75.000%   15.56ms
 90.000%   28.45ms
 99.000%   47.84ms
 99.900%   66.43ms
 99.990%   86.65ms
 99.999%  108.99ms
100.000%  112.25ms
...
#[Mean    =        9.851, StdDeviation   =       11.993]
#[Max     =      112.192, Total count    =       496948]
#[Buckets =           27, SubBuckets     =         2048]
----------------------------------------------------------
  598476 requests in 1.00m, 38.40MB read
Requests/sec:   9974.43
Transfer/sec:    655.34KB
```

<b>Узел с портом 8081</b>
```
Running 1m test @ http://localhost:8081
  2 threads and 128 connections
  Thread calibration: mean lat.: 11.040ms, rate sampling interval: 58ms
  Thread calibration: mean lat.: 16.579ms, rate sampling interval: 75ms
  Thread Stats   Avg      Stdev     Max   +/- Stdev
    Latency    12.84ms   14.71ms 142.90ms   86.71%
    Req/Sec     5.05k     1.22k   10.46k    68.96%
  Latency Distribution (HdrHistogram - Recorded Latency)
 50.000%    7.38ms
 75.000%   19.10ms
 90.000%   30.75ms
 99.000%   62.53ms
 99.900%  125.57ms
 99.990%  131.29ms
 99.999%  137.32ms
100.000%  142.03ms
...
#[Mean    =       12.844, StdDeviation   =       14.707]
#[Max     =      142.904, Total count    =       496606]
#[Buckets =           27, SubBuckets     =         2048]
----------------------------------------------------------
  594024 requests in 1.00m, 51.45MB read
Requests/sec:   9900.26
Transfer/sec:      0.86MB
```

<b>Узел с портом 8082</b>
```
Running 1m test @ http://localhost:8082
  2 threads and 128 connections
  Thread calibration: mean lat.: 11.068ms, rate sampling interval: 57ms
  Thread calibration: mean lat.: 14.706ms, rate sampling interval: 69ms
  Thread Stats   Avg      Stdev     Max   +/- Stdev
    Latency    12.11ms   12.64ms 115.26ms   83.59%
    Req/Sec     5.06k     1.26k    9.46k    68.78%
  Latency Distribution (HdrHistogram - Recorded Latency)
 50.000%    6.50ms
 75.000%   18.59ms
 90.000%   31.10ms
 99.000%   52.74ms
 99.900%   72.83ms
 99.990%   87.68ms
 99.999%  106.24ms
100.000%  115.33ms
...
#[Mean    =       12.108, StdDeviation   =       12.645]
#[Max     =      115.264, Total count    =       496818]
#[Buckets =           27, SubBuckets     =         2048]
----------------------------------------------------------
  594272 requests in 1.00m, 51.52MB read
Requests/sec:   9904.33
Transfer/sec:      0.86MB
```

<b>Профилирование с помощью async-profiler в течение 20 секунд. FlameGraph для cpu, alloc, lock:</b>

<a href=./resource/profile-html/stage6/stage6-put-cpu.html>PUT запросы CPU</a>

<a href=./resource/profile-html/stage6/stage6-put-mem.html>PUT запросы память</a>

<a href=./resource/profile-html/stage6/stage6-put-lock.html>PUT запросы блокировки</a>


<b>Вывод по результатам:</b>
Как видно из отчета wrk реализация для  PUT запросов показывает результаты хуже, чем было до асинхронного опроса узлов кластера. Из данных профилировщика видно, что множество системных вызововов приходится на обработку потоков. Блокировки в данной реализации связаны только с блокирующей очередью в пуле потоков, никаких других блокировок не происходит.  


<b>Запуск wrk GET запросы на 128 соединений в 2 потока с rate 10000 запросов в секундну - 1 минута:</b>
<b>Узел с портом 8080</b>
```
Running 1m test @ http://localhost:8080
  2 threads and 128 connections
  Thread calibration: mean lat.: 8.698ms, rate sampling interval: 53ms
  Thread calibration: mean lat.: 8.440ms, rate sampling interval: 50ms
  Thread Stats   Avg      Stdev     Max   +/- Stdev
    Latency     8.08ms   10.24ms  59.65ms   82.16%
    Req/Sec     5.07k     1.25k    9.84k    68.14%
  Latency Distribution (HdrHistogram - Recorded Latency)
 50.000%    2.21ms
 75.000%   12.86ms
 90.000%   25.14ms
 99.000%   39.01ms
 99.900%   47.87ms
 99.990%   55.39ms
 99.999%   58.78ms
100.000%   59.68ms
...
#[Mean    =        8.077, StdDeviation   =       10.245]
#[Max     =       59.648, Total count    =       496885]
#[Buckets =           27, SubBuckets     =         2048]
----------------------------------------------------------
  598476 requests in 1.00m, 42.00MB read
Requests/sec:   9974.56
Transfer/sec:    716.88KB
```

<b>Узел с портом 8081</b>
```
Running 1m test @ http://localhost:8081
  2 threads and 128 connections
  Thread calibration: mean lat.: 8.371ms, rate sampling interval: 43ms
  Thread calibration: mean lat.: 7.890ms, rate sampling interval: 41ms
  Thread Stats   Avg      Stdev     Max   +/- Stdev
    Latency     8.07ms    9.27ms  51.30ms   81.75%
    Req/Sec     5.07k     1.24k    8.19k    65.94%
  Latency Distribution (HdrHistogram - Recorded Latency)
 50.000%    2.61ms
 75.000%   13.05ms
 90.000%   23.61ms
 99.000%   34.43ms
 99.900%   40.29ms
 99.990%   44.99ms
 99.999%   49.25ms
100.000%   51.33ms
...
#[Mean    =        8.072, StdDeviation   =        9.267]
#[Max     =       51.296, Total count    =       496918]
#[Buckets =           27, SubBuckets     =         2048]
----------------------------------------------------------
  598475 requests in 1.00m, 42.00MB read
Requests/sec:   9974.38
Transfer/sec:    716.86KB
```

<b>Узел с портом 8082</b>
```
Running 1m test @ http://localhost:8082
  2 threads and 128 connections
  Thread calibration: mean lat.: 8.399ms, rate sampling interval: 46ms
  Thread calibration: mean lat.: 12.584ms, rate sampling interval: 58ms
  Thread Stats   Avg      Stdev     Max   +/- Stdev
    Latency    11.27ms   10.33ms  61.02ms   78.88%
    Req/Sec     5.07k     1.25k    8.61k    66.81%
  Latency Distribution (HdrHistogram - Recorded Latency)
 50.000%    6.95ms
 75.000%   17.39ms
 90.000%   27.36ms
 99.000%   41.31ms
 99.900%   49.34ms
 99.990%   55.17ms
 99.999%   60.38ms
100.000%   61.06ms
...
#[Mean    =       11.271, StdDeviation   =       10.332]
#[Max     =       61.024, Total count    =       496857]
#[Buckets =           27, SubBuckets     =         2048]
----------------------------------------------------------
  594252 requests in 1.00m, 41.71MB read
Requests/sec:   9902.29
Transfer/sec:    711.65KB
```

<b>Профилирование с помощью async-profiler в течение 20 секунд. FlameGraph для cpu, alloc, lock:</b>

<a href=./resource/profile-html/stage6/stage6-get-cpu.html>GET запросы CPU</a>

<a href=./resource/profile-html/stage6/stage6-get-mem.html>GET запросы память</a>

<a href=./resource/profile-html/stage6/stage6-get-lock.html>GET запросы блокировки</a>


<b>Вывод по результатам:</b>
Как видно из отчета wrk реализация для GET запросов показывает результаты такие же, как и до асинхронного опроса узлов кластера. Из данных профилировщика видно, что множество системных вызововов приходится на обработку поиска данных в хранилище DAO и на обработку потоков в пуле. Блокировки в данной реализации связаны только с блокирующей очередью в пуле потоков, никаких других блокировок не происходит.  



<h4>Общий вывод и оптимизации:</h4>
Из отчетов wrk и async-profiler видно, что добавление асинхронного опроса узлов кластера, не изменило или ухудшила некоторые показатели, скорее всего это связано с тем, что системе не хватает производительности для обслуживания такого количества потоков. Большая часть вызовов связано с работой с потоками и блокирующими очередями в пуле потоков. В качестве оптимизации можно предложить опрос CompletableFuture на готовность выполнения потока с помощью метода isDone и, если, метод указывает, что поток ещё выполняет работу, то можно опросить следующий Future, выполняя это действие циклично. В итоговой реалзации не использовался HttpClient из java.net.http.HttpClient, так как при его использований сильно проседает производительносить. Результаты wrk показывают до 17 секунд по 99.999 персентилю, скорее всего это связано с тем, что HttpClient не держит соединение с узлами, а устанавливает его по факту в HttpRequest, тем самым тратится время на установление соединение при каждом запросе. Результаты замера выполнения метода ReplicationService.handleRequest для обычного синхронного клиента java.one.nio.HttpClient 0-1 мс, а для асинхронного java.net.http.HttpClient 1-21 мс.