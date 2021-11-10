# 2021-highload-dht
Курсовой проект 2021 года [курса](https://polis.mail.ru/curriculum/program/discipline/1257/) "Проектирование высоконагруженных систем" в [Технополис](https://polis.mail.ru).

## Этап 5
## Report

### Introduction
В данном этапе была реализована поддержка репликации за счёт введения timestamp'ов. 
Так же был добавлен новый range, в котором нет TombstoneFilter. Так же с помощью headear'ов у
response была реализовано возможность проверять timestamp, isTombstone у возвращаемого значения в get-методе 
и возможность быстро проверить является ли запрос перенаправленным с другой ноды. 
К сожалению, perfomance очень сильно деградировал 

### Использование wrk2 для нагрузочного тестирования с заданным rate (стабильная нагрузка) 
#### Type request: [PUT](https://github.com/sdimosik/2021-highload-dht/tree/stage5/wrk/put.lua)
#### Запуск wrk2 для put-запроосов
Были запущены 3 экземпляра wrk на каждый узел (output сокращён)
```
wrk2 -c 16 -t 4 -d 1m -R 10k -L -s wrk/put.lua http://localhost:8080
wrk2 -c 16 -t 4 -d 1m -R 10k -L -s wrk/put.lua http://localhost:8081
wrk2 -c 16 -t 4 -d 1m -R 10k -L -s wrk/put.lua http://localhost:8082
```
#### Output
Node 1
```
wrk2 -c 16 -t 4 -d 1m -R 10k -L -s wrk/put.lua http://localhost:8080     
Running 1m test @ http://localhost:8080
  4 threads and 16 connections
  Thread calibration: mean lat.: 2401.075ms, rate sampling interval: 10321ms
  Thread calibration: mean lat.: 2296.764ms, rate sampling interval: 10182ms
  Thread calibration: mean lat.: 2402.248ms, rate sampling interval: 10420ms
  Thread calibration: mean lat.: 2292.102ms, rate sampling interval: 10354ms
  Thread Stats   Avg      Stdev     Max   +/- Stdev
    Latency    21.36s     9.41s   39.65s    60.21%
    Req/Sec   841.94     48.50     0.92k    68.75%
  Latency Distribution (HdrHistogram - Recorded Latency)
 50.000%   20.89s 
 75.000%   28.56s 
 90.000%   35.09s 
 99.000%   38.80s 
 99.900%   39.52s 
 99.990%   39.65s 
 99.999%   39.68s 
100.000%   39.68s 

#[Mean    =    21356.605, StdDeviation   =     9410.876]
#[Max     =    39649.280, Total count    =       163399]
#[Buckets =           27, SubBuckets     =         2048]
----------------------------------------------------------
  205522 requests in 1.00m, 13.13MB read
Requests/sec:   3425.28
Transfer/sec:    224.12KB



```
Node 2
```
wrk2 -c 16 -t 4 -d 1m -R 10k -L -s wrk/put.lua http://localhost:8081   
Running 1m test @ http://localhost:8081
  4 threads and 16 connections
  Thread calibration: mean lat.: 2815.242ms, rate sampling interval: 10870ms
  Thread calibration: mean lat.: 2805.647ms, rate sampling interval: 10944ms
  Thread calibration: mean lat.: 2695.356ms, rate sampling interval: 10706ms
  Thread calibration: mean lat.: 2581.146ms, rate sampling interval: 10518ms
  Thread Stats   Avg      Stdev     Max   +/- Stdev
    Latency    21.78s     9.54s   40.01s    59.86%
    Req/Sec   833.81     61.29     0.90k    68.75%
  Latency Distribution (HdrHistogram - Recorded Latency)
 50.000%   21.20s 
 75.000%   28.93s 
 90.000%   35.52s 
 99.000%   39.45s 
 99.900%   39.88s 
 99.990%   40.01s 
 99.999%   40.04s 
100.000%   40.04s 

#[Mean    =    21775.457, StdDeviation   =     9538.451]
#[Max     =    40009.728, Total count    =       163945]
#[Buckets =           27, SubBuckets     =         2048]
----------------------------------------------------------
  203526 requests in 1.00m, 13.00MB read
Requests/sec:   3391.94
Transfer/sec:    221.93KB

```
Node 3
```
wrk2 -c 16 -t 4 -d 1m -R 10k -L -s wrk/put.lua http://localhost:8082   
Running 1m test @ http://localhost:8082
  4 threads and 16 connections
  Thread calibration: mean lat.: 2998.774ms, rate sampling interval: 11149ms
  Thread calibration: mean lat.: 3065.555ms, rate sampling interval: 11288ms
  Thread calibration: mean lat.: 3138.952ms, rate sampling interval: 11370ms
  Thread calibration: mean lat.: 3043.420ms, rate sampling interval: 11165ms
  Thread Stats   Avg      Stdev     Max   +/- Stdev
    Latency    22.28s     9.76s   39.71s    58.72%
    Req/Sec   830.69     83.50     0.91k    75.00%
  Latency Distribution (HdrHistogram - Recorded Latency)
 50.000%   21.77s 
 75.000%   29.36s 
 90.000%   37.03s 
 99.000%   39.45s 
 99.900%   39.68s 
 99.990%   39.71s 
 99.999%   39.75s 
100.000%   39.75s 


#[Mean    =    22281.823, StdDeviation   =     9755.316]
#[Max     =    39714.816, Total count    =       168809]
#[Buckets =           27, SubBuckets     =         2048]
----------------------------------------------------------
  206789 requests in 1.00m, 13.21MB read
Requests/sec:   3446.51
Transfer/sec:    225.50KB

```


### Async-profiler
#### [Output CPU for PUT-requests](https://htmlpreview.github.io/?https://github.com/sdimosik/2021-highload-dht/blob/stage5/report/stage5/put-cpu.html)
#### [Output ALLOC for PUT-requests](https://htmlpreview.github.io/?https://github.com/sdimosik/2021-highload-dht/blob/stage5/report/stage5/put-alloc.html)
#### [Output LOCK for PUT-requests](https://htmlpreview.github.io/?https://github.com/sdimosik/2021-highload-dht/blob/stage5/report/stage5/put-lock.html)
### Conclusions

1. Сервер НЕ ВЫДЕРЖАЛ нагрузку в 30к req/ses в течении 1m
  
 
2. При профилировании cpu мы видим, что GC занимает 7%, что очень много (на прошлом этапе был 1%). 
Очень много % съедает logger (около 13%).


3. При профилировании alloc мы видим, что invoke всё так же аллоцирует практически 50%. Добавился Logger который съедает 12%


4. При профилировании lock мы видим очень печальную картину - много сэмплов и 95% из них у Logger'а!!!  
Видимо внедрение логера в базовые методы, которые очень часто вызываются - не очень хорошая идея


### Type request: [GET](https://github.com/sdimosik/2021-highload-dht/tree/stage5/wrk/get.lua)
#### Запуск wrk2 для put-запроосов
```
wrk2 -c 16 -t 4 -d 1m -R 10k -L -s wrk/get.lua http://localhost:8080  
wrk2 -c 16 -t 4 -d 1m -R 10k -L -s wrk/get.lua http://localhost:8081
wrk2 -c 16 -t 4 -d 1m -R 10k -L -s wrk/get.lua http://localhost:8082  
```

#### Output
Node 1
```
Running 1m test @ http://localhost:8080
  4 threads and 16 connections
  Thread calibration: mean lat.: 9223372036854776.000ms, rate sampling interval: 10ms
  Thread calibration: mean lat.: 9223372036854776.000ms, rate sampling interval: 10ms
  Thread calibration: mean lat.: 9223372036854776.000ms, rate sampling interval: 10ms
  Thread calibration: mean lat.: 9223372036854776.000ms, rate sampling interval: 10ms
  Thread Stats   Avg      Stdev     Max   +/- Stdev
    Latency     -nanus    -nanus   0.00us    0.00%
    Req/Sec     0.00      0.00     0.00    100.00%
  Latency Distribution (HdrHistogram - Recorded Latency)
 50.000%    0.00us
 75.000%    0.00us
 90.000%    0.00us
 99.000%    0.00us
 99.900%    0.00us
 99.990%    0.00us
 99.999%    0.00us
100.000%    0.00us

  Detailed Percentile spectrum:
       Value   Percentile   TotalCount 1/(1-Percentile)

       0.000     1.000000            0          inf
#[Mean    =         -nan, StdDeviation   =         -nan]
#[Max     =        0.000, Total count    =            0]
#[Buckets =           27, SubBuckets     =         2048]
----------------------------------------------------------
  0 requests in 1.00m, 0.00B read
  Socket errors: connect 0, read 163844, write 0, timeout 0
Requests/sec:      0.00
Transfer/sec:       0.00B



```
Node 2
```
wrk2 -c 16 -t 4 -d 1m -R 10k -L -s wrk/get.lua http://localhost:8081   
Running 1m test @ http://localhost:8081
  4 threads and 16 connections
  Thread calibration: mean lat.: 9223372036854776.000ms, rate sampling interval: 10ms
  Thread calibration: mean lat.: 9223372036854776.000ms, rate sampling interval: 10ms
  Thread calibration: mean lat.: 9223372036854776.000ms, rate sampling interval: 10ms
  Thread calibration: mean lat.: 9223372036854776.000ms, rate sampling interval: 10ms
  Thread Stats   Avg      Stdev     Max   +/- Stdev
    Latency     -nanus    -nanus   0.00us    0.00%
    Req/Sec     0.00      0.00     0.00    100.00%
  Latency Distribution (HdrHistogram - Recorded Latency)
 50.000%    0.00us
 75.000%    0.00us
 90.000%    0.00us
 99.000%    0.00us
 99.900%    0.00us
 99.990%    0.00us
 99.999%    0.00us
100.000%    0.00us

  Detailed Percentile spectrum:
       Value   Percentile   TotalCount 1/(1-Percentile)

       0.000     1.000000            0          inf
#[Mean    =         -nan, StdDeviation   =         -nan]
#[Max     =        0.000, Total count    =            0]
#[Buckets =           27, SubBuckets     =         2048]
----------------------------------------------------------
  0 requests in 1.00m, 0.00B read
  Socket errors: connect 0, read 162525, write 0, timeout 0
Requests/sec:      0.00
Transfer/sec:       0.00B

```
Node 3
```
wrk2 -c 16 -t 4 -d 1m -R 10k -L -s wrk/get.lua http://localhost:8082   
Running 1m test @ http://localhost:8082
  4 threads and 16 connections
  Thread calibration: mean lat.: 9223372036854776.000ms, rate sampling interval: 10ms
  Thread calibration: mean lat.: 9223372036854776.000ms, rate sampling interval: 10ms
  Thread calibration: mean lat.: 9223372036854776.000ms, rate sampling interval: 10ms
  Thread calibration: mean lat.: 9223372036854776.000ms, rate sampling interval: 10ms
  Thread Stats   Avg      Stdev     Max   +/- Stdev
    Latency     -nanus    -nanus   0.00us    0.00%
    Req/Sec     0.00      0.00     0.00    100.00%
  Latency Distribution (HdrHistogram - Recorded Latency)
 50.000%    0.00us
 75.000%    0.00us
 90.000%    0.00us
 99.000%    0.00us
 99.900%    0.00us
 99.990%    0.00us
 99.999%    0.00us
100.000%    0.00us

  Detailed Percentile spectrum:
       Value   Percentile   TotalCount 1/(1-Percentile)

       0.000     1.000000            0          inf
#[Mean    =         -nan, StdDeviation   =         -nan]
#[Max     =        0.000, Total count    =            0]
#[Buckets =           27, SubBuckets     =         2048]
----------------------------------------------------------
  0 requests in 1.00m, 0.00B read
  Socket errors: connect 0, read 164508, write 0, timeout 0
Requests/sec:      0.00
Transfer/sec:       0.00B

```

### Async-profiler
#### [Output CPU for GET-requests](https://htmlpreview.github.io/?https://github.com/sdimosik/2021-highload-dht/blob/stage5/report/stage5/get-cpu.html)
#### [Output ALLOC for GET-requests](https://htmlpreview.github.io/?https://github.com/sdimosik/2021-highload-dht/blob/stage5/report/stage5/get-alloc.html)
#### [Output LOCK for GET-requests](https://htmlpreview.github.io/?https://github.com/sdimosik/2021-highload-dht/blob/stage5/report/stage5/get-lock.html)

### Conclusions

1. Сервер вообще ничего не выдержал


2. При профилировании CPU ммы видим схожу картину с put запросами


3. При профилировании alloc мы видим схожу картину с put запросами


4. При профилировании lock мы видим очень много сэмплов и у логгера 97%. Видимо из-за этого прочитать данные почему-то не получилось,
хоть вроде всё считывалось, sstable лежали в папках, тесты проходили