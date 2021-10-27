# 2021-highload-dht
Курсовой проект 2021 года [курса](https://polis.mail.ru/curriculum/program/discipline/1257/) "Проектирование высоконагруженных систем" в [Технополис](https://polis.mail.ru).

## Этап 4. Шардирование (deadline 2021-10-27 23:59:59 MSK)
## Report

### Introduction
В данном этапе была реализована поддержка распределения данных, которая основывается на алгоритме консистентного
хеширования. Так же в данной работе был использован алгоритм хеширования MD5. С помощью него мы можем однозначно вычислить
хэш

### Использование wrk2 для нагрузочного тестирования с заданным rate (стабильная нагрузка) 
#### Type request: [PUT](https://github.com/sdimosik/2021-highload-dht/tree/stage2/wrk/put.lua)
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
  Thread calibration: mean lat.: 1.193ms, rate sampling interval: 10ms
  Thread calibration: mean lat.: 1.151ms, rate sampling interval: 10ms
  Thread calibration: mean lat.: 1.210ms, rate sampling interval: 10ms
  Thread calibration: mean lat.: 1.269ms, rate sampling interval: 10ms
  Thread Stats   Avg      Stdev     Max   +/- Stdev
    Latency     1.08ms  822.65us  21.20ms   92.49%
    Req/Sec     2.64k   261.03     7.33k    88.50%
  Latency Distribution (HdrHistogram - Recorded Latency)
 50.000%    0.98ms
 75.000%    1.35ms
 90.000%    1.65ms
 99.000%    4.69ms
 99.900%    9.10ms
 99.990%   16.74ms
 99.999%   19.58ms
100.000%   21.22ms

#[Mean    =        1.083, StdDeviation   =        0.823]
#[Max     =       21.200, Total count    =       499788]
#[Buckets =           27, SubBuckets     =         2048]
----------------------------------------------------------
  599912 requests in 1.00m, 49.30MB read
Requests/sec:   9998.78
Transfer/sec:    841.35KB
```
Node 2
```
wrk2 -c 16 -t 4 -d 1m -R 10k -L -s wrk/put.lua http://localhost:8081   
Running 1m test @ http://localhost:8081
  4 threads and 16 connections
  Thread calibration: mean lat.: 1.208ms, rate sampling interval: 10ms
  Thread calibration: mean lat.: 1.205ms, rate sampling interval: 10ms
  Thread calibration: mean lat.: 1.140ms, rate sampling interval: 10ms
  Thread calibration: mean lat.: 1.180ms, rate sampling interval: 10ms
  Thread Stats   Avg      Stdev     Max   +/- Stdev
    Latency     1.06ms  812.10us  20.99ms   92.54%
    Req/Sec     2.64k   269.88     7.40k    88.89%
  Latency Distribution (HdrHistogram - Recorded Latency)
 50.000%    0.96ms
 75.000%    1.32ms
 90.000%    1.61ms
 99.000%    4.62ms
 99.900%    9.45ms
 99.990%   15.81ms
 99.999%   19.22ms
100.000%   21.01ms

#[Mean    =        1.062, StdDeviation   =        0.812]
#[Max     =       20.992, Total count    =       499800]
#[Buckets =           27, SubBuckets     =         2048]
----------------------------------------------------------
  599923 requests in 1.00m, 47.45MB read
Requests/sec:   9998.81
Transfer/sec:    809.84KB
```
Node 3
```
wrk2 -c 16 -t 4 -d 1m -R 10k -L -s wrk/put.lua http://localhost:8082   
Running 1m test @ http://localhost:8082
  4 threads and 16 connections
  Thread calibration: mean lat.: 1.165ms, rate sampling interval: 10ms
  Thread calibration: mean lat.: 1.220ms, rate sampling interval: 10ms
  Thread calibration: mean lat.: 1.155ms, rate sampling interval: 10ms
  Thread calibration: mean lat.: 1.154ms, rate sampling interval: 10ms
  Thread Stats   Avg      Stdev     Max   +/- Stdev
    Latency     1.06ms    0.87ms  28.30ms   94.45%
    Req/Sec     2.63k   273.45     8.89k    89.55%
  Latency Distribution (HdrHistogram - Recorded Latency)
 50.000%    0.95ms
 75.000%    1.31ms
 90.000%    1.61ms
 99.000%    4.66ms
 99.900%   10.18ms
 99.990%   20.80ms
 99.999%   25.76ms
100.000%   28.32ms

#[Mean    =        1.057, StdDeviation   =        0.869]
#[Max     =       28.304, Total count    =       499768]
#[Buckets =           27, SubBuckets     =         2048]
----------------------------------------------------------
  599892 requests in 1.00m, 45.71MB read
Requests/sec:   9998.31
Transfer/sec:    780.12KB
```


### Async-profiler
#### [Output CPU for PUT-requests](https://htmlpreview.github.io/?https://github.com/sdimosik/2021-highload-dht/blob/stage4/report/stage4/put-cpu.html)
#### [Output ALLOC for PUT-requests](https://htmlpreview.github.io/?https://github.com/sdimosik/2021-highload-dht/blob/stage4/report/stage4/put-alloc.html)
#### [Output LOCK for PUT-requests](https://htmlpreview.github.io/?https://github.com/sdimosik/2021-highload-dht/blob/stage4/report/stage4/put-lock.html)
### Conclusions

1. Сервер выдержал нагрузку в 30к req/ses в течении 1m
  
 
2. При профилировании cpu мы видим, что на графике появился HttpClient.invoke, с помощью которого мы перенаправляем
запросы на нужные узлы. Остальное без изменений


3. При профилировании alloc мы видим, что появилась функция хэширования MD5, которая потребляет 10%. А метод invoke у
HttpClient занимает целых 50%! Скорее всего виной всему протокол http


4. При профилировании lock мы видим такую же картину, что и в прошлом stage


### Type request: [GET](https://github.com/sdimosik/2021-highload-dht/tree/stage2/wrk/get.lua)
#### Запуск wrk2 для put-запроосов
```
wrk2 -c 16 -t 4 -d 1m -R 10k -L -s wrk/get.lua http://localhost:8080  
wrk2 -c 16 -t 4 -d 1m -R 10k -L -s wrk/get.lua http://localhost:8081
wrk2 -c 16 -t 4 -d 1m -R 10k -L -s wrk/get.lua http://localhost:8082  
```

#### Output
Node 1
```
wrk2 -c 16 -t 4 -d 1m -R 10k -L -s wrk/get.lua http://localhost:8080   
Running 1m test @ http://localhost:8080
  4 threads and 16 connections
  Thread calibration: mean lat.: 1.163ms, rate sampling interval: 10ms
  Thread calibration: mean lat.: 1.119ms, rate sampling interval: 10ms
  Thread calibration: mean lat.: 1.132ms, rate sampling interval: 10ms
  Thread calibration: mean lat.: 1.136ms, rate sampling interval: 10ms
  Thread Stats   Avg      Stdev     Max   +/- Stdev
    Latency     1.07ms  530.74us  11.81ms   70.35%
    Req/Sec     2.64k   188.75     4.55k    73.91%
  Latency Distribution (HdrHistogram - Recorded Latency)
 50.000%    1.02ms
 75.000%    1.39ms
 90.000%    1.70ms
 99.000%    2.48ms
 99.900%    4.72ms
 99.990%    7.22ms
 99.999%    9.63ms
100.000%   11.81ms

#[Mean    =        1.065, StdDeviation   =        0.531]
#[Max     =       11.808, Total count    =       499790]
#[Buckets =           27, SubBuckets     =         2048]
----------------------------------------------------------
  599913 requests in 1.00m, 52.84MB read
Requests/sec:   9998.73
Transfer/sec:      0.88MB
```
Node 2
```
wrk2 -c 16 -t 4 -d 1m -R 10k -L -s wrk/get.lua http://localhost:8081   
Running 1m test @ http://localhost:8081
  4 threads and 16 connections
  Thread calibration: mean lat.: 1.090ms, rate sampling interval: 10ms
  Thread calibration: mean lat.: 1.093ms, rate sampling interval: 10ms
  Thread calibration: mean lat.: 1.092ms, rate sampling interval: 10ms
  Thread calibration: mean lat.: 1.079ms, rate sampling interval: 10ms
  Thread Stats   Avg      Stdev     Max   +/- Stdev
    Latency     1.06ms  537.91us  11.26ms   70.44%
    Req/Sec     2.64k   187.81     4.80k    73.86%
  Latency Distribution (HdrHistogram - Recorded Latency)
 50.000%    1.01ms
 75.000%    1.39ms
 90.000%    1.70ms
 99.000%    2.49ms
 99.900%    4.84ms
 99.990%    7.27ms
 99.999%   10.34ms
100.000%   11.26ms

#[Mean    =        1.059, StdDeviation   =        0.538]
#[Max     =       11.256, Total count    =       499792]
#[Buckets =           27, SubBuckets     =         2048]
----------------------------------------------------------
  599917 requests in 1.00m, 50.99MB read
Requests/sec:   9998.74
Transfer/sec:    870.30KB
```
Node 3
```
wrk2 -c 16 -t 4 -d 1m -R 10k -L -s wrk/get.lua http://localhost:8082   
Running 1m test @ http://localhost:8082
  4 threads and 16 connections
  Thread calibration: mean lat.: 1.063ms, rate sampling interval: 10ms
  Thread calibration: mean lat.: 1.062ms, rate sampling interval: 10ms
  Thread calibration: mean lat.: 1.050ms, rate sampling interval: 10ms
  Thread calibration: mean lat.: 1.057ms, rate sampling interval: 10ms
  Thread Stats   Avg      Stdev     Max   +/- Stdev
    Latency     1.04ms  523.46us  10.99ms   69.81%
    Req/Sec     2.64k   179.89     3.80k    74.68%
  Latency Distribution (HdrHistogram - Recorded Latency)
 50.000%    1.00ms
 75.000%    1.37ms
 90.000%    1.67ms
 99.000%    2.44ms
 99.900%    4.54ms
 99.990%    6.82ms
 99.999%    9.45ms
100.000%   11.00ms

#[Mean    =        1.044, StdDeviation   =        0.523]
#[Max     =       10.992, Total count    =       499794]
#[Buckets =           27, SubBuckets     =         2048]
----------------------------------------------------------
  599918 requests in 1.00m, 49.25MB read
Requests/sec:   9998.65
Transfer/sec:    840.61KB
```

### Async-profiler
#### [Output CPU for GET-requests](https://htmlpreview.github.io/?https://github.com/sdimosik/2021-highload-dht/blob/stage4/report/stage4/get-cpu.html)
#### [Output ALLOC for GET-requests](https://htmlpreview.github.io/?https://github.com/sdimosik/2021-highload-dht/blob/stage4/report/stage4/get-alloc.html)
#### [Output LOCK for GET-requests](https://htmlpreview.github.io/?https://github.com/sdimosik/2021-highload-dht/blob/stage4/report/stage4/get-lock.html)

### Conclusions

1. Сервер выдержал нагрузку в 30к req/ses в течении 1m


2. При профилировании CPU мы видим, что появился HttpClient.invoke, как и при профилировании put-cpu. 
Остальное осталось без изменений


3. При профилировании alloc мы видим схожу картину, что и с put-запросами, но уже подсчёт хэша и перенаправление запроса
отъедает меньше памяти


4. При профилировании lock мы видим такую же картину, что и в прошлом stage