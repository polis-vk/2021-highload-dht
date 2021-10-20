## **Синхронный сервер**

### **PUT запрос**

Проведение нагрузочного тестирования с помощью wrk в не меньше 64 соединений

`     `Нагрузочное тестирование было проведено при следующих параметрах :

- t (thread) : 4
- c (connection) : 64
- d (duration) : 2 mn
- R (rate) : 2000

Running 2m test @ http://localhost:8080

4 threads and 64 connections

Thread calibration: mean lat.: 2085.072ms, rate sampling interval: 6938ms

Thread calibration: mean lat.: 2076.774ms, rate sampling interval: 6840ms

Thread calibration: mean lat.: 2080.772ms, rate sampling interval: 6852ms

Thread calibration: mean lat.: 2084.676ms, rate sampling interval: 6893ms

Thread Stats   Avg      Stdev     Max   +/- Stdev

Latency    27.90s    20.08s    1.10m    55.69%

Req/Sec   220.81     87.36   470.00     85.48%

Latency Distribution (HdrHistogram - Recorded Latency)

50.000%   22.59s

75.000%   45.65s

90.000%    0.95m

99.000%    1.09m

99.900%    1.09m

99.990%    1.10m

99.999%    1.10m

100.000%    1.10m

Отпрофилирование приложения (CPU, alloc и lock)

`  `[Lock](D:\TechnoPolis\Stage2\Ressources\PutRequest\lock___put.html)

При lock никакой блокировки было при PUT и GET запросах.


` `*PAGE   \\* MERGEFORMAT 4*



`	`Количество задач, выполняемых потоками Selector, значительно сократилось, их загрузка ЦП составила 9,0%, а метод entity, отвечающий за обработку запросов, теперь больше не находится под контролем потоков Selector, а теперь управляется пулом исполнителей (ThreadPoolExecutor). Видно, что сборщик мусора был использован значительно.


### **GET запрос**

Проведение нагрузочного тестирования с помощью wrk в не меньше 64 соединений

`     `Нагрузочное тестирование было проведено при следующих параметрах :

- t (thread) : 4
- c (connection) : 16
- d (duration) : 2 mn
- R (rate) : 2000

Running 2m test @ http://localhost:8080

4 threads and 64 connections

Thread calibration: mean lat.: 3.689ms, rate sampling interval: 12ms

Thread calibration: mean lat.: 3.628ms, rate sampling interval: 12ms

Thread calibration: mean lat.: 3.721ms, rate sampling interval: 12ms

Thread calibration: mean lat.: 3.878ms, rate sampling interval: 12ms

Thread Stats   Avg      Stdev     Max   +/- Stdev

Latency     3.74ms   38.60ms   1.00s    99.49%

Req/Sec   522.92    344.00     5.82k    74.47%

Latency Distribution (HdrHistogram - Recorded Latency)

50.000%    1.14ms

75.000%    1.85ms

90.000%    2.59ms

99.000%    8.61ms

99.900%  775.17ms

99.990%  972.29ms

99.999%  998.91ms

100.000%    1.00s


` `*PAGE   \\* MERGEFORMAT 20*



`	`Отпрофилирование приложения (CPU, alloc и lock) 

































## **Асинхронный сервер**

### **PUT запрос**

Проведение нагрузочного тестирования с помощью wrk в не меньше 64 соединений 

Нагрузочное тестирование было проведено при следующих параметрах :

- t (thread) : 4                          PoolSize : 2
- c (connection) : 64               MaxPoolSize : 4
- d (duration) : 2 mn               Capacity (Queue) : 1000
- R (rate) : 2000

Running 2m test @ http://localhost:8080

`  `4 threads and 64 connections

`  `Thread calibration: mean lat.: 2453.724ms, rate sampling interval: 9109ms

`  `Thread calibration: mean lat.: 2723.347ms, rate sampling interval: 8814ms

`  `Thread calibration: mean lat.: 2677.951ms, rate sampling interval: 8749ms

`  `Thread calibration: mean lat.: 2387.516ms, rate sampling interval: 9035ms

`  `Thread Stats   Avg      Stdev     Max   +/- Stdev

`    `Latency    29.21s    17.67s    1.27m    61.39%

`    `Req/Sec   209.31     52.91   302.00     64.58%

`  `Latency Distribution (HdrHistogram - Recorded Latency)

` `50.000%   25.48s 

` `75.000%   42.89s 

` `90.000%    0.94m 

` `99.000%    1.14m 

` `99.900%    1.18m 

` `99.990%    1.27m 

` `99.999%    1.27m 

100.000%    1.27m



Отпрофилирование приложения (CPU, alloc и lock)

















### **GET запрос**

Проведение нагрузочного тестирования с помощью wrk в не меньше 64 соединений 

Нагрузочное тестирование было проведено при следующих параметрах :

- t (thread) : 4                          PoolSize : 2
- c (connection) : 64               MaxPoolSize : 4
- d (duration) : 2 mn               Capacity (Queue) : 1000
- R (rate) : 2000


Running 2m test @ http://localhost:8080

4 threads and 64 connections

Thread calibration: mean lat.: 80.355ms, rate sampling interval: 598ms

Thread calibration: mean lat.: 79.325ms, rate sampling interval: 566ms

Thread calibration: mean lat.: 79.312ms, rate sampling interval: 584ms

Thread calibration: mean lat.: 33.170ms, rate sampling interval: 250ms

Thread Stats   Avg      Stdev     Max   +/- Stdev

Latency     6.18ms   54.49ms   1.00s    99.14%

Req/Sec   501.56     35.44     1.01k    92.01%

Latency Distribution (HdrHistogram - Recorded Latency)

50.000%    1.29ms

75.000%    2.05ms

90.000%    2.92ms

99.000%   24.98ms

99.900%  885.76ms

99.990%  984.58ms

99.999%    1.00s

100.000%    1.00s





Отпрофилирование приложения (CPU, alloc и lock)






























### **PUT запрос**

Проведение нагрузочного тестирования с помощью wrk в не меньше 64 соединений 

Нагрузочное тестирование было проведено при следующих параметрах :

- t (thread) : 4                          PoolSize : 4
- c (connection) : 64               MaxPoolSize : 8
- d (duration) : 2 mn               Capacity (Queue) : 1000
- R (rate) : 2000

Running 2m test @ http://localhost:8080

4 threads and 64 connections

Thread calibration: mean lat.: 3082.666ms, rate sampling interval: 10813ms

Thread calibration: mean lat.: 3125.204ms, rate sampling interval: 10706ms

Thread calibration: mean lat.: 3178.000ms, rate sampling interval: 10919ms

Thread calibration: mean lat.: 3090.980ms, rate sampling interval: 10797ms

Thread Stats   Avg      Stdev     Max   +/- Stdev

Latency    31.59s    18.60s    1.18m    57.54%

Req/Sec   209.90     41.82   288.00     65.00%

Latency Distribution (HdrHistogram - Recorded Latency)

50.000%   28.10s

75.000%   46.83s

90.000%    1.00m

99.000%    1.14m

99.900%    1.17m

99.990%    1.18m

99.999%    1.18m

100.000%    1.18m



Отпрофилирование приложения (CPU, alloc и lock)



















### **GET запрос**

Проведение нагрузочного тестирования с помощью wrk в не меньше 64 соединений 

Нагрузочное тестирование было проведено при следующих параметрах :

- t (thread) : 4                          PoolSize : 4
- c (connection) : 64               MaxPoolSize : 8
- d (duration) : 2 mn               Capacity (Queue) : 1000
- R (rate) : 2000

Running 2m test @ http://localhost:8080

4 threads and 64 connections

Thread calibration: mean lat.: 2.968ms, rate sampling interval: 11ms

Thread calibration: mean lat.: 4.420ms, rate sampling interval: 14ms

Thread calibration: mean lat.: 3.017ms, rate sampling interval: 12ms

Thread calibration: mean lat.: 2.958ms, rate sampling interval: 12ms

Thread Stats   Avg      Stdev     Max   +/- Stdev

Latency     6.80ms   32.02ms 388.86ms   97.41%

Req/Sec   522.68    275.02     2.46k    77.53%

Latency Distribution (HdrHistogram - Recorded Latency)

50.000%    1.50ms

75.000%    2.83ms

90.000%    4.73ms

99.000%  220.16ms

99.900%  330.24ms

99.990%  365.57ms

99.999%  381.95ms

100.000%  389.12ms


Отпрофилирование приложения (CPU, alloc и lock)


































# Выводы





` `*PAGE   \\* MERGEFORMAT 28*

