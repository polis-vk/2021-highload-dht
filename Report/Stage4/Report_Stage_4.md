## **Отчет по шардированию** 
##
## **	
## **Настройки**

`	`Нагрузочное тестирование и профилирование производились на виртуальной машине Ubuntu 21.04 под следующими параметрами : память -*1920 MB*, основной жесткий диск *-40 Гб*,  хост процессор – Intel Celeron *N4000 CPU 1.10GHz*


## **Выбор хеш-функции** 

`	`В рамках данной работы  был выбрана не криптографическая хэш функция murmur2, которая обеспечивает гарантии скорости работы и удовлетворенности предположением о равномерном распределении














### **PUT запрос**

**Проведение нагрузочного тестирования с помощью wrk соединений на порте 8080**

`     `Нагрузочное тестирование было проведено при следующих параметрах :

- t (thread) : 4
- c (connection) : 128
- d (duration) : 1 mn
- R (rate) : 2000

Running 1m test @ http://localhost:8080

4 threads and 128 connections

Thread calibration: mean lat.: 74.010ms, rate sampling interval: 702ms

Thread calibration: mean lat.: 67.852ms, rate sampling interval: 634ms

Thread calibration: mean lat.: 65.852ms, rate sampling interval: 585ms

Thread calibration: mean lat.: 67.642ms, rate sampling interval: 610ms

Thread Stats   Avg      Stdev     Max   +/- Stdev

Latency     5.51ms    7.00ms  63.74ms   90.25%

Req/Sec   499.95     18.74   547.00     67.52%

Latency Distribution (HdrHistogram - Recorded Latency)

50.000%    3.21ms

75.000%    4.69ms

90.000%   12.27ms

99.000%   35.71ms

99.900%   55.01ms

99.990%   61.82ms

99.999%   63.23ms

100.000%   63.78ms

Отпрофилирование приложения (CPU, alloc и lock)

CPU

Alloc

Lock

### **GET запрос**

**Проведение нагрузочного тестирования с помощью wrk соединений на порте 8080**

`     `Нагрузочное тестирование было проведено при следующих параметрах :

- t (thread) : 4
- c (connection) : 128
- d (duration) : 1 mn
- R (rate) : 2000

4 threads and 128 connections

Thread calibration: mean lat.: 4.504ms, rate sampling interval: 14ms

Thread calibration: mean lat.: 2.287ms, rate sampling interval: 10ms

Thread calibration: mean lat.: 3.814ms, rate sampling interval: 12ms

Thread calibration: mean lat.: 2.380ms, rate sampling interval: 10ms

Thread Stats   Avg      Stdev     Max   +/- Stdev

Latency     2.66ms    2.10ms  39.81ms   83.61%

Req/Sec   530.39    650.34     2.91k    90.56%

Latency Distribution (HdrHistogram - Recorded Latency)

50.000%    2.17ms	

75.000%    3.77ms

90.000%    4.77ms

99.000%   10.33ms

99.900%   19.23ms

99.990%   32.61ms

99.999%   38.75ms

100.000%   39.84ms

`	`Отпрофилирование приложения (CPU, alloc и lock) 

CPU

Alloc

Lock


### **PUT запрос**

**Проведение нагрузочного тестирования с помощью wrk соединений на порте 8081**

Нагрузочное тестирование было проведено при следующих параметрах :

- t (thread) : 4
- c (connection) : 128
- d (duration) : 1 mn
- R (rate) : 2000

Thread calibration: mean lat.: 788.472ms, rate sampling interval: 1826ms

Thread calibration: mean lat.: 852.466ms, rate sampling interval: 2099ms

Thread calibration: mean lat.: 852.128ms, rate sampling interval: 2096ms

Thread calibration: mean lat.: 797.627ms, rate sampling interval: 1842ms

Thread Stats   Avg      Stdev     Max   +/- Stdev

Latency   235.61ms  545.57ms   2.07s    86.56%

Req/Sec   511.71     71.54   724.00     86.00%

Latency Distribution (HdrHistogram - Recorded Latency)

50.000%    6.45ms

75.000%   16.07ms	

90.000%    1.24s

99.000%    1.99s

99.900%    2.03s

99.990%    2.06s 

99.999%    2.07s

100.000%    2.07s



Отпрофилирование приложения (CPU, alloc и lock) 

CPU

Alloc

Lock
### `	`**GET запрос**

**Проведение нагрузочного тестирования с помощью wrk соединений на порте 8081**

`     `Нагрузочное тестирование было проведено при следующих параметрах :

- t (thread) : 4
- c (connection) : 128
- d (duration) : 1 mn
- R (rate) : 2000

`  `Thread calibration: mean lat.: 348.409ms, rate sampling interval: 1350ms

`  `Thread calibration: mean lat.: 347.896ms, rate sampling interval: 1349ms

`  `Thread calibration: mean lat.: 349.114ms, rate sampling interval: 1352ms

`  `Thread calibration: mean lat.: 348.391ms, rate sampling interval: 1351ms

`  `Thread Stats   Avg      Stdev     Max   +/- Stdev

`    `Latency   140.99ms  337.58ms   1.43s    87.36%

`    `Req/Sec   499.70     64.49   786.00     83.33%

`  `Latency Distribution (HdrHistogram - Recorded Latency)

` `50.000%    1.56ms

` `75.000%    5.39ms

` `90.000%  704.00ms	

` `99.000%    1.36s 

` `99.900%    1.42s 

` `99.990%    1.43s 

` `99.999%    1.43s 

100.000%    1.43s 

Отпрофилирование приложения (CPU, alloc и lock) 

CPU

Alloc

Lock

### **PUT запрос**

**Проведение нагрузочного тестирования с помощью wrk соединений на порте 8082**

Нагрузочное тестирование было проведено при следующих параметрах :

- t (thread) : 4
- c (connection) : 128
- d (duration) : 1 mn
- R (rate) : 2000

`  `Thread calibration: mean lat.: 1435.463ms, rate sampling interval: 3837ms

`  `Thread calibration: mean lat.: 1435.195ms, rate sampling interval: 3833ms

`  `Thread calibration: mean lat.: 1435.350ms, rate sampling interval: 3835ms

`  `Thread calibration: mean lat.: 1161.697ms, rate sampling interval: 3115ms

`  `Thread Stats   Avg      Stdev     Max   +/- Stdev

`    `Latency   284.37ms  643.24ms   2.25s    85.48%

`    `Req/Sec   519.78     65.70   750.00     90.20%

`  `Latency Distribution (HdrHistogram - Recorded Latency)

` `50.000%    4.10ms

` `75.000%   12.63ms

` `90.000%    1.67s 	

` `99.000%    2.20s 

` `99.900%    2.23s 

` `99.990%    2.24s 

` `99.999%    2.25s 

100.000%    2.25s 



Отпрофилирование приложения (CPU, alloc и lock) 

CPU

Alloc

Lock
### `	`**GET запрос**

**Проведение нагрузочного тестирования с помощью wrk соединений на порте 8082**

`     `Нагрузочное тестирование было проведено при следующих параметрах :

- t (thread) : 4
- c (connection) : 128
- d (duration) : 1 mn
- R (rate) : 2000

`  `Thread calibration: mean lat.: 372.149ms, rate sampling interval: 1076ms

`  `Thread calibration: mean lat.: 374.511ms, rate sampling interval: 1076ms

`  `Thread calibration: mean lat.: 373.516ms, rate sampling interval: 1076ms

`  `Thread calibration: mean lat.: 375.637ms, rate sampling interval: 1076ms

`  `Thread Stats   Avg      Stdev     Max   +/- Stdev

`    `Latency     4.12ms   11.00ms 172.80ms   94.81%

`    `Req/Sec   500.96      7.95   558.00     97.83%

`  `Latency Distribution (HdrHistogram - Recorded Latency)

` `50.000%    1.47ms

` `75.000%    2.25ms

` `90.000%    7.28ms

` `99.000%   56.70ms	

` `99.900%  131.20ms

` `99.990%  161.54ms

` `99.999%  170.88ms

100.000%  172.93ms


Отпрофилирование приложения (CPU, alloc и lock) 

CPU

Alloc

Lock
# **Выводы**

При запуске нагрузочного тестирования на порте 8080, заметили задержки при PUT И GET запросах значительно меньше, чем на остальных портах (8081,8082). 

При профилировании приложения есть следующие замечания:

- - насчет использования процессора видно, что во внесение и получение данных зависит от нода, который был выбран алгоритмом rendezvous hashing и после этого идет либо обращение к нашему DAO, либо к нашему MAP содержащий все ноды.
- - насчет выделения памяти анализ аналогичен. 

При длительности нагрузочного тестирования более 2 минут и/или доля запросов более 5000 запросов в секунду кластер не успевает обрабатывать все запросы (non request 2xx or 3xx responses/ timeout)

\- Основным недостатком rendezvoushashing алгоритма является его временная сложность O(n), где n - количество узлов (нодов). Это очень эффективно, если нам нужно иметь ограниченное количество узлов ( в нашем случае 3). Тем не менее, если мы начнем обслуживать тысячи узлов, это может привести к проблемам с производительностью.
` `*PAGE   \\* MERGEFORMAT 9*

#
#



