
## **До многопоточности и оптимизации**

### **PUT запрос**

Проведение нагрузочного тестирования с помощью wrk в не меньше 16 соединений

`     `Нагрузочное тестирование было проведено при следующих параметрах :

- t (thread) : 4
- c (connection) : 16
- d (duration) : 5 mn
- R (rate) : 2000

Thread calibration: mean lat.: 20.938ms, rate sampling interval: 195ms

Thread calibration: mean lat.: 21.254ms, rate sampling interval: 198ms

Thread calibration: mean lat.: 21.334ms, rate sampling interval: 201ms

Thread calibration: mean lat.: 22.793ms, rate sampling interval: 211ms

Thread Stats   Avg      Stdev     Max   +/- Stdev

Latency    52.66ms  300.71ms   3.05s    96.73%

Req/Sec   501.39    172.45     2.65k    95.65%

Latency Distribution (HdrHistogram - Recorded Latency)

50.000%    1.07ms

75.000%    1.44ms

90.000%    2.12ms

99.000%    1.98s

99.900%    2.85s

99.990%    3.04s

99.999%    3.05s

100.000%    3.06s

Отпрофилирование приложения (CPU, alloc и lock)

`  `[Lock](D:\TechnoPolis\Stage2\Ressources\PutRequest\lock___put.html)

При lock никакой блокировки было при PUT и GET запросах.


` `*PAGE   \\* MERGEFORMAT 2*

















На графике ЦП можно наблюдать: пик кода ядра, выполняющего метод send () (29.75% времени ЦП). Селекторы потоков сервера в основном используются процессором (91%) в большинстве случаев. Наш метод entity () из нашего созданного Service, обрабатывающий различные запросы (GET,PUT, Delete), обеспечивает выполнение метода put() (10,74%) с использованием нашего метода upsert (10,74%) нашего DAO, а также ответа на отправку (sendResponse) (33,88%).

В верхней части графика распределения можно заметить,  наличие HeapbytebufferR (13,24%) ответственный за буферизацию только при чтении запросов. При выделении памяти в отличии от использования ЦП, выполнение метода put() занимает 30,88% памяти на основе метода entity(),  а метод ответственный за ответ на отправку (sendResponse) (12,50%).


















### **GET запрос**

Проведение нагрузочного тестирования с помощью wrk в не меньше 16 соединений

`     `Нагрузочное тестирование было проведено при следующих параметрах :

- t (thread) : 4
- c (connection) : 16
- d (duration) : 5 mn
- R (rate) : 2000

Running 5m test @ http://localhost:8080

4 threads and 16 connections

Thread calibration: mean lat.: 59.669ms, rate sampling interval: 458ms

Thread calibration: mean lat.: 58.457ms, rate sampling interval: 448ms

Thread calibration: mean lat.: 58.112ms, rate sampling interval: 442ms

Thread calibration: mean lat.: 58.197ms, rate sampling interval: 444ms

Thread Stats   Avg      Stdev     Max   +/- Stdev

Latency     3.13ms   34.39ms   1.00s    99.50%

Req/Sec   500.80     24.97     1.00k    98.99%

Latency Distribution (HdrHistogram - Recorded Latency)

50.000%    0.98ms

75.000%    1.32ms

90.000%    1.74ms

99.000%   11.74ms

99.900%  711.17ms

99.990%  972.29ms

99.999%  998.40ms

100.000%    1.00s
` `*PAGE   \\* MERGEFORMAT 10*



`	`Отпрофилирование приложения (CPU, alloc и lock) 



[CPU]()



[Alloc]()

Интерпретация графика использования ЦП методом GET существенно не отличается от интерпретации метода PUT. Однако можно заметить, что в какой-то момент времени (немножко больше чем в методе PUT) процессор выполнял операции в сборщик мусора. 

В верхней части графика распределения памяти есть DirectByteBufferR (7,58%) аналогичен HeapbytebufferR, только при выполнении некоторых методов как getInt() из этих классов, реализация DirectByteBufferR оказывается быстрей чем реализация HeapbytebufferR, то, что нам важно при обработке запросов  . Выполнение метода get() занимает 74,24% памяти на основе метода entity(),  а метод ответственный за ответ на отправку (sendResponse) (4,17%).



















## **После многопоточности и оптимизаций**
### **PUT запрос**

Проведение нагрузочного тестирования с помощью wrk в не меньше 16 соединений (без применения *метода compact()*)

Нагрузочное тестирование было проведено при следующих параметрах :

- t (thread) : 4
- c (connection) : 16
- d (duration) : 5 mn
- R (rate) : 2000

Running 5m test @ http://localhost:8080

`  `4 threads and 16 connections

`  `Thread calibration: mean lat.: 397.991ms, rate sampling interval: 1351ms

`  `Thread calibration: mean lat.: 392.832ms, rate sampling interval: 1348ms

`  `Thread calibration: mean lat.: 407.268ms, rate sampling interval: 1344ms

`  `Thread calibration: mean lat.: 395.186ms, rate sampling interval: 1348ms

`  `Thread Stats   Avg      Stdev     Max   +/- Stdev

`    `Latency     3.27ms    9.28ms 230.27ms   95.48%

`    `Req/Sec   499.77      2.50   521.00     90.45%

`  `Latency Distribution (HdrHistogram - Recorded Latency)

` `50.000%    1.34ms

` `75.000%    2.30ms

` `90.000%    6.95ms

` `99.000%   27.17ms

` `99.900%  154.37ms

` `99.990%  203.01ms

` `99.999%  220.41ms

100.000%  230.40ms

`	`Отпрофилирование приложения (CPU, alloc и lock)



Можно отметить разницу в задержках на разных уровнях перцентилей : например, до оптимизации на 99.00% у нас было время выполнения 1.98 s, в отличие от того, что сейчас для одного и того же перцентиля у нас  27.74 ms

Если исходить из глубины оси Y, мы можем заметить, что, как и в предыдущем случае без оптимизации, процесс использования нашего процессора направлен на отправку запросов нашим ядром(метод send (18.95%)). Однако заметно, что использование наших селекторов значительно сократилось и уступило место выполнению потоков из библиотеки java.lang (28.76%). Исходя из этого, можно отметить асинхронное выполнение нашего метода flush() (25,49%) в родительском методе (upsert), которое, в отличие от базовой реализации, больше не обрабатывается в стеке селекторов потоков.


По оси абсцисс на самом низком уровне видно, что распределение памяти достаточно сбалансировано для потоков java.lang и селекторов потоков (тем не менее, с меньшим распределением для первых). Наш метод put предусматривает относительное распределение 44 сэмплов, а метод upsert 168 сэмплов практически столько же (169), сколько и его базовый стек Java. lang.run, беги. Также нашему методу flush() выделяется 37.89% памяти на основе его родительского метода upsert (). Отметим также небольшое распределение памяти для наших регистраторов (7,14%), используемых в методе upsert

### **GET запрос**

Проведение нагрузочного тестирования с помощью wrk в не меньше 16 соединений (без применения *метода compact()*)

Нагрузочное тестирование было проведено при следующих параметрах :

- t (thread) : 4
- c (connection) : 16
- d (duration) : 5 mn
- R (rate) : 2000

Running 5m test @ http://localhost:8080

4 threads and 16 connections

Thread calibration: mean lat.: 6560.238ms, rate sampling interval: 18120ms

Thread calibration: mean lat.: 6606.737ms, rate sampling interval: 18120ms

Thread calibration: mean lat.: 6615.428ms, rate sampling interval: 18153ms

Thread calibration: mean lat.: 6646.342ms, rate sampling interval: 18186ms

Thread Stats   Avg      Stdev     Max   +/- Stdev

Latency     2.37m     1.22m    4.51m    58.88%

Req/Sec    48.63      3.81    55.00     67.74%

Latency Distribution (HdrHistogram - Recorded Latency)

50.000%    2.40m

75.000%    3.40m

90.000%    4.07m

99.000%    4.47m

99.900%    4.52m

99.990%    4.52m

99.999%    4.52m

100.000%    4.52m

Отпрофилирование приложения (CPU, alloc и lock)




Ухудшилось значительно задержка, это можно быть объяснено дополнительным операциями внесены нами связаны со слиянием итераторов в методе range из LSMDAO.

На графике использования ЦП видно, что самой глубокой операцией (на основе оси Y) оказывается поиск и слияние итераторов при поиске данных.

Метод get нашего Http-сервиса занимает процессор, в 95% случаев на основе его родительского метода entity, то, что действительно отличается от предыдущего случая.



Распределение памяти по методу range() значительно увеличилось на 99.69%, оно занимает практически все распределение памяти.









### **PUT запрос**

Проведение нагрузочного тестирования с помощью wrk в не меньше 16 соединений (с методом *compact()* в фоновом потоке)

`     `Нагрузочное тестирование было проведено при следующих параметрах :

- t (thread) : 4
- c (connection) : 16
- d (duration) : 5 mn
- R (rate) : 2000

Running 5m test @ http://localhost:8080

4 threads and 16 connections

Thread calibration: mean lat.: 2088.905ms, rate sampling interval: 6283ms

Thread calibration: mean lat.: 2089.234ms, rate sampling interval: 6266ms

Thread calibration: mean lat.: 2084.657ms, rate sampling interval: 6279ms

Thread calibration: mean lat.: 2086.075ms, rate sampling interval: 6295ms

Thread Stats   Avg      Stdev     Max   +/- Stdev

Latency     1.27m     1.00m    3.43m    57.18%

Req/Sec   151.27     65.55   397.00     85.87%

Latency Distribution (HdrHistogram - Recorded Latency)

50.000%    1.09m

75.000%    2.05m

90.000%    2.86m

99.000%    3.38m

99.900%    3.43m

99.990%    3.43m

99.999%    3.43m

100.000%    3.43m


Отпрофилирование приложения (CPU, alloc и lock)

`  `[Lock](D:\TechnoPolis\Stage2\Ressources\PutRequest\lock___put.html)

При lock никакой блокировки было при PUT и GET запросах.


` `*PAGE   \\* MERGEFORMAT 20*


`	`В отличие от предыдущего, с применением метода performcompact () в методе вставки данных, мы можем отметить, что метод performcompact() занимает значительную часть ЦП (84%), а метод flush() значительно снизился на 8,18%.%	



`	`Распределение памяти достаточно широко для потоков java.lang (72%). Метод flush использует 24,8

0% памяти и performcompact 45.45%.








### **GET запрос**

Проведение нагрузочного тестирования с помощью wrk в не меньше 16 соединений (с методом *compact()* в фоновом потоке)

`     `Нагрузочное тестирование было проведено при следующих параметрах :

- t (thread) : 4
- c (connection) : 16
- d (duration) : 5 mn
- R (rate) : 2000

`  `4 threads and 16 connections

`  `Thread calibration: mean lat.: 30.258ms, rate sampling interval: 241ms

`  `Thread calibration: mean lat.: 29.787ms, rate sampling interval: 228ms

`  `Thread calibration: mean lat.: 29.165ms, rate sampling interval: 220ms

`  `Thread calibration: mean lat.: 29.011ms, rate sampling interval: 215ms

`  `Thread Stats   Avg      Stdev     Max   +/- Stdev

`    `Latency     2.73ms   33.09ms   1.01s    99.68%

`    `Req/Sec   501.46     23.79     1.43k    99.41%

`  `Latency Distribution (HdrHistogram - Recorded Latency)

` `50.000%    1.01ms

` `75.000%    1.34ms

` `90.000%    1.73ms

` `99.000%    5.33ms

` `99.900%  697.34ms

` `99.990%  975.36ms

` `99.999%    1.00s 

100.000%    1.01s




` `*PAGE   \\* MERGEFORMAT 24*



Отпрофилирование приложения (CPU, alloc и lock) 



[]()



[]() 


В рамках оптимизации необходимо было осуществлять асинхронизацию метода flush() и compact(), при этом использовали объект типа CompletableFuture, с методом runAsync() позволяющий выполнять задачу асинхронно в заданный Executor (в нашем случае flushExecutor) в методе flush(). 

В методе compact(), использовали ExecutorService (compactExecutor в нашем случае) с методом execute() чтобы обеспечивать выполнение данной задачи в новом потоке.



