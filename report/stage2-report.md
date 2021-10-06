## Отчет №2 "Многопоточность"
## Автор: Гаспарян Сократ

### 1. Результаты до реализации многопоточности:
Запуск wrk PUT запросы на 16 соединений на 4 потоках с rate 10000 запросов в секундну:
```
Running 1m test @ http://localhost:8080
  4 threads and 16 connections
  Thread calibration: mean lat.: 19.287ms, rate sampling interval: 20ms
  Thread calibration: mean lat.: 19.958ms, rate sampling interval: 23ms
  Thread calibration: mean lat.: 19.690ms, rate sampling interval: 22ms
  Thread calibration: mean lat.: 19.689ms, rate sampling interval: 22ms
  Thread Stats   Avg      Stdev     Max   +/- Stdev
    Latency    11.96ms   59.50ms 566.27ms   96.79%
    Req/Sec     2.57k     1.28k   32.32k    94.37%
  Latency Distribution (HdrHistogram - Recorded Latency)
 50.000%    1.24ms
 75.000%    1.81ms
 90.000%    4.06ms
 99.000%  397.82ms
 99.900%  539.65ms
 99.990%  563.20ms
 99.999%  565.76ms
100.000%  566.78ms
...
#[Mean    =       11.959, StdDeviation   =       59.505]
#[Max     =      566.272, Total count    =       499767]
#[Buckets =           27, SubBuckets     =         2048]
----------------------------------------------------------
  599892 requests in 1.00m, 38.33MB read
Requests/sec:   9998.74
Transfer/sec:    654.21KB
```
Профилирование с помощью async-profiler в течение 15 секунд. FlameGraph для cpu, alloc и lock:

<a href=./resource/profile-html/stage2/stage2-put-cpu-before-lock-free.html>PUT запросы CPU</a>

<a href=./resource/profile-html/stage2/stage2-put-mem-before-lock-free.html>PUT запросы память</a>

<a href=./resource/profile-html/stage2/stage2-put-lock-before-lock-free.html>PUT запросы lock</a>

На рисунке 1 представлен FlameGraph lock'ов без реализации многопоточности, на котором видно, что количество блокировок равно 69.
<img src=./resource/stage-2/stage2-put-lock-before-lock-free.jpg>
<h6>Рис.1 lock FlameGraph PUT запросов до многопоточности</h6>

Запуск wrk GET запросы на 16 соединений на 4 потоках с rate 10000 запросов в секундну:
```
Running 1m test @ http://localhost:8080
  4 threads and 16 connections
  Thread calibration: mean lat.: 1.236ms, rate sampling interval: 10ms
  Thread calibration: mean lat.: 1.242ms, rate sampling interval: 10ms
  Thread calibration: mean lat.: 1.261ms, rate sampling interval: 10ms
  Thread calibration: mean lat.: 1.248ms, rate sampling interval: 10ms
  Thread Stats   Avg      Stdev     Max   +/- Stdev
    Latency     1.33ms    0.94ms  24.45ms   89.64%
    Req/Sec     2.64k   291.18     6.55k    82.96%
  Latency Distribution (HdrHistogram - Recorded Latency)
 50.000%    1.21ms
 75.000%    1.61ms
 90.000%    2.06ms
 99.000%    4.71ms
 99.900%   12.02ms
 99.990%   18.17ms
 99.999%   22.88ms
100.000%   24.46ms
...
#[Mean    =        1.329, StdDeviation   =        0.937]
#[Max     =       24.448, Total count    =       499796]
#[Buckets =           27, SubBuckets     =         2048]
----------------------------------------------------------
  599920 requests in 1.00m, 39.48MB read
  Non-2xx or 3xx responses: 599920
Requests/sec:   9998.61
Transfer/sec:    673.73KB
```
Профилирование с помощью async-profiler в течение 15 секунд. FlameGraph для cpu, alloc и lock:

<a href=./resource/profile-html/stage2/stage2-get-cpu-before-lock-free.html>GET запросы CPU</a>

<a href=./resource/profile-html/stage2/stage2-get-mem-before-lock-free.html>GET запросы память</a>

<a href=./resource/profile-html/stage2/stage2-get-lock-before-lock-free.html>GET запросы lock</a>

На рисунке 2 представлен FlameGraph lock'ов без реализации многопоточности, на котором видно, что количество блокировок равно 3543.
<img src=./resource/stage-2/stage2-get-lock-before-lock-free.jpg>
<h6>Рис.2 lock FlameGraph GET запросов до многопоточности</h6>


### 2. Результаты реализации с многопоточностью:
### 2.1 Реализация потокобезопасной DAO с использованием java.util.concurrent.* и асинхронным flush.
Запуск wrk PUT запросы на 16 соединений на 4 потоках с rate 10000 запросов в секундну:
```
Running 1m test @ http://localhost:8080
  4 threads and 16 connections
  Thread calibration: mean lat.: 1.617ms, rate sampling interval: 10ms
  Thread calibration: mean lat.: 1.426ms, rate sampling interval: 10ms
  Thread calibration: mean lat.: 1.494ms, rate sampling interval: 10ms
  Thread calibration: mean lat.: 1.441ms, rate sampling interval: 10ms
  Thread Stats   Avg      Stdev     Max   +/- Stdev
    Latency     1.60ms    3.00ms 102.34ms   96.36%
    Req/Sec     2.65k   506.23    14.90k    90.83%
  Latency Distribution (HdrHistogram - Recorded Latency)
 50.000%    1.14ms
 75.000%    1.62ms
 90.000%    2.23ms
 99.000%   13.10ms
 99.900%   37.76ms
 99.990%   86.53ms
 99.999%  100.10ms
100.000%  102.40ms
...
#[Mean    =        1.604, StdDeviation   =        3.004]
#[Max     =      102.336, Total count    =       499790]
#[Buckets =           27, SubBuckets     =         2048]
----------------------------------------------------------
  599912 requests in 1.00m, 38.33MB read
Requests/sec:   9998.71
Transfer/sec:    654.21KB
```
Профилирование с помощью async-profiler в течение 15 секунд. FlameGraph для cpu, alloc и lock:

<a href=./resource/profile-html/stage2/stage2-put-cpu-async-flush.html>PUT запросы CPU</a>

<a href=./resource/profile-html/stage2/stage2-put-mem-async-flush.html>PUT запросы память</a>

<a href=./resource/profile-html/stage2/stage2-put-lock-async-flush.html>PUT запросы lock</a>

<h6>
Как видно из отчета wrk среднее время ответа на запрос сократилось в несколько раз. Также уменьшилось время в доле полученных запросов, что видно из отчета. У доли в 99.0% запросов время ответа уменьшилось в десятки раз. И при увеличений доли перцентилей на каждом шаге время ответа тоже уменьшается по сравнению с реализацией без асинхронного вызова. На FlameGraph'е для CPU добавился асинхронный вызов функции flush, который занимает 8.07% CPU, этот результат можно сравнить с реализацией без асинхронного вызовы, где выполнение flush занимало 22.8%, такая разница происходит, возможно, из-за того, что мы перестаем ожидать завершения всех системных вызовов для записи в основном потоке и дальше обрабатываем запрос. FlameGraph с блокировками оказался пустым, так как в реализации у методов нет блокирующих вызовов и каких-то механизмов синхронизации.
</h6>

Запуск wrk GET запросы на 16 соединений на 4 потоках с rate 10000 запросов в секундну:
```
Running 1m test @ http://localhost:8080
  4 threads and 16 connections
  Thread calibration: mean lat.: 1.866ms, rate sampling interval: 10ms
  Thread calibration: mean lat.: 1.817ms, rate sampling interval: 10ms
  Thread calibration: mean lat.: 1.734ms, rate sampling interval: 10ms
  Thread calibration: mean lat.: 1.739ms, rate sampling interval: 10ms
  Thread Stats   Avg      Stdev     Max   +/- Stdev
    Latency     1.60ms    2.66ms  56.03ms   94.32%
    Req/Sec     2.65k   482.56    10.11k    86.59%
  Latency Distribution (HdrHistogram - Recorded Latency)
 50.000%    1.02ms
 75.000%    1.47ms
 90.000%    2.65ms
 99.000%   14.26ms
 99.900%   30.74ms
 99.990%   47.62ms
 99.999%   53.60ms
100.000%   56.06ms
...
#[Mean    =        1.600, StdDeviation   =        2.665]
#[Max     =       56.032, Total count    =       499785]
#[Buckets =           27, SubBuckets     =         2048]
----------------------------------------------------------
  599907 requests in 1.00m, 41.87MB read
Requests/sec:   9998.80
Transfer/sec:    714.68KB
```
Профилирование с помощью async-profiler в течение 15 секунд. FlameGraph для cpu, alloc и lock:

<a href=/resource/profile-html/stage2/stage2-get-cpu-async-flush.html>GET запросы CPU</a>

<a href=/resource/profile-html/stage2/stage2-get-mem-async-flush.html>GET запросы память</a>

<a href=/resource/profile-html/stage2/stage2-get-lock-async-flush.html>GET запросы lock</a>

<h6>
Как видно из отчета wrk время ответа GET запросов несильно изменилось, так как сам HTTP сервер на этой стадий этапа ещё не многопоточен(без пула потоков). FlameGraph для CPU показывает изменения в вызовах, но это связано с изменением реализации метода get класса ServiceDAO, был заменен цикл на проверку только одного условия. FlameGraph для блокировок пустой, так как у данной реализации больше нет методов с вызовом синхронизации.
</h6>

### 2.2 Реализация многопоточности HTTP сервера с пулом потоков.
Запуск wrk PUT запросы на 16 соединений на 4 потоках с rate 10000 запросов в секундну:
```
Running 1m test @ http://localhost:8080
  4 threads and 16 connections
  Thread calibration: mean lat.: 1.312ms, rate sampling interval: 10ms
  Thread calibration: mean lat.: 1.403ms, rate sampling interval: 10ms
  Thread calibration: mean lat.: 1.357ms, rate sampling interval: 10ms
  Thread calibration: mean lat.: 1.338ms, rate sampling interval: 10ms
  Thread Stats   Avg      Stdev     Max   +/- Stdev
    Latency     1.73ms    2.82ms  96.00ms   94.03%
    Req/Sec     2.65k   575.11    12.30k    86.65%
  Latency Distribution (HdrHistogram - Recorded Latency)
 50.000%    1.11ms
 75.000%    1.61ms
 90.000%    3.02ms
 99.000%   14.18ms
 99.900%   33.92ms
 99.990%   53.89ms
 99.999%   92.80ms
100.000%   96.06ms
...
#[Mean    =        1.728, StdDeviation   =        2.822]
#[Max     =       96.000, Total count    =       499798]
#[Buckets =           27, SubBuckets     =         2048]
----------------------------------------------------------
  599923 requests in 1.00m, 38.33MB read
Requests/sec:   9998.43
Transfer/sec:    654.19KB
```
Профилирование с помощью async-profiler в течение 15 секунд. FlameGraph для cpu, alloc и lock:

<a href=/resource/profile-html/stage2/stage2-put-cpu-threadpool.html>PUT запросы CPU</a>

<a href=/resource/profile-html/stage2/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stage2-put-mem-threadpool.html>PUT запросы память</a>

<a href=/resource/profile-html/stage2/stage2-put-lock-threadpool.html>PUT запросы lock</a>

<img src=/resource/stage-2/stage2-put-lock-threadpool.jpg>
<h6>Рис.3 lock FlameGraph PUT запросов с многопоточностью</h6>

<h6>
В качестве пула потока использовалась реализация ThreadPoolExecutor из java.util.concurrent.ThreadPoolExecutor. Из отчета wrk видно, что среднее время ответа на запрос почти не изменилось и время по перцентилям тоже почти не изменилось. Такое поведение может быть из-за того, что все дорогостоящие операции, т.е. системные вызовы для записи в файл были асинхронными и следовательно добавление дополнительных потоков для сервера не оказали влияния. Во FlameGraph'е для CPU добавились вызовы исполнения методов и синхронизации ThreadPoolExecutor и занимают около 10.25% CPU. На рисунке 3, где изображен FlameGraph для lock'ов, добавились синхронизации для ThreadPoolExecutor 4795 sample'ов. Это возможно из-за того внутри пула потоков используюся средства синхронизации или блокировок для обработки потоком задачи из очереди, это необходимо, чтобы два потока не взяли на выполнение одну и ту же задачу.
</h6>

Запуск wrk GET запросы на 16 соединений на 4 потоках с rate 10000 запросов в секундну:
```
Running 1m test @ http://localhost:8080
  4 threads and 16 connections
  Thread calibration: mean lat.: 3.772ms, rate sampling interval: 10ms
  Thread calibration: mean lat.: 3.694ms, rate sampling interval: 10ms
  Thread calibration: mean lat.: 3.654ms, rate sampling interval: 10ms
  Thread calibration: mean lat.: 3.691ms, rate sampling interval: 10ms
  Thread Stats   Avg      Stdev     Max   +/- Stdev
    Latency     1.27ms    1.02ms  30.70ms   93.16%
    Req/Sec     2.64k   290.23    10.22k    85.51%
  Latency Distribution (HdrHistogram - Recorded Latency)
 50.000%    1.13ms
 75.000%    1.55ms
 90.000%    1.98ms
 99.000%    5.24ms
 99.900%   12.53ms
 99.990%   21.36ms
 99.999%   28.53ms
100.000%   30.72ms
...
#[Mean    =        1.270, StdDeviation   =        1.016]
#[Max     =       30.704, Total count    =       499786]
#[Buckets =           27, SubBuckets     =         2048]
----------------------------------------------------------
  599914 requests in 1.00m, 41.88MB read
Requests/sec:   9998.74
Transfer/sec:    714.68KB
```
Профилирование с помощью async-profiler в течение 15 секунд. FlameGraph для cpu, alloc и lock:

<a href=/resource/profile-html/stage2/stage2-get-cpu-threadpool.html>GET запросы CPU</a>

<a href=/resource/profile-html/stage2/stage2-get-mem-threadpool.html>GET запросы память</a>

<a href=/resource/profile-html/stage2/stage2-get-lock-threadpool.html>GET запросы lock</a>

<img src=/resource/stage-2/stage2-get-lock-threadpool.jpg>
<h6>Рис.4 lock FlameGraph GET запросов с многопоточностью</h6>

<h6>
Результат из wrk показывает незначительные улучшения по сравнению с реалзиацией без многопоточного сервера. На рисунке 4 представлен FlameGraph lock'ов, на нем видно количество блокировок для пула поток 4112 samples, данное значение является большим, чем в первоначальной реализации сервера. 
Также стоит отметить, что выделение потоков тоже занимает время и на продолжительном профилирований, время ответа на запросы на каждом шаге перцентиля будет уменьшаться по сравнению с первоначальной реализацией. 
</h6>

<h4>Общие оптимизации:</h4>
В качестве оптмизации можно предложить использовать пул потоков не только для сервера, но и для LsmDAO для обработки запросов на получение данных. Также можно использовать систему асинхронной обработки запросов. Можно попробовать использовать планировщик задач вместе с профилированием для подсчета статистики и отдавать большее количество потоков на более ресурсоёмкие задачи.




