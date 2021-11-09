# Stage 5 Report

## Нагрузочное тестирование `PUT`-запросами

```bash
wrk -c 64 -t 16 -d5m -R 15000 -L -s scripts/put.lua http://localhost:8080
```

### CPU

```bash
 ./profiler.sh -e cpu -d 270 -f putCpuReplication.html 6092
```

### Alloc

```bash
 ./profiler.sh -e alloc -d 270 -f putAllocReplication.html 6092
```

### Lock

```bash
 ./profiler.sh -e lock -d 270 -f putLockReplication.html 6092
```

## Нагрузочное тестирование `GET`-запросами

```bash
wrk -c 64 -t 16 -d5m -R 15000 -L -s scripts/get.lua http://localhost:8080
```

### CPU

```bash
 ./profiler.sh -e cpu -d 270 -f getCpuReplication.html 6092
```

### Alloc

```bash
 ./profiler.sh -e alloc -d 270 -f getAllocReplication.html 6092
```

### Lock

```bash
 ./profiler.sh -e lock -d 270 -f getLockReplication.html 6092
```

## Результаты

### PUT

```text 
  Thread Stats   Avg      Stdev     Max   +/- Stdev
    Latency    12.56ms   50.18ms 508.42ms   95.30%
    Req/Sec     0.99k   169.23     3.00k    74.21%
  Latency Distribution (HdrHistogram - Recorded Latency)
 50.000%    1.61ms
 75.000%    2.44ms
 90.000%    8.76ms
 99.000%  351.74ms
 99.900%  431.36ms
 99.990%  470.78ms
 99.999%  495.61ms
100.000%  508.67ms

```

### GET

```text
  Thread Stats   Avg      Stdev     Max   +/- Stdev
    Latency   191.03ms  304.43ms   1.28s    83.43%
    Req/Sec     0.99k   198.07     2.00k    71.07%
  Latency Distribution (HdrHistogram - Recorded Latency)
 50.000%    4.72ms
 75.000%  302.59ms
 90.000%  728.58ms
 99.000%    1.10s
 99.900%    1.21s
 99.990%    1.25s
 99.999%    1.27s
100.000%    1.28s

```

## Профилировщик

|     | CPU | Alloc | Lock |
| --- | --- | ----- | ---- |
| GET | [ссылка](https://htmlpreview.github.io/?https://github.com/IgorSamohin/2021-highload-dht/blob/igor-samokhin-content/graphs/stage5/getCpuReplication.html) | [ссылка](https://htmlpreview.github.io/?https://github.com/IgorSamohin/2021-highload-dht/blob/igor-samokhin-content/graphs/stage5/getAllocReplication.html) | [ссылка](https://htmlpreview.github.io/?https://github.com/IgorSamohin/2021-highload-dht/blob/igor-samokhin-content/graphs/stage5/getLockReplication.html) |   
| PUT | [ссылка](https://htmlpreview.github.io/?https://github.com/IgorSamohin/2021-highload-dht/blob/igor-samokhin-content/graphs/stage5/putCpuReplication.html) | [ссылка](https://htmlpreview.github.io/?https://github.com/IgorSamohin/2021-highload-dht/blob/igor-samokhin-content/graphs/stage5/putAllocReplication.html) | [ссылка](https://htmlpreview.github.io/?https://github.com/IgorSamohin/2021-highload-dht/blob/igor-samokhin-content/graphs/stage5/putLockReplication.html) |

## Выводы

* Стало интересно, сколько проблем приносит неправильная конфигурация HttpClient, поэтому профилировка производилась при неправильной. В результате на `Lock` графах видно, что примерно 19% блокировок - вызов HttpClient.invoke.
* После проведения некоторой конфигурации удалось полностью добиться отсутствия данных блокировок: [flame graph блокировок](https://htmlpreview.github.io/?https://github.com/IgorSamohin/2021-highload-dht/blob/igor-samokhin-content/graphs/stage5/putLockHttpClient.html)
* Однако это никак не повлияло на задержку - основную долю блокировок по-прежнему занимает блокирующая очередь и блокировка на HttpSession. 
```text
  Thread Stats   Avg      Stdev     Max   +/- Stdev
    Latency    14.06ms   68.76ms 698.37ms   96.58%
    Req/Sec     0.99k   173.13     2.89k    76.94%
  Latency Distribution (HdrHistogram - Recorded Latency)
 50.000%    1.50ms
 75.000%    2.12ms
 90.000%    4.57ms
 99.000%  391.42ms
 99.900%  657.92ms
 99.990%  678.91ms
 99.999%  693.76ms
100.000%  698.88ms
```
* Если блокировка на очереди вполне понятна, то блокировка на HttpSession вызывала некоторые вопросы. После небольшого исследования предположу, что она появляется из-за сильного потока запросов по одному соединению - мы не можем получать запросы и отправлять ответы одновременно из-за synchronized на `HttpSession.sendResponse` и `Session.process`
* Далее на графе аллокаций при put-запросах я заметил, что ~7% аллокаций - создание ответов с пустым телом (UtilResponses.emptyResponse). Кажется, что эти запросы можно закешировать и переиспользовать, но увы это привело к множеству ошибок, поэтому идею решено отложить до выяснения обстоятельств 