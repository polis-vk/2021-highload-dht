# Stage 6 Report

## Нагрузочное тестирование `PUT`-запросами

```bash
wrk -c 64 -t 16 -d5m -R 4500 -L -s scripts/put.lua http://localhost:8080
```

### CPU

```bash
 ./profiler.sh -e cpu -d 270 -f putCpuAsyncHttpClient.html 12785
```

### Alloc

```bash
 ./profiler.sh -e alloc -d 270 -f putAllocAsyncHttpClient.html 12785
```

### Lock

```bash
 ./profiler.sh -e lock -d 270 -f putLockAsyncHttpClient.html 12785
```

## Нагрузочное тестирование `GET`-запросами

```bash
wrk -c 64 -t 16 -d5m -R 4500 -L -s scripts/get.lua http://localhost:8080
```

### CPU

```bash
 ./profiler.sh -e cpu -d 270 -f getCpuAsyncHttpClient.html 12785
```

### Alloc

```bash
 ./profiler.sh -e alloc -d 270 -f getAllocAsyncHttpClient.html 12785
```

### Lock

```bash
 ./profiler.sh -e lock -d 270 -f getLockAsyncHttpClient.html 12785
```

## Результаты

### PUT

```text 
  Thread Stats   Avg      Stdev     Max   +/- Stdev
    Latency   441.00ms  827.03ms   3.39s    85.15%
    Req/Sec   292.00     77.59   666.00     76.28%
  Latency Distribution (HdrHistogram - Recorded Latency)
 50.000%    7.22ms
 75.000%  432.13ms
 90.000%    1.87s
 99.000%    3.17s
 99.900%    3.33s
 99.990%    3.36s
 99.999%    3.38s
100.000%    3.39s
```

### GET

```text
  Thread Stats   Avg      Stdev     Max   +/- Stdev
    Latency    61.56ms  190.98ms   1.09s    91.58%
    Req/Sec   292.46     69.26   727.00     81.58%
  Latency Distribution (HdrHistogram - Recorded Latency)
 50.000%    2.26ms
 75.000%    3.87ms
 90.000%  144.26ms
 99.000%  960.51ms
 99.900%    1.06s
 99.990%    1.08s
 99.999%    1.09s
100.000%    1.10s

```

## Профилировщик

|     | CPU | Alloc | Lock |
| --- | --- | ----- | ---- |
| GET | [ссылка](https://htmlpreview.github.io/?https://github.com/IgorSamohin/2021-highload-dht/blob/igor-samokhin-content/graphs/stage6/getCpuAsyncHttpClient.html) | [ссылка](https://htmlpreview.github.io/?https://github.com/IgorSamohin/2021-highload-dht/blob/igor-samokhin-content/graphs/stage6/getAllocAsyncHttpClient.html) | [ссылка](https://htmlpreview.github.io/?https://github.com/IgorSamohin/2021-highload-dht/blob/igor-samokhin-content/graphs/stage6/getLockAsyncHttpClient.html) |   
| PUT | [ссылка](https://htmlpreview.github.io/?https://github.com/IgorSamohin/2021-highload-dht/blob/igor-samokhin-content/graphs/stage6/putCpuAsyncHttpClient.html) | [ссылка](https://htmlpreview.github.io/?https://github.com/IgorSamohin/2021-highload-dht/blob/igor-samokhin-content/graphs/stage6/putAllocAsyncHttpClient.html) | [ссылка](https://htmlpreview.github.io/?https://github.com/IgorSamohin/2021-highload-dht/blob/igor-samokhin-content/graphs/stage6/putLockAsyncHttpClient.html) |

## Выводы

1. После внедрения HttpClient и CompletableFuture задержка сильно увеличилась, поэтому пришлось снизить количество запросов в секунду с 15_000 до 4_500. Но даже при этом задержка все равно очень большая.
2. На графе cpu появилось очень много всяких вещей, многие из них отвечают за внутреннюю работу "нововведений". Но среди них сразу в глаза бросился столбец ProxyClient.proxy, который находится внутри работы основного потока. Не удивительно - в коде я дожидаюсь пока из одного CompletableFuture не выполнится другой CompletableFuture, чтобы отправить пользователю ответ (т.е. стою на блокировке). Принято решение варварским способом спустить HttpSession пониже и отправить ответ без ожиданий. 

Теперь на [графе](https://htmlpreview.github.io/?https://github.com/IgorSamohin/2021-highload-dht/blob/igor-samokhin-content/graphs/stage6/getCpuAsyncHttpClient2.html) нет этого столбца. 
Количество запросов в секунду стало возможным поднять до 10_000. Задержка на put-запросах стала такой:
```text
  Thread Stats   Avg      Stdev     Max   +/- Stdev
    Latency    48.53ms   84.43ms 517.38ms   87.62%
    Req/Sec   648.09    138.93     1.31k    73.66%
  Latency Distribution (HdrHistogram - Recorded Latency)
 50.000%    8.80ms
 75.000%   47.20ms
 90.000%  164.99ms
 99.000%  377.09ms
 99.900%  446.46ms
 99.990%  488.19ms
 99.999%  511.74ms
100.000%  517.63ms
```
3. В графах блокировок также виднеются только блокировки на SelectorManager, но судя по всему это внутренний инструмент HttpClient'а и это его стандартное поведение. Хотя, нельзя отрицать факт, что я неправильно использую CompletableFuture.
4
