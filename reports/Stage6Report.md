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

---

После лекции еще раз провел профилировку с параметром -t в профайлере. (Жирным выделены изменения между замерами по отношению к предыдущему варианту). В wrk стрелял только put-запросами, с R=4500. Профилировал только cpu 

2. thenAsync + exceptionally + newFixedThreadPool(16) в ProxyClients [cpu-graph](https://htmlpreview.github.io/?https://github.com/IgorSamohin/2021-highload-dht/blob/igor-samokhin-content/graphs/stage6/test.html)
```text
 50.000%    1.96ms
 75.000%    2.58ms
 90.000%    3.78ms
 99.000%    8.06ms
 99.900%   14.47ms
 99.990%   19.31ms
 99.999%   20.25ms
100.000%   20.25ms
```

2. **whenComplete** + newFixedThreadPool(16) в ProxyClients [cpu-graph](https://htmlpreview.github.io/?https://github.com/IgorSamohin/2021-highload-dht/blob/igor-samokhin-content/graphs/stage6/test2.html)
```text
 50.000%    2.12ms
 75.000%    2.95ms
 90.000%    5.05ms
 99.000%  108.61ms
 99.900%  199.42ms
 99.990%  241.41ms
 99.999%  263.68ms
100.000%  273.41ms
```

Где-то здесь я заметил очень много потоков селекторов. Есть ощущение, что одного потока должно хватить (раньше было по количеству доступных процессоров).

3. whenComplete + newFixedThreadPool(16) в ProxyClients + **selectors = 1** [cpu-graph](https://htmlpreview.github.io/?https://github.com/IgorSamohin/2021-highload-dht/blob/igor-samokhin-content/graphs/stage6/test3.html)
```text
 50.000%    2.29ms
 75.000%    3.25ms
 90.000%    5.12ms
 99.000%   32.19ms
 99.900%  190.46ms
 99.990%  252.80ms
 99.999%  274.94ms
100.000%  296.96ms
```
Сильно ничего не поменялось, но кажется, что сильный скачок сдвинулся на девятку

4. whenComplete + **newFixedThreadPool(4)** в ProxyClients (есть ощущение, что 16 потоков не нужно, поскольку они не блокируются. Вообще, наверное должно хватить количества == кворуму) + selectors = 1
```text
 50.000%    1.94ms
 75.000%    2.51ms
 90.000%    3.70ms
 99.000%   49.79ms
 99.900%  197.50ms
 99.990%  236.41ms
 99.999%  262.65ms
100.000%  280.58ms
```

5. whenComplete + **ForkJoinPool(4)** в ProxyClients  + selectors = 1 [cpu-graph](https://htmlpreview.github.io/?https://github.com/IgorSamohin/2021-highload-dht/blob/igor-samokhin-content/graphs/stage6/test4.html)

```text
 50.000%    1.87ms
 75.000%    2.40ms
 90.000%    3.28ms
 99.000%   12.73ms
 99.900%   62.97ms
 99.990%   88.83ms
 99.999%   99.07ms
100.000%  114.05ms
```

Неплохо, но в рамках "погрешности". Проверю, что будет, если в сервере тоже ForkJoinPool использовать

6. whenComplete + ForkJoinPool(4) в ProxyClients  + selectors = 1 + **ForkJoinPool(16) в ServiceImpl** 

```text
 50.000%    2.13ms
 75.000%    3.32ms
 90.000%    6.46ms
 99.000%   49.31ms
 99.900%  146.56ms
 99.990%  188.93ms
 99.999%  223.74ms
100.000%  247.68ms
```

7.  whenComplete + ForkJoinPool(4) в ProxyClients  + selectors = 1 + ForkJoinPool(**4**) в ServiceImpl
```text
 50.000%    1.73ms
 75.000%    2.17ms
 90.000%    2.78ms
 99.000%   10.15ms
 99.900%   40.10ms
 99.990%   82.94ms
 99.999%   98.94ms
100.000%  112.96ms
```

Получается ForkJoinPool на 4 потока в ServiceImpl практически не отличается от ThreadPoolExecutor на 16 потоков (пункт 5). Попробую уменьшить количество потоков до 4

8. whenComplete + ForkJoinPool(4) в ProxyClients  + selectors = 1 + **ThreadPoolExecutor(4)** в ServiceImpl
```text
 50.000%    1.71ms
 75.000%    2.13ms
 90.000%    2.62ms
 99.000%    5.08ms
 99.900%   35.01ms
 99.990%  100.42ms
 99.999%  124.35ms
100.000%  141.70ms
```

Все в рамках небольшого отклонения. Но все же в ForkJoinPool(4) нет сильных скачков, поэтому предположу, что в перспективе он будет лучше из-за work stealing'a, поэтому оставлю его

9. **thenAsync + exceptionally** + ForkJoinPool(4) в ProxyClients  + selectors = 1 + ForkJoinPool(4) в ServiceImpl

```text
 50.000%    1.73ms
 75.000%    2.16ms
 90.000%    2.71ms
 99.000%   11.31ms
 99.900%  126.33ms
 99.990%  166.01ms
 99.999%  197.76ms
100.000%  220.80ms
```
Контрольный выстрел в thenAsync показал, что он хуже whenComplete, поэтому оставляю второй вариант.


* Во всех новых графах видно наличие ForkJoinPool.commonPool'а. Это может говорить о том, что в какие-то future я не передаю executor и там по дефолту используется этот пул, но меня это вроде бы не сильно должно расстраивать.

