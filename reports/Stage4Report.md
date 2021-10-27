# Stage 4 Report

* После перехода на кластер сервер перестал держать значимую нагрузку, появились большие таймауты. Максимальное значение запросов, которое мог принимать кластер = 2500 при 64 потоках. Этого значения не хватало для того, что бы запустить процедуру flush, не говорю даже и о compaction. 
* Другими словами, сервер вообще не выполняет поставленных на него задач. Из-за этого было принято решение продолжать стрелять в 50к запросов и не выдерживать это количество. Это позволит сравнить показания с предыдущей работой и точно увидеть, где кластер тупит много времени

## Нагрузочное тестирование `PUT`-запросами

```bash
wrk -c 64 -t 16 -d5m -R 50000 -L -s scripts/put.lua http://localhost:8080
```

### CPU

```bash
 ./profiler.sh -e cpu -d 270 -f putCpuCluster.html 3498
```

### Alloc

```bash
 ./profiler.sh -e alloc -d 270 -f putAllocCluster.html 3498
```

### Lock

```bash
 ./profiler.sh -e lock -d 270 -f putLockConcurrentServer.html 3498
```

## Нагрузочное тестирование `GET`-запросами

```bash
wrk -c 64 -t 16 -d5m -R 50000 -L -s scripts/get.lua http://localhost:8080
```

### CPU

```bash
 ./profiler.sh -e cpu -d 270 -f getCpuCluster.html 3498
```

### Alloc

```bash
 ./profiler.sh -e alloc -d 270 -f getAllocCluster.html 3498
```

### Lock

```bash
 ./profiler.sh -e lock -d 270 -f getLockCluster.html 3498
```

## Результаты

### PUT

```text 
  Thread Stats   Avg      Stdev     Max   +/- Stdev
    Latency    31.06ms  103.92ms   1.66s    97.11%
    Req/Sec    61.74    224.11     3.08k    92.43%
  Latency Distribution (HdrHistogram - Recorded Latency)
 50.000%   10.90ms
 75.000%   26.83ms
 90.000%   53.22ms
 99.000%  416.00ms
 99.900%    1.44s
 99.990%    1.63s
 99.999%    1.65s
100.000%    1.66s

#[Mean    =       31.064, StdDeviation   =      103.921]
#[Max     =     1655.808, Total count    =       915411]
#[Buckets =           27, SubBuckets     =         2048]
----------------------------------------------------------
  967985 requests in 5.03m, 75.60MB read
  Socket errors: connect 0, read 0, write 0, timeout 8939
  Non-2xx or 3xx responses: 7
Requests/sec:   3207.26
Transfer/sec:    256.50KB
```

### GET

```text
  Thread Stats   Avg      Stdev     Max   +/- Stdev
    Latency    19.99ms   72.79ms   1.22s    96.80%
    Req/Sec   168.22    337.92     3.68k    79.98%
  Latency Distribution (HdrHistogram - Recorded Latency)
 50.000%    5.67ms
 75.000%   14.96ms
 90.000%   26.85ms
 99.000%  424.96ms
 99.900%  921.09ms
 99.990%    1.18s
 99.999%    1.21s
100.000%    1.22s

#[Mean    =       19.994, StdDeviation   =       72.788]
#[Max     =     1219.584, Total count    =       905768]
#[Buckets =           27, SubBuckets     =         2048]
----------------------------------------------------------
  939597 requests in 5.01m, 167.85MB read
  Socket errors: connect 0, read 0, write 0, timeout 8951
  Non-2xx or 3xx responses: 24
Requests/sec:   3125.00
Transfer/sec:    571.66KB


```

## Профилировщик

|     | CPU | Alloc | Lock |
| --- | --- | ----- | ---- |
| GET | [ссылка](https://htmlpreview.github.io/?https://github.com/IgorSamohin/2021-highload-dht/blob/igor-samokhin-content/graphs/stage4/getCpuCluster.html) | [ссылка](https://htmlpreview.github.io/?https://github.com/IgorSamohin/2021-highload-dht/blob/igor-samokhin-content/graphs/stage4/getAllocCluster.html) | [ссылка](https://htmlpreview.github.io/?https://github.com/IgorSamohin/2021-highload-dht/blob/igor-samokhin-content/graphs/stage4/getLockCluster.html) |   
| PUT | [ссылка](https://htmlpreview.github.io/?https://github.com/IgorSamohin/2021-highload-dht/blob/igor-samokhin-content/graphs/stage4/putCpuCluster.html) | [ссылка](https://htmlpreview.github.io/?https://github.com/IgorSamohin/2021-highload-dht/blob/igor-samokhin-content/graphs/stage4/putAllocCluster.html) | [ссылка](https://htmlpreview.github.io/?https://github.com/IgorSamohin/2021-highload-dht/blob/igor-samokhin-content/graphs/stage4/putLockCluster.html) |

## Выводы

* В данный момент если нода понимает, что запрос предназначен не ей, то она перенаправляет его на другую ноду, ожидая от нее ответа. После получения ответа она отправляет ответ обратно юзеру. Это сильно отразилось на времени ответа и на появлении таймаутов, поскольку теперь 3 ноды очень часто ждут ответов друг от друга, а не выполняют действия на своих машинах. Хотелось бы, чтобы нода как-то связывала другую ноду с пользователем, не участвуя в дальнейшем в их обмене информацией. Пойду читать матчасть, чтобы хоть примерно понять, как это сделать 

* Для того чтобы перенаправить запрос с ноды на ноду необходимо сделать новое соединение и загрузить данные в селектор, что довольно дорого. Это можно наблюдать в новых "горбах" в flame-графах cpu при get запросах (HttpClient.invoke). 
* Первое возможное решение - редиректить пользователя на другую ноду, но это раскроет топологию кластера и сделает его уязвимым, а так же сгенерирует еще один запрос на кластер (то есть общее кол-во запросов не уменьшается)
* Второе решение - сменить протокол на rpc. При первом взгляде кажется, что там есть инструменты, способные решить проблему

* Еще одну проблему HttpClient.invoke() видно в alloc графах - почти половину всех выделений памяти начал занимать этот метод, который создает ResponseReader для ожидания ответа от другой ноды.
* Все это так же можно увидеть и при get запросах, что говорит о неэффективности выбранных методов общения нод друг с другом.
