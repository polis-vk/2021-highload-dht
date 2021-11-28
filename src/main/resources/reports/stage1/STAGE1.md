# 2021-highload-dht
Курсовой проект 2021 года [курса](https://polis.mail.ru/curriculum/program/discipline/1257/) "Проектирование высоконагруженных систем" в [Технополис](https://polis.mail.ru).

## Этап 1. HTTP + storage. Отчёт
### Нагрузочное тестирование с помощью wrk2
Произведём нагрузочное тестирование с помощью wrk2 
в течении десяти минут, используя одно активное соединение,
один поток и поддерживая поток запросов на уровне 1000 запросов/с.

#### PUT-запросами:
```
$ wrk2 -c1 -t1 -d10m -R1001 -L -s 2021-highload-dht/src/main/resources/wrk/put.lua http://localhost:8080
Running 10m test @ http://localhost:8080
  1 threads and 1 connections
  Thread calibration: mean lat.: 2.557ms, rate sampling interval: 10ms
  Thread Stats   Avg      Stdev     Max   +/- Stdev
    Latency     3.27ms   13.37ms 208.00ms   97.13%
    Req/Sec     1.06k   527.92    13.78k    96.62%
  Latency Distribution (HdrHistogram - Recorded Latency)
 50.000%    1.20ms
 75.000%    1.60ms
 90.000%    1.88ms
 99.000%   86.21ms
 99.900%  142.08ms
 99.990%  178.18ms
 99.999%  203.39ms
100.000%  208.13ms

  Detailed Percentile spectrum:
       Value   Percentile   TotalCount 1/(1-Percentile)

       0.081     0.000000            1         1.00
       0.607     0.100000        59074         1.11
       0.787     0.200000       118260         1.25
       0.931     0.300000       177411         1.43
       1.067     0.400000       236403         1.67
       1.202     0.500000       295617         2.00
       1.268     0.550000       324911         2.22
       1.338     0.600000       354489         2.50
       1.413     0.650000       384011         2.86
       1.493     0.700000       413473         3.33
       1.601     0.750000       442987         4.00
       1.649     0.775000       457986         4.44
       1.691     0.800000       472785         5.00
       1.731     0.825000       487286         5.71
       1.773     0.850000       502090         6.67
       1.820     0.875000       516776         8.00
       1.846     0.887500       524163         8.89
       1.876     0.900000       531748        10.00
       1.907     0.912500       539120        11.43
       1.941     0.925000       546474        13.33
       1.989     0.937500       553695        16.00
       2.049     0.943750       557421        17.78
       2.151     0.950000       561119        20.00
       2.287     0.956250       564746        22.86
       2.493     0.962500       568438        26.67
       7.895     0.968750       572133        32.00
      18.591     0.971875       573974        35.56
      30.223     0.975000       575820        40.00
      41.951     0.978125       577669        45.71
      53.599     0.981250       579514        53.33
      65.375     0.984375       581359        64.00
      71.231     0.985938       582286        71.11
      76.991     0.987500       583212        80.00
      82.751     0.989062       584127        91.43
      88.447     0.990625       585051       106.67
      94.271     0.992188       585971       128.00
      97.407     0.992969       586439       142.22
     100.671     0.993750       586898       160.00
     104.191     0.994531       587360       182.86
     108.159     0.995313       587817       213.33
     113.087     0.996094       588284       256.00
     116.031     0.996484       588513       284.44
     118.911     0.996875       588740       320.00
     122.175     0.997266       588973       365.71
     125.503     0.997656       589201       426.67
     129.471     0.998047       589431       512.00
     131.583     0.998242       589548       568.89
     134.015     0.998437       589665       640.00
     136.575     0.998633       589781       731.43
     139.263     0.998828       589893       853.33
     142.591     0.999023       590008      1024.00
     144.511     0.999121       590069      1137.78
     146.559     0.999219       590123      1280.00
     148.991     0.999316       590182      1462.86
     151.807     0.999414       590238      1706.67
     155.135     0.999512       590297      2048.00
     156.799     0.999561       590325      2275.56
     158.975     0.999609       590355      2560.00
     161.407     0.999658       590384      2925.71
     163.839     0.999707       590411      3413.33
     166.783     0.999756       590441      4096.00
     168.575     0.999780       590457      4551.11
     170.239     0.999805       590470      5120.00
     171.903     0.999829       590484      5851.43
     173.951     0.999854       590498      6826.67
     175.871     0.999878       590512      8192.00
     177.279     0.999890       590520      9102.22
     178.559     0.999902       590527     10240.00
     179.583     0.999915       590534     11702.86
     181.503     0.999927       590541     13653.33
     183.295     0.999939       590548     16384.00
     184.319     0.999945       590552     18204.44
     186.111     0.999951       590556     20480.00
     187.391     0.999957       590559     23405.71
     190.591     0.999963       590563     27306.67
     193.279     0.999969       590566     32768.00
     195.071     0.999973       590568     36408.89
     196.735     0.999976       590570     40960.00
     198.527     0.999979       590572     46811.43
     200.063     0.999982       590574     54613.33
     200.959     0.999985       590575     65536.00
     201.727     0.999986       590576     72817.78
     202.495     0.999988       590577     81920.00
     203.391     0.999989       590578     93622.86
     204.287     0.999991       590579    109226.67
     205.055     0.999992       590580    131072.00
     205.055     0.999993       590580    145635.56
     205.823     0.999994       590581    163840.00
     205.823     0.999995       590581    187245.71
     206.719     0.999995       590582    218453.33
     206.719     0.999996       590582    262144.00
     206.719     0.999997       590582    291271.11
     207.487     0.999997       590583    327680.00
     207.487     0.999997       590583    374491.43
     207.487     0.999998       590583    436906.67
     207.487     0.999998       590583    524288.00
     207.487     0.999998       590583    582542.22
     208.127     0.999998       590584    655360.00
     208.127     1.000000       590584          inf
#[Mean    =        3.274, StdDeviation   =       13.367]
#[Max     =      208.000, Total count    =       590584]
#[Buckets =           27, SubBuckets     =         2048]
----------------------------------------------------------
  600600 requests in 10.00m, 38.38MB read
Requests/sec:   1001.00
Transfer/sec:     65.50KB
```

По результатам нагрузочного тестирования
видно, что 75% приходящих запросов обрабатываются 
в промежуток до 1.6мс, 90% — до 1.88мс,
99% — до 86.21мс, 99,999% — до 203.39мс,
максимальное время обрабоки запроса до 208.13 мс.

Такой большой разрыв во времени обработки запроса между
90% и 99% процентами может быть обосновам в т. ч.,
записью на диск, которую выполняет наша реализация
NoSQL хранилища при превышении заданного порога хранения 
даннных.

#### GET запросами:
```
$ wrk2 -c1 -t1 -d10m -R1000 -L -s 2021-highload-dht/src/main/resources/wrk/get.lua http://localhost:8080
Running 10m test @ http://localhost:8080
  1 threads and 1 connections
  Thread calibration: mean lat.: 2.488ms, rate sampling interval: 10ms
  Thread Stats   Avg      Stdev     Max   +/- Stdev
    Latency     1.48ms    2.11ms 103.94ms   99.25%
    Req/Sec     1.05k   121.17     3.33k    71.80%
  Latency Distribution (HdrHistogram - Recorded Latency)
 50.000%    1.38ms
 75.000%    1.76ms
 90.000%    2.10ms
 99.000%    2.94ms
 99.900%   22.86ms
 99.990%   91.14ms
 99.999%  103.23ms
100.000%  104.00ms

  Detailed Percentile spectrum:
       Value   Percentile   TotalCount 1/(1-Percentile)

       0.281     0.000000            1         1.00
       0.638     0.100000        59084         1.11
       0.865     0.200000       118238         1.25
       1.075     0.300000       177085         1.43
       1.234     0.400000       236055         1.67
       1.385     0.500000       295291         2.00
       1.457     0.550000       324521         2.22
       1.530     0.600000       354284         2.50
       1.603     0.650000       383683         2.86
       1.675     0.700000       413083         3.33
       1.757     0.750000       442715         4.00
       1.803     0.775000       457445         4.44
       1.855     0.800000       472023         5.00
       1.913     0.825000       486806         5.71
       1.974     0.850000       501581         6.67
       2.036     0.875000       516322         8.00
       2.069     0.887500       523961         8.89
       2.101     0.900000       531095        10.00
       2.137     0.912500       538550        11.43
       2.173     0.925000       545877        13.33
       2.213     0.937500       553430        16.00
       2.235     0.943750       557037        17.78
       2.261     0.950000       560537        20.00
       2.291     0.956250       564254        22.86
       2.323     0.962500       567896        26.67
       2.361     0.968750       571553        32.00
       2.383     0.971875       573476        35.56
       2.407     0.975000       575302        40.00
       2.439     0.978125       577179        45.71
       2.489     0.981250       578943        53.33
       2.581     0.984375       580775        64.00
       2.651     0.985938       581713        71.11
       2.731     0.987500       582617        80.00
       2.845     0.989062       583538        91.43
       3.045     0.990625       584456       106.67
       3.447     0.992188       585377       128.00
       3.775     0.992969       585840       142.22
       4.223     0.993750       586302       160.00
       4.867     0.994531       586761       182.86
       5.919     0.995313       587221       213.33
       7.787     0.996094       587682       256.00
       8.935     0.996484       587914       284.44
      10.127     0.996875       588143       320.00
      11.623     0.997266       588373       365.71
      13.287     0.997656       588604       426.67
      15.079     0.998047       588834       512.00
      16.383     0.998242       588950       568.89
      17.679     0.998437       589065       640.00
      19.071     0.998633       589180       731.43
      20.943     0.998828       589296       853.33
      23.247     0.999023       589410      1024.00
      24.767     0.999121       589468      1137.78
      26.895     0.999219       589526      1280.00
      37.119     0.999316       589583      1462.86
      52.287     0.999414       589641      1706.67
      58.975     0.999512       589698      2048.00
      64.607     0.999561       589727      2275.56
      69.695     0.999609       589756      2560.00
      71.295     0.999658       589786      2925.71
      73.471     0.999707       589816      3413.33
      74.751     0.999756       589842      4096.00
      75.647     0.999780       589858      4551.11
      76.287     0.999805       589871      5120.00
      77.503     0.999829       589886      5851.43
      79.359     0.999854       589900      6826.67
      86.079     0.999878       589916      8192.00
      88.575     0.999890       589922      9102.22
      92.223     0.999902       589929     10240.00
      95.743     0.999915       589936     11702.86
      97.663     0.999927       589943     13653.33
      98.303     0.999939       589950     16384.00
      98.815     0.999945       589954     18204.44
      99.391     0.999951       589958     20480.00
      99.775     0.999957       589961     23405.71
     100.223     0.999963       589965     27306.67
     100.415     0.999969       589968     32768.00
     100.543     0.999973       589971     36408.89
     100.735     0.999976       589972     40960.00
     100.927     0.999979       589974     46811.43
     101.439     0.999982       589976     54613.33
     101.887     0.999985       589977     65536.00
     102.335     0.999986       589978     72817.78
     102.783     0.999988       589979     81920.00
     103.231     0.999989       589980     93622.86
     103.423     0.999991       589981    109226.67
     103.551     0.999992       589982    131072.00
     103.551     0.999993       589982    145635.56
     103.679     0.999994       589983    163840.00
     103.679     0.999995       589983    187245.71
     103.807     0.999995       589984    218453.33
     103.807     0.999996       589984    262144.00
     103.807     0.999997       589984    291271.11
     103.935     0.999997       589985    327680.00
     103.935     0.999997       589985    374491.43
     103.935     0.999998       589985    436906.67
     103.935     0.999998       589985    524288.00
     103.935     0.999998       589985    582542.22
     103.999     0.999998       589986    655360.00
     103.999     1.000000       589986          inf
#[Mean    =        1.478, StdDeviation   =        2.107]
#[Max     =      103.936, Total count    =       589986]
#[Buckets =           27, SubBuckets     =         2048]
----------------------------------------------------------
  599991 requests in 10.00m, 612.72MB read
Requests/sec:    999.98
Transfer/sec:      1.02MB
```

По результатам нагрузочного тестирования
видно, что 75% приходящих запросов обрабатываются
в промежуток до 1.76мс, 90% — до 2.1мс,
99% — до 2.94мс, 99.999% — до 103.23мс,
максимальное время обрабоки запроса до 104мс.

### Профилирование с помощью async-profiler

Произведём профилирование использования процессорного времени и 
запросов выделения памяти.

#### При обработке PUT запросов: 

[put_cpu.html](put_cpu.html) </br>
[put_mem.html](put_mem.html)

По результатам профилирования CPU видно, что большую
часть процессорного времени (45.85%) занимает метод 
`HttpSession.writeResponse`, включающий запись в сокет.
Чтение из сокета занимате 9.03% процессорного времени.
В нашей реализации метода `put` большую часть времени занимает метод 
`LsmDao.flush()` 16.25% (большую часть времени которого используется
для системных вызовов записи на диск), 
запись в `ConcurrentSkipListMap` занимает 3.25%.

По результата профилирования alloc видно, что большая часть
аллокаций происходит при обработке HTTP запросов
(библиотекой one-nio).
В нашей реализации сервиса большая часть аллокаций происходит
при создании
копий `HeapByteBufferR` — 16.34%.


#### При обработке GET запросов:

[get_cpu.html](get_cpu.html) </br>
[get_mem.html](put_mem.html)

По результатам профилирования CPU видно, что большую
часть процессорного времени (45.04%) занимает преобразование 
порядка бит при чтении числа типа `int` из буффера, методом `getInt`,
(больше, чем метод `mismatch` (14,72%)!).
Запись в сокет занимает 12.59% процессорного времени.
уходит на чтение из сокета.
Затем процессорное время используют методы слияния и фильтрации
итераторов — 4,06%.

По результата профилирования alloc видно, что большая часть
аллокаций происходит при слиянии и фильтрации итераторов — 49.99%.
при обработке HTTP запросов
(преобразование параметров запроса и ответа).
Остальная часть выделений уходит на обработку параметров и созданий
копий буфферов.

По результатам анализа можно предложить несколько путей оптимизации:
* Не блокировать выполнение запроса, при необходимости записи таблицы
в память
* Исследовать возможные пути оптимизации обработки запросов
библиотекой one-nio, чтобы избежать излишних конвертаций
* Исследовать реализацию метода convEndian 
(исследовать его оптимальность, предложить более оптимальный метод, 
если возможно) рассмотреть возможность
реализации метода `offset` учитывая обратный порядок бит. Возможно
записывать адреса в обратном порядке, чтобы избежать конвертаций.
* Не создавать новые объекты для пустых итераторов, чтобы уменьшить
выделение объектов и процессорное время (при слиянии и фильтрации).