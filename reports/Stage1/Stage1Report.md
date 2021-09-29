# Stage 1 Report

## Tips

* Нагрузка давалась в одно подключение и одно соединение с R = 1000 запросов в секунду

* Профилировка и при `GET` и при `PUT` запросах производлась на протяжении 1 минуты

## Нагрузочное тестирование `PUT`-запросами

```bash
wrk -c 1 -t 1 -d3m -R 1000 -L -s scripts/put.lua -L http://localhost:8080
```

#### Результаты

```text
1 threads and 1 connections
  Thread calibration: mean lat.: 1.720ms, rate sampling interval: 10ms
  Thread Stats   Avg      Stdev     Max   +/- Stdev
    Latency     1.34ms  621.57us  32.69ms   71.98%
    Req/Sec     1.05k    87.15     2.78k    84.57%
  Latency Distribution (HdrHistogram - Recorded Latency)
 50.000%    1.33ms
 75.000%    1.69ms
 90.000%    2.07ms
 99.000%    2.40ms
 99.900%    2.60ms
 99.990%   20.25ms
 99.999%   31.34ms
100.000%   32.70ms
  Detailed Percentile spectrum:
       Value   Percentile   TotalCount 1/(1-Percentile)

       0.179     0.000000            1         1.00
       0.603     0.100000        17006         1.11
       0.847     0.200000        34072         1.25
       1.042     0.300000        51024         1.43
       1.192     0.400000        68091         1.67
       1.330     0.500000        85027         2.00
       1.396     0.550000        93518         2.22
       1.465     0.600000       102081         2.50
       1.534     0.650000       110567         2.86
       1.608     0.700000       119041         3.33
       1.688     0.750000       127549         4.00
       1.735     0.775000       131818         4.44
       1.791     0.800000       136030         5.00
       1.857     0.825000       140308         5.71
       1.925     0.850000       144551         6.67
       1.997     0.875000       148791         8.00
       2.033     0.887500       150899         8.89
       2.069     0.900000       153057        10.00
       2.105     0.912500       155149        11.43
       2.143     0.925000       157280        13.33
       2.183     0.937500       159430        16.00
       2.203     0.943750       160518        17.78
       2.223     0.950000       161492        20.00
       2.245     0.956250       162591        22.86
       2.269     0.962500       163650        26.67
       2.295     0.968750       164720        32.00
       2.311     0.971875       165273        35.56
       2.323     0.975000       165751        40.00
       2.339     0.978125       166277        45.71
       2.355     0.981250       166822        53.33
       2.371     0.984375       167352        64.00
       2.381     0.985938       167646        71.11
       2.391     0.987500       167912        80.00
       2.401     0.989062       168199        91.43
       2.411     0.990625       168419       106.67
       2.423     0.992188       168666       128.00
       2.431     0.992969       168798       142.22
       2.439     0.993750       168942       160.00
       2.447     0.994531       169077       182.86
       2.457     0.995313       169217       213.33
       2.467     0.996094       169342       256.00
       2.473     0.996484       169396       284.44
       2.483     0.996875       169460       320.00
       2.493     0.997266       169530       365.71
       2.507     0.997656       169604       426.67
       2.521     0.998047       169662       512.00
       2.533     0.998242       169702       568.89
       2.541     0.998437       169728       640.00
       2.551     0.998633       169760       731.43
       2.579     0.998828       169791       853.33
       2.603     0.999023       169824      1024.00
       2.641     0.999121       169841      1137.78
       2.701     0.999219       169858      1280.00
       3.071     0.999316       169874      1462.86
       4.111     0.999414       169891      1706.67
       5.619     0.999512       169907      2048.00
       6.727     0.999561       169916      2275.56
       8.335     0.999609       169924      2560.00
       9.791     0.999658       169932      2925.71
      11.239     0.999707       169941      3413.33
      12.631     0.999756       169949      4096.00
      13.343     0.999780       169953      4551.11
      14.087     0.999805       169957      5120.00
      14.815     0.999829       169961      5851.43
      16.215     0.999854       169966      6826.67
      18.015     0.999878       169970      8192.00
      19.487     0.999890       169972      9102.22
      20.991     0.999902       169974     10240.00
      22.495     0.999915       169976     11702.86
      23.983     0.999927       169978     13653.33
      25.487     0.999939       169980     16384.00
      26.223     0.999945       169981     18204.44
      26.975     0.999951       169982     20480.00
      27.727     0.999957       169983     23405.71
      28.479     0.999963       169984     27306.67
      29.215     0.999969       169985     32768.00
      29.919     0.999973       169986     36408.89
      29.919     0.999976       169986     40960.00
      30.639     0.999979       169987     46811.43
      30.639     0.999982       169987     54613.33
      31.343     0.999985       169988     65536.00
      31.343     0.999986       169988     72817.78
      31.343     0.999988       169988     81920.00
      32.015     0.999989       169989     93622.86
      32.015     0.999991       169989    109226.67
      32.015     0.999992       169989    131072.00
      32.015     0.999993       169989    145635.56
      32.015     0.999994       169989    163840.00
      32.703     0.999995       169990    187245.71
      32.703     1.000000       169990          inf
#[Mean    =        1.336, StdDeviation   =        0.622]
#[Max     =       32.688, Total count    =       169990]
#[Buckets =           27, SubBuckets     =         2048]
----------------------------------------------------------
  179994 requests in 3.00m, 11.50MB read
Requests/sec:   1000.00
Transfer/sec:     65.43KB
```


### Cpu

```bash
./profiler.sh -e cpu -d 60 -f putCpu.html 175
```

#### Результаты

![](/reports/Stage1/putCpu.html)

### Alloc

```bash
./profiler.sh -e alloc -d 60 -f putAlloc.html 175
```

#### Результаты

![](/reports/Stage1/putAlloc.html)

## Нагрузочное тестирование `GET`-запросами

```bash
wrk -c 1 -t 1 -d3m -R 1000 -L -s scripts/get.lua -L http://localhost:8080
```

#### Результаты

```text
  1 threads and 1 connections
  Thread calibration: mean lat.: 1.458ms, rate sampling interval: 10ms
  Thread Stats   Avg      Stdev     Max   +/- Stdev
    Latency     1.32ms  522.73us   9.13ms   65.05%
    Req/Sec     1.05k    91.88     1.60k    81.31%
  Latency Distribution (HdrHistogram - Recorded Latency)
 50.000%    1.31ms
 75.000%    1.68ms
 90.000%    2.05ms
 99.000%    2.41ms
 99.900%    2.56ms
 99.990%    3.78ms
 99.999%    7.76ms
100.000%    9.14ms

  Detailed Percentile spectrum:
       Value   Percentile   TotalCount 1/(1-Percentile)

       0.145     0.000000            2         1.00
       0.592     0.100000        17006         1.11
       0.845     0.200000        34039         1.25
       1.030     0.300000        51036         1.43
       1.175     0.400000        68100         1.67
       1.313     0.500000        85052         2.00
       1.383     0.550000        93552         2.22
       1.452     0.600000       102098         2.50
       1.525     0.650000       110595         2.86
       1.599     0.700000       119042         3.33
       1.678     0.750000       127584         4.00
       1.721     0.775000       131769         4.44
       1.774     0.800000       136054         5.00
       1.835     0.825000       140298         5.71
       1.901     0.850000       144523         6.67
       1.973     0.875000       148789         8.00
       2.010     0.887500       150915         8.89
       2.049     0.900000       153028        10.00
       2.089     0.912500       155161        11.43
       2.129     0.925000       157259        13.33
       2.171     0.937500       159433        16.00
       2.193     0.943750       160537        17.78
       2.215     0.950000       161523        20.00
       2.239     0.956250       162600        22.86
       2.265     0.962500       163685        26.67
       2.289     0.968750       164682        32.00
       2.305     0.971875       165262        35.56
       2.321     0.975000       165812        40.00
       2.335     0.978125       166294        45.71
       2.353     0.981250       166829        53.33
       2.371     0.984375       167363        64.00
       2.381     0.985938       167630        71.11
       2.391     0.987500       167886        80.00
       2.403     0.989062       168170        91.43
       2.413     0.990625       168411       106.67
       2.425     0.992188       168681       128.00
       2.431     0.992969       168818       142.22
       2.437     0.993750       168937       160.00
       2.445     0.994531       169059       182.86
       2.453     0.995313       169194       213.33
       2.463     0.996094       169343       256.00
       2.469     0.996484       169399       284.44
       2.475     0.996875       169468       320.00
       2.483     0.997266       169531       365.71
       2.493     0.997656       169593       426.67
       2.505     0.998047       169660       512.00
       2.513     0.998242       169691       568.89
       2.519     0.998437       169724       640.00
       2.527     0.998633       169757       731.43
       2.537     0.998828       169789       853.33
       2.557     0.999023       169824      1024.00
       2.565     0.999121       169841      1137.78
       2.575     0.999219       169859      1280.00
       2.583     0.999316       169873      1462.86
       2.599     0.999414       169889      1706.67
       2.615     0.999512       169905      2048.00
       2.631     0.999561       169914      2275.56
       2.643     0.999609       169922      2560.00
       2.659     0.999658       169930      2925.71
       2.695     0.999707       169940      3413.33
       2.731     0.999756       169947      4096.00
       2.801     0.999780       169951      4551.11
       2.853     0.999805       169955      5120.00
       3.103     0.999829       169959      5851.43
       3.393     0.999854       169964      6826.67
       3.555     0.999878       169968      8192.00
       3.611     0.999890       169970      9102.22
       3.881     0.999902       169972     10240.00
       4.081     0.999915       169974     11702.86
       4.223     0.999927       169976     13653.33
       4.523     0.999939       169978     16384.00
       4.911     0.999945       169979     18204.44
       5.003     0.999951       169980     20480.00
       5.335     0.999957       169981     23405.71
       5.703     0.999963       169982     27306.67
       5.727     0.999969       169983     32768.00
       6.411     0.999973       169984     36408.89
       6.411     0.999976       169984     40960.00
       7.043     0.999979       169985     46811.43
       7.043     0.999982       169985     54613.33
       7.759     0.999985       169986     65536.00
       7.759     0.999986       169986     72817.78
       7.759     0.999988       169986     81920.00
       8.439     0.999989       169987     93622.86
       8.439     0.999991       169987    109226.67
       8.439     0.999992       169987    131072.00
       8.439     0.999993       169987    145635.56
       8.439     0.999994       169987    163840.00
       9.135     0.999995       169988    187245.71
       9.135     1.000000       169988          inf
#[Mean    =        1.319, StdDeviation   =        0.523]
#[Max     =        9.128, Total count    =       169988]
#[Buckets =           27, SubBuckets     =         2048]
----------------------------------------------------------
  179993 requests in 3.00m, 12.59MB read
Requests/sec:   1000.00
Transfer/sec:     71.61KB

```

### Cpu

```bash
./profiler.sh -e cpu -d 60 -f getCpu.html 175
```

#### Результаты

![](/reports/Stage1/getCpu.html)

### Alloc

```bash
./profiler.sh -e alloc -d 60 -f getAlloc.html 175
```

#### Результаты

![](/reports/Stage1/getAlloc.html)