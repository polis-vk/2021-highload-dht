./profiler.sh -d 120 -e lock -f hw2lock_get.html 712  
wrk -c 64 -t 3 -d 1m -R 10000 -L -s get.lua http://localhost:8080


wrk -c 64 -t 3 -d 4m -R 10000 -L -s put.lua http://localhost:8080
Running 4m test @ http://localhost:8080
3 threads and 64 connections
Thread calibration: mean lat.: 1.537ms, rate sampling interval: 10ms
Thread calibration: mean lat.: 1.463ms, rate sampling interval: 10ms
Thread calibration: mean lat.: 1.503ms, rate sampling interval: 10ms
Thread Stats   Avg      Stdev     Max   +/- Stdev
Latency     2.42ms   25.17ms 964.61ms   99.69%
Req/Sec     3.51k   616.11    35.80k    93.73%
Latency Distribution (HdrHistogram - Recorded Latency)
50.000%    1.16ms
75.000%    1.61ms
90.000%    2.03ms
99.000%    4.53ms
99.900%  516.61ms
99.990%  871.93ms
99.999%  956.93ms
100.000%  965.12ms

Detailed Percentile spectrum:
Value   Percentile   TotalCount 1/(1-Percentile)

       0.037     0.000000            1         1.00
       0.499     0.100000       230969         1.11
       0.678     0.200000       460013         1.25
       0.842     0.300000       691022         1.43
       1.000     0.400000       920593         1.67
       1.161     0.500000      1149641         2.00
       1.245     0.550000      1264532         2.22
       1.331     0.600000      1379546         2.50
       1.421     0.650000      1494986         2.86
       1.514     0.700000      1609696         3.33
       1.614     0.750000      1724779         4.00
       1.667     0.775000      1782374         4.44
       1.723     0.800000      1839985         5.00
       1.784     0.825000      1897277         5.71
       1.852     0.850000      1954530         6.67
       1.931     0.875000      2011721         8.00
       1.977     0.887500      2040425         8.89
       2.029     0.900000      2069359        10.00
       2.089     0.912500      2098318        11.43
       2.161     0.925000      2127131        13.33
       2.249     0.937500      2155716        16.00
       2.303     0.943750      2169991        17.78
       2.365     0.950000      2184026        20.00
       2.443     0.956250      2198695        22.86
       2.537     0.962500      2212834        26.67
       2.667     0.968750      2227157        32.00
       2.755     0.971875      2234430        35.56
       2.863     0.975000      2241541        40.00
       3.007     0.978125      2248697        45.71
       3.201     0.981250      2255848        53.33
       3.481     0.984375      2263052        64.00
       3.673     0.985938      2266629        71.11
       3.919     0.987500      2270213        80.00
       4.251     0.989062      2273812        91.43
       4.747     0.990625      2277403       106.67
       5.615     0.992188      2280978       128.00
       6.295     0.992969      2282770       142.22
       7.323     0.993750      2284566       160.00
       8.815     0.994531      2286364       182.86
      11.031     0.995313      2288158       213.33
      14.727     0.996094      2289951       256.00
      18.255     0.996484      2290850       284.44
      26.703     0.996875      2291746       320.00
      48.511     0.997266      2292645       365.71
     119.743     0.997656      2293543       426.67
     235.775     0.998047      2294440       512.00
     292.863     0.998242      2294893       568.89
     351.231     0.998437      2295338       640.00
     407.807     0.998633      2295787       731.43
     465.663     0.998828      2296236       853.33
     523.519     0.999023      2296686      1024.00
     552.447     0.999121      2296910      1137.78
     581.119     0.999219      2297136      1280.00
     610.303     0.999316      2297361      1462.86
     638.975     0.999414      2297587      1706.67
     667.647     0.999512      2297808      2048.00
     681.983     0.999561      2297923      2275.56
     696.831     0.999609      2298032      2560.00
     711.167     0.999658      2298148      2925.71
     729.087     0.999707      2298261      3413.33
     751.103     0.999756      2298369      4096.00
     762.367     0.999780      2298429      4551.11
     782.847     0.999805      2298483      5120.00
     806.399     0.999829      2298541      5851.43
     828.927     0.999854      2298594      6826.67
     850.431     0.999878      2298650      8192.00
     861.695     0.999890      2298678      9102.22
     872.959     0.999902      2298706     10240.00
     885.247     0.999915      2298736     11702.86
     897.023     0.999927      2298762     13653.33
     908.799     0.999939      2298791     16384.00
     914.943     0.999945      2298806     18204.44
     920.063     0.999951      2298818     20480.00
     926.207     0.999957      2298833     23405.71
     931.327     0.999963      2298846     27306.67
     936.959     0.999969      2298860     32768.00
     940.543     0.999973      2298869     36408.89
     942.591     0.999976      2298874     40960.00
     946.175     0.999979      2298882     46811.43
     948.735     0.999982      2298889     54613.33
     952.319     0.999985      2298896     65536.00
     953.343     0.999986      2298901     72817.78
     954.367     0.999988      2298904     81920.00
     955.903     0.999989      2298906     93622.86
     957.951     0.999991      2298910    109226.67
     958.975     0.999992      2298916    131072.00
     958.975     0.999993      2298916    145635.56
     958.975     0.999994      2298916    163840.00
     960.511     0.999995      2298918    187245.71
     961.023     0.999995      2298920    218453.33
     963.071     0.999996      2298922    262144.00
     963.583     0.999997      2298923    291271.11
     963.583     0.999997      2298923    327680.00
     964.095     0.999997      2298925    374491.43
     964.095     0.999998      2298925    436906.67
     964.607     0.999998      2298926    524288.00
     965.119     0.999998      2298930    582542.22
     965.119     1.000000      2298930          inf
#[Mean    =        2.425, StdDeviation   =       25.168]
#[Max     =      964.608, Total count    =      2298930]
#[Buckets =           27, SubBuckets     =         2048]
----------------------------------------------------------
2399484 requests in 4.00m, 153.32MB read
Requests/sec:   9998.07
Transfer/sec:    654.17KB


----

#GET

/wrk2# wrk -c 64 -t 3 -d 4m -R 10000 -L -s get.lua http://localhost:8080
Running 4m test @ http://localhost:8080
3 threads and 64 connections
Thread calibration: mean lat.: 2.234ms, rate sampling interval: 10ms
Thread calibration: mean lat.: 2.296ms, rate sampling interval: 10ms
Thread calibration: mean lat.: 2.410ms, rate sampling interval: 10ms
Thread Stats   Avg      Stdev     Max   +/- Stdev
Latency     1.27ms  674.91us  23.84ms   73.41%
Req/Sec     3.51k   317.41     7.33k    75.27%
Latency Distribution (HdrHistogram - Recorded Latency)
50.000%    1.19ms
75.000%    1.65ms
90.000%    2.04ms
99.000%    3.26ms
99.900%    6.07ms
99.990%   11.32ms
99.999%   17.53ms
100.000%   23.85ms

Detailed Percentile spectrum:
Value   Percentile   TotalCount 1/(1-Percentile)

       0.054     0.000000            1         1.00
       0.521     0.100000       230080         1.11
       0.700     0.200000       460958         1.25
       0.861     0.300000       689937         1.43
       1.022     0.400000       920213         1.67
       1.187     0.500000      1149869         2.00
       1.273     0.550000      1264756         2.22
       1.362     0.600000      1379653         2.50
       1.455     0.650000      1495463         2.86
       1.550     0.700000      1609346         3.33
       1.651     0.750000      1724360         4.00
       1.704     0.775000      1782376         4.44
       1.759     0.800000      1839842         5.00
       1.818     0.825000      1897276         5.71
       1.882     0.850000      1954313         6.67
       1.954     0.875000      2011774         8.00
       1.995     0.887500      2040682         8.89
       2.040     0.900000      2069308        10.00
       2.091     0.912500      2098348        11.43
       2.149     0.925000      2126778        13.33
       2.221     0.937500      2155919        16.00
       2.261     0.943750      2169785        17.78
       2.309     0.950000      2184442        20.00
       2.363     0.956250      2198392        22.86
       2.431     0.962500      2212907        26.67
       2.517     0.968750      2227315        32.00
       2.569     0.971875      2234370        35.56
       2.631     0.975000      2241630        40.00
       2.705     0.978125      2248702        45.71
       2.801     0.981250      2255939        53.33
       2.921     0.984375      2263078        64.00
       2.995     0.985938      2266624        71.11
       3.081     0.987500      2270286        80.00
       3.181     0.989062      2273833        91.43
       3.311     0.990625      2277415       106.67
       3.475     0.992188      2280998       128.00
       3.577     0.992969      2282792       142.22
       3.695     0.993750      2284612       160.00
       3.829     0.994531      2286399       182.86
       3.989     0.995313      2288189       213.33
       4.195     0.996094      2290002       256.00
       4.315     0.996484      2290893       284.44
       4.447     0.996875      2291779       320.00
       4.607     0.997266      2292685       365.71
       4.803     0.997656      2293575       426.67
       5.035     0.998047      2294470       512.00
       5.183     0.998242      2294920       568.89
       5.351     0.998437      2295365       640.00
       5.535     0.998633      2295811       731.43
       5.807     0.998828      2296265       853.33
       6.099     0.999023      2296711      1024.00
       6.291     0.999121      2296935      1137.78
       6.515     0.999219      2297158      1280.00
       6.747     0.999316      2297383      1462.86
       7.043     0.999414      2297606      1706.67
       7.363     0.999512      2297832      2048.00
       7.559     0.999561      2297943      2275.56
       7.807     0.999609      2298056      2560.00
       8.115     0.999658      2298170      2925.71
       8.439     0.999707      2298283      3413.33
       8.919     0.999756      2298392      4096.00
       9.191     0.999780      2298448      4551.11
       9.543     0.999805      2298506      5120.00
       9.855     0.999829      2298561      5851.43
      10.311     0.999854      2298617      6826.67
      10.863     0.999878      2298673      8192.00
      11.119     0.999890      2298701      9102.22
      11.367     0.999902      2298729     10240.00
      11.807     0.999915      2298757     11702.86
      12.287     0.999927      2298785     13653.33
      12.791     0.999939      2298814     16384.00
      13.007     0.999945      2298827     18204.44
      13.295     0.999951      2298842     20480.00
      13.615     0.999957      2298855     23405.71
      14.135     0.999963      2298869     27306.67
      14.399     0.999969      2298884     32768.00
      14.551     0.999973      2298890     36408.89
      14.903     0.999976      2298897     40960.00
      15.359     0.999979      2298904     46811.43
      15.919     0.999982      2298911     54613.33
      16.623     0.999985      2298919     65536.00
      16.815     0.999986      2298923     72817.78
      17.119     0.999988      2298925     81920.00
      17.487     0.999989      2298929     93622.86
      17.567     0.999991      2298932    109226.67
      17.775     0.999992      2298936    131072.00
      18.047     0.999993      2298938    145635.56
      18.223     0.999994      2298939    163840.00
      18.511     0.999995      2298941    187245.71
      18.639     0.999995      2298943    218453.33
      18.783     0.999996      2298945    262144.00
      18.975     0.999997      2298946    291271.11
      18.975     0.999997      2298946    327680.00
      19.567     0.999997      2298947    374491.43
      19.663     0.999998      2298948    436906.67
      20.127     0.999998      2298949    524288.00
      20.223     0.999998      2298950    582542.22
      20.223     0.999998      2298950    655360.00
      20.223     0.999999      2298950    748982.86
      20.879     0.999999      2298951    873813.33
      20.879     0.999999      2298951   1048576.00
      22.895     0.999999      2298952   1165084.44
      22.895     0.999999      2298952   1310720.00
      22.895     0.999999      2298952   1497965.71
      22.895     0.999999      2298952   1747626.67
      22.895     1.000000      2298952   2097152.00
      23.855     1.000000      2298953   2330168.89
      23.855     1.000000      2298953          inf
#[Mean    =        1.267, StdDeviation   =        0.675]
#[Max     =       23.840, Total count    =      2298953]
#[Buckets =           27, SubBuckets     =         2048]
----------------------------------------------------------
2399522 requests in 4.00m, 168.99MB read
Non-2xx or 3xx responses: 6
Requests/sec:   9997.97
Transfer/sec:    721.03KB