<h1>Stage 1</h1>

*Можно было бы подумать, что студенты Технополиса приобретают компуктеры на М1 ради допольнительных двух баллов, но даже два балла не стоили таких мучений...*

Нагрузочное тестирование и профилирование были произведены с помощью [lima-vm](https://github.com/lima-vm/lima).

Сначала с помощью wrk было произведено нагрузочное тестирование PUT-запросами на стабильной загрузке:

```
wrk -c 1 -t 1 -d 2m -R 2000 -s get.lua http://localhost:8080
Initialised 1 threads in 0 ms.
Running 2m test @ http://localhost:8080
  1 threads and 1 connections
  Thread calibration: mean lat.: 1.017ms, rate sampling interval: 10ms
  Thread Stats   Avg      Stdev     Max   +/- Stdev
    Latency     2.08ms   19.12ms 470.78ms   99.53%
    Req/Sec     2.15k   391.26    19.89k    98.67%
  240000 requests in 2.00m, 15.34MB read
Requests/sec:   1999.99
Transfer/sec:    130.86KB
```

Под нагрузкой было произведено 30-секундное профилирование CPU с помощью async-profiler:

```
./profiler.sh -d 30 -f putcpu.html 1914
```

![CPU (put)](putcpu.png)

После этого было проведено тестирование GET-запросами. Так же в одно соединение, один поток, в течение двух минут на стабильной нагрузке 2000 запросов в секунду:

```
wrk -c 1 -t 1 -d 2m -R 2000 -s get.lua http://localhost:8080
Initialised 1 threads in 0 ms.
Running 2m test @ http://localhost:8080
  1 threads and 1 connections
  Thread calibration: mean lat.: 0.978ms, rate sampling interval: 10ms
  Thread Stats   Avg      Stdev     Max   +/- Stdev
    Latency     0.92ms  443.39us   5.68ms   62.01%
    Req/Sec     2.15k   126.54     3.11k    64.34%
  239999 requests in 2.00m, 16.82MB read
Requests/sec:   1999.98
Transfer/sec:    143.54KB
```

Результаты 30-секундного профилирования CPU под нагрузкой GET-запросов:

![CPU (get)](getcpu.png)

Видно, что большую часть времени занимает вызов метода range (26.42%). Можно предположить, что это связано с тем, что при 2-минутном заполнении БД все записи были записаны в один файл (размер которого составил 4МБ), и вероятно, поиск ключа в таком довольно большом файле занимает много времени.

К сожалению, не удалось провести профилирование alloc. Даже после установки openjdk-11-dbg была получена следующая ошибка:
![error](error.png)