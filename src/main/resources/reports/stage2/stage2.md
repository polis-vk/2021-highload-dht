# Оптимизация PUT-запросов

------------------------------------------------------------

## Визуальное сравнение оптимизаций по отчетам wrk2

[put_comparison.html](put_results/put_comparison.html)

## Описание оптимизаций

* [wrk_put_1_seq.txt](put_results/wrk_put_1_seq.txt) - исходная последовательная реализация
* [wrk_put_2_no_poll_no_sync.txt](put_results/wrk_put_2_no_poll_no_sync.txt) - убран мьютекс, захватывающий все тело
  функции `upsert(..)`
* [wrk_put_3_poll41_no_flag.txt](put_results/wrk_put_3_poll41_no_flag.txt) - добавлен пулл из 1 потока и параллельная
  обработка входящий запросов 4 потоками
* [wrk_put_4_poll41_with_flag.txt](put_results/wrk_put_4_poll41_with_flag.txt) - добавлен флаг, позволяющий
  контролировать условие "среди потоков попавших в гонку за выполнение функции flush(..) только один займется записью"
* [wrk_put_5_poll41_with_snapshot.txt](put_results/wrk_put_5_poll41_with_snapshot.txt) - добавлено копирование объекта
  memoryStorage и передача копии в функцию `flush(
  ..)` , чтобы асинхронный `flush(..)` не соревновался с новыми запросами за объект memoryStorage
* [wrk_put_6_smart_poll44_switch_careful.txt](put_results/wrk_put_6_smart_poll44_switch_careful.txt) - добавлен подсчет
  памяти, выделяемой под задачи отправленные асинхронное выполнение в Thread Poll (считаем общий memoryConsumption
  находящийся в очереди к пуллу). Thread Poll из 1-ого потока заменен Thread Poll-ом на 4 потока
* [wrk_put_7_smart_poll44_switch_test_believer.txt](put_results/wrk_put_7_smart_poll44_switch_test_believer.txt) -
  удален спорный код, лишний раз разделяющий ресурс memoryStorage между потоками выполняющими `flush(..)` и потоками
  производящими записи в объект memoryStorage

## Профилирование cpu в финальной версии

[put_cpu.html](put_results/put_cpu.html)

## Профилирование alloc-ов в финальной версии

[put_alloc.html](put_results/put_alloc.html)

## Профилирование lock-ов в финальной версии

[put_lock.html](put_results/put_lock.html)

# Оптимизация GET-запросов

------------------------------------------------------------

## Визуальное сравнение оптимизаций по отчетам wrk2

[get_comparison.html](get_results/get_comparison.html)

## Описание оптимизаций

* [wrk_get_1_seq.txt](get_results/wrk_get_1_seq.txt) - исходная последовательная реализация
* [wrk_get_2_no_sync.txt](get_results/wrk_get_2_no_sync.txt) - убран мьютекс, захватывающий все тело функции `range(..)`

## Профилирование cpu в финальной версии

[get_cpu.html](get_results/get_cpu.html)

## Профилирование alloc-ов в финальной версии

[get_alloc.html](get_results/get_alloc.html)

## Профилирование lock-ов в финальной версии

[get_lock.html](get_results/get_lock.html)

