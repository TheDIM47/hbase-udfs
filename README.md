#### UDF for Phoenix/HBase/Hadoop

For more information see https://phoenix.apache.org/udf.html

#### en

DateToInv/InvToDate - Convert Date to Inverted date format Unsigned long and back

LongToMac/MacToLong - Convert MAC-address to Long and back

FlipByte - flip sign bit in byte for negative numbers 

Note: If you use inverted date format, you should also exchange start and stop dates. 

Example:
``` 
  Plain SQL: ...where st between DATETOINV(TO_TIMESTAMP('2015-09-11 00:00:00.000')) 
                             and DATETOINV(TO_TIMESTAMP('2015-09-10 00:00:00.000'))
```
```                               
  Phoenix SQL: ...where st between DATETOINV(TO_TIMESTAMP('2015-09-10 00:00:00.000')) 
                               and DATETOINV(TO_TIMESTAMP('2015-09-11 00:00:00.000'))
```

#### ru

DateToInv/InvToDate - преобразование Timestamp в Long (inverted date format) - используется в ключах

LongToMac/MacToLong - преобразование Строки MAC-адреса (6-и сегментного) в Long и обратно

FlipByte - обращение (инверсия) старшего бита в байте для корректного представления отрицательных чисел

Важное напоминание: Так как в ключах используется дата/время в инвертированном формате,
при поиске необходимо также инвертировать диапазоны. 

Например:
``` 
  Обычный SQL: ...where st between DATETOINV(TO_TIMESTAMP('2015-09-11 00:00:00.000')) 
                               and DATETOINV(TO_TIMESTAMP('2015-09-10 00:00:00.000'))
```
```                               
  Phoenix SQL: ...where st between DATETOINV(TO_TIMESTAMP('2015-09-10 00:00:00.000')) 
                               and DATETOINV(TO_TIMESTAMP('2015-09-11 00:00:00.000'))
```
