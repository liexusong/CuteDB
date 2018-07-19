# CuteDB
Tiny DB implements in PHP using HashTable algorithm

example:
========
```php
<?php

include('CuteDB.php');

$db = new CuteDB();

$db->open('test');
$db->set('test_key', 'test_value');
echo $db->get('test_key');

```
