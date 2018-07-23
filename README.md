# CuteDB
A single PHP file. Tiny DB implements in PHP using HashTable algorithm.

Example:
========
```php
<?php

include('CuteDB.php');

$db = new CuteDB();

$db->open('test'); // Open DB

$db->set('test_key', 'test_value'); // Set key value map

echo $db->get('test_key'); // Get key's value

$db->moveHead();

while (true) {
    $next = $db->next();
    echo $next[0].":".$next[1];
}

$db->close(); // Close DB

```

Principle
=========
![avatar](https://raw.githubusercontent.com/liexusong/CuteDB/master/CuteDB.jpg)
