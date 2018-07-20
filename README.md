# CuteDB
A single PHP file. Tiny DB implements in PHP using HashTable algorithm.

example:
========
```php
<?php

include('CuteDB.php');

$db = new CuteDB();

$db->open('test'); // Open DB

$db->set('test_key', 'test_value'); // Set key value map

echo $db->get('test_key'); // Get key's value

$db->close(); // Close DB

```
