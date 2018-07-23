<?php

include('CuteDB.php');

$db = new CuteDB();

$db->open('test');

$times = 1000000;

$time = microtime(true);

for ($i = 0; $i < $times; $i++) {
	$db->set('name_'.$i, $i);
}

printf("set benchmark: %5f\n", microtime(true) - $time);

$time = microtime(true);

for ($i = 0; $i < $times; $i++) {
	$db->get('name_'.$i);
}

printf("get benchmark: %5f\n", microtime(true) - $time);

$time = microtime(true);

$db->moveHead();

while (true) {
    $next = $db->next();
    if (!$next) {
        break;
    }
}

$db->moveTail();

while (true) {
    $prev = $db->prev();
    if (!$prev) {
        break;
    }
}

printf("iterator benchmark: %5f\n", microtime(true) - $time);

$db->close();
