<?php

include('CuteDB.php');

$db = new CuteDB();
$db->open('test');

$time = microtime(true);

for ($i = 0; $i < 100000; $i++) {
	$db->set('name_'.$i, $i);
}

printf("set benchmark: %5f\n", microtime(true) - $time);

$time = microtime(true);

for ($i = 0; $i < 100000; $i++) {
	$db->get('name_'.$i);
}

printf("get benchmark: %5f\n", microtime(true) - $time);

$db->close();
