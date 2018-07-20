<?php

include('CuteDB.php');

$db = new CuteDB();

$db->open('image');

$imageFile = $_GET['imageFile'];

header('content-type:image/jpeg');

echo $db->get($imageFile);

$db->close();
