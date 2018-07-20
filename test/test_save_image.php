<?php

include('CuteDB.php');

$db = new CuteDB();

$db->open('image');

$imageFiles = [
    '1.jpg',
    '2.jpg',
    '3.jpg',
    '4.jpg',
    '5.jpg',
    '6.jpg',
    '7.jpg',
    '8.jpg',
    '9.jpg',
    '10.jpg',
    '11.jpg',
    '12.jpg',
    '13.jpg',
    '14.jpg',
    '15.jpg',
    '16.jpg',
];

foreach ($imageFiles as $imageFile) {
    $db->set($imageFile, file_get_contents($imageFile));
}

$db->close();
