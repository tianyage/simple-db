<?php

use Tianyage\SimpleDb\Db;

require_once '../vendor/autoload.php';
$db    = Db::getInstance();
$qqrow = $db->find('qqs', "qq=454701103");
print_r($qqrow);