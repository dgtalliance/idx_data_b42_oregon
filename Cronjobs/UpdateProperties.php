<?php
require_once '../autoload.php';

$initTime = date("H:i:s");
$status = getopt('', ["status:"]);
$status = (isset($status['status']) ? $status['status'] : 'active');

$update = getopt('', ["update:"]);
$update = (isset($update['update']) ? $update['update'] : NULL);

$helpers = new Helpers($status);


$lastUpdate = null;
$helpers->setIndex(42, $status);

if ($update !== NULL) {
    $lastUpdate = $helpers->getLastUpdate();
}

$helpers->UpdateProperties($lastUpdate);


$helpers->logTimer($initTime);

