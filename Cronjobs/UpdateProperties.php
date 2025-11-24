<?php
require_once '../autoload.php';

$initTime = date("H:i:s");
$status = getopt('', ["status:"]);
$status = (isset($status['status']) ? $status['status'] : NULL);

$update = getopt('', ["update:"]);
$update = (isset($update['update']) ? $update['update'] : NULL);

$helpers = new Helpers($status);


$lastUpdate = null;
$helpers->setIndex(36, $status);

if ($update !== NULL) {
    $lastUpdate = $helpers->getLastUpdate();
}

$helpers->UpdateProperties($lastUpdate);


$helpers->logTimer($initTime);

