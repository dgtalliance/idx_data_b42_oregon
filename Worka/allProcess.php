<?php

require_once '../autoload.php';

$params = getopt('', ["mls:", "sysId:", "sourceId:", "status:"]);
$mls = $params['mls'] ?? null;
$sysId = $params['sysId'] ?? null;
$sourceId = $params['sourceId'] ?? null;
$status = $params['status'] ?? null;

$helper = new Helpers();
$dbConnection = new DbConnection();
$imageService = new ImageSyncService($helper, $dbConnection);

$images = $helper->getImageByProvider($dbConnection->WorkaConnection(), $sysId, $status);
$imageService->sync([$images, $mls, $sysId, $status]);