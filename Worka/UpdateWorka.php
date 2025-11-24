<?php

require_once '../autoload.php';

try {
  $variables = new GlobalVariables();
  $helpers = new Helpers();

  $initTime = date("H:i:s");

  $update = getopt('', ["update:"]);
  $update = (isset($update['update']) ? $update['update'] : NULL);

  $status = getopt('', ["status:"]);
  $status2 = (isset($status['status']) ? $status['status'] : 'active');
  $status = Ucfirst($status2);

  $lastUpdate = NULL;
  IdxLogger::setLog('STARTING PROCESS TO UPDATE WORKA TABLE', IdxLog::type_confirmation);
  if ($update == 1) {
    $lastUpdate = $helpers->getLastUpdateByEndpoint("Active_Property");
  }
  else {
    $lastUpdate = '1970-01-01T00:00:00Z';
  }

  $dateForLog = date('Y-m-d H:i:s');
  $nextLink = NULL;
  $allProperties = [];
  $skip = 0;
  do {
    $data = $helpers->getAllActivePendingProperties($status,  $lastUpdate);
    $lastUpdate = isset($data['value'][999]['ModificationTimestamp']) ? $data['value'][999]['ModificationTimestamp'] : NULL;

    $helpers->processDataToInsertInWorka($data, $status);
    if ($lastUpdate == NULL) {
      break;
    }
    IdxLogger::setLog("Properties processed: " . count($data) . " - LastUpdate: $lastUpdate", IdxLog::type_confirmation);
  }
  while (count($data['value']) == 1000);

  if ($status == 'Active') {
    $lastUpdate = '1970-01-01T00:00:00Z';
    $allProperties = [];
    do {
      $data = $helpers->getAllActivePendingPropertiestoDelete($lastUpdate);
      $lastUpdate = isset($data['value'][999]['ModificationTimestamp']) ? $data['value'][999]['ModificationTimestamp'] : NULL;

      foreach ($data['value'] as $key => $value) {
        $allProperties[] = $value['ListingKey'];
      }
      IdxLogger::setLog("Properties processed: " . count($data) . " - LastUpdate: $lastUpdate", IdxLog::type_confirmation);
    }
    while (count($data['value']) == 1000);

    if (count($allProperties) > 50000) {
      $helpers->deleteNonComingProperties($allProperties);
    }
  }
  $comand = "php EnquevePhotos.php  --status=$status2 > enquevePhotosBoard36.log 2>&1";
  shell_exec($comand);

  $comand = "php ../Cronjobs/geocode.php --status=$status2 --top=3000 > geocode36.log 2>&1";
  shell_exec($comand);
}
catch (\Throwable $th) {
  makeReport($th, __FILE__);
}