<?php

require_once '../autoload.php';

try {
  $helpers = new Helpers();
  $conection = new DbConnection();

  $initTime = date("H:i:s");

  $status = getopt('', ["status:"]);
  $status = (isset($status['status']) ? $status['status'] : NULL);

  $ActiveCon = $conection->WorkaConnection();
  IdxLogger::setLog("Starting process...Init time: $initTime", IdxLog::type_confirmation);

  $table = ($status == 'active') ? "Active_Property" : "Closed_Property";

  $helpers->saveGeocodeFromBackup($table, $ActiveCon);
  //Get all MapAddress from worka
}
catch (\Throwable $th) {
  makeReport($th, __FILE__);
}