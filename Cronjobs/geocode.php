<?php

require_once '../autoload.php';

try {
    $helpers = new Helpers();
    $conection = new DbConnection();

    $initTime = date("H:i:s");

    $status = getopt('', ["status:"]);
    $status = (isset($status['status']) ? $status['status'] : NULL);

    $board = getopt('', ["board:"]);
    $board = (isset($board['board']) ? $board['board'] : 36);

    $top = getopt('', ["top:"]);
    $top = (isset($top['top']) ? $top['top'] : 1000);

    $boardList = explode(",", $board);
    foreach ($boardList as $board) {
        $ActiveCon = $conection->WorkaConnection();
        IdxLogger::setLog("Starting process...Init time: $initTime", IdxLog::type_confirmation);

        $table = ($status == 'active') ? "Active_Property" : "Closed_Property";
        $alladdress = $helpers->getMapAddresses($table, $ActiveCon, $top);
        $total = count($alladdress);
        IdxLogger::setLog("Loading.... : $total for Process coordinates", IdxLog::type_confirmation);

        $helpers->loadDataPropertyGeocode2($alladdress, $table, $ActiveCon, $board);

        IdxLogger::setLog("Loading.... : " . count($alladdress) . " for Process coordinates board [{$board}] finished", IdxLog::type_confirmation);
        $helpers->logTimer($initTime);
    }
    $comand = "php UpdateProperties.php --status=$status --update=1 > properties.log 2>&1";
    shell_exec($comand);
} catch (\Throwable $th) {
    makeReport($th, __FILE__);
}