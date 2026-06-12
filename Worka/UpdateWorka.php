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

    $dateForLog = date('Y-m-d H:i:s');
    $nextLink = NULL;
    $allProperties = [];
    $skip = 0;
    do {
        $data = $helpers->getAllActivePendingProperties($status, $skip, $lastUpdate);

        $helpers->processDataToInsertInWorka($data, $status);
        IdxLogger::setLog("Properties processed: " . count($data) . " - LastUpdate: $lastUpdate", IdxLog::type_confirmation);
        $skip += 100;
    } while (count($data['value']) == 100);

    $comand = "php EnquevePhotos.php  --status=$status2 > enquevePhotosBoard42.log 2>&1";
    shell_exec($comand);

    if ($status == 'Active') {
        $allProperties = [];
        $nextLink = null;
        do {
            $data = $helpers->getAllActivePendingPropertiestodelete($nextLink);

            if ($data === FALSE || !isset($data['value'])) {
                IdxLogger::setLog("API failed — aborting delete to avoid data loss", IdxLog::type_error);
                $allProperties = [];
                break;
            }

            foreach ($data['value'] as $value) {
                $allProperties[] = $value['ListingKey'];
            }

            $nextLink = $data['@odata.nextLink'] ?? null;
            IdxLogger::setLog("Properties fetched: " . count($data['value']) . " total=" . count($allProperties), IdxLog::type_confirmation);
        } while ($nextLink !== null);

        if (count($allProperties) > 20000) {
            $helpers->deleteNonComingProperties($allProperties);
        }
    }

} catch (\Throwable $th) {
    makeReport($th, __FILE__);
}