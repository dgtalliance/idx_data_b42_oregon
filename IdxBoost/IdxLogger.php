<?php

class IdxLogger
{
    const path = "../logs/";

    public static function setLog($msg, $type = IdxLog::type_success)
    {
        $log = new IdxLog($msg, $type);
        $currentDate = date('Y-m-d H:i:s');
        echo "{$log->getColor()} DATE: {$currentDate} MESSAGE: {$log->getMsg()} \n";

        if ($log->getType() === IdxLog::type_error) {
            file_put_contents(IdxLogger::getFileLogs(), "[{$currentDate}]= MESSAGE:{$log->getMsg()} \n", FILE_APPEND);

            $slack = new WebHooks($log->getMsg());
            $slack->send();
        }
    }

    private static function createDir()
    {
        /**
         *  Create directory/folder logs.
         */
        if (!file_exists(self::path)) {
            mkdir(self::path, 0777, true);
        }
    }

    private static function getFileLogs(): string
    {
        IdxLogger::createDir();

        $path = self::path;
        $date = date('Y-M');

        return "{$path}log_{$date}.log";
    }
}
