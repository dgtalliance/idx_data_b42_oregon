<?php

require_once 'vendor/autoload.php';

class Autoloader
{
    const SERVICE_PATH = [
        '../Helpers',
        '../IdxBoost',
    ];

    public static function register()
    {
        spl_autoload_register(function ($class) {
            foreach (Autoloader::SERVICE_PATH as $path) {
                $file = str_replace('\\', DIRECTORY_SEPARATOR, "{$path}/{$class}") . '.php';
                if (file_exists($file)) {
                    require_once $file;
                    return true;
                }
            }
            return false;
        });
    }
}

Autoloader::register();

function pr($data, $die = true)
{
    print_r($data);
    print_r("\n");

    if ($die) {
        die();
    }
}

function dd($msg, $die = true)
{
    print_r($msg);
    print_r("\n");

    if ($die) {
        die();
    }
}

function makeReport($th, $file = __FILE__)
{
    $process_dir = explode('/', $file);
    $fileError = explode('/', $th->getFile());

    $error = [
        'MSG' => $th->getMessage(),
        'Process' => array_pop($process_dir),
        'Cron' => array_pop($process_dir),
        'File' => array_pop($fileError),
        'Line' => $th->getLine(),
    ];

    IdxLogger::setLog($error, IdxLog::type_error);
}
