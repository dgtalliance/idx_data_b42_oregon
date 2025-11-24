<?php

class Logger
{

    private $currentDate;

    public function __construct()
    {
        $this->currentDate = date('Y-m-d H:i:s');
    }

    public function errorLog(string $message)
    {
        echo "\e[1;31m DATE: {$this->currentDate} MESSAGE:$message \n";
    }

    public function confirmationLog(string $message)
    {
        echo "\e[0;33m DATE: {$this->currentDate} MESSAGE: $message \n";
    }

    public function successLog(string $message)
    {
        echo "\e[0;37m DATE: {$this->currentDate} MESSAGE: $message \n";
    }

    public function infoLog(string $message)
    {
        echo "\e[1;34m DATE: {$this->currentDate} MESSAGE: $message \n";
    }

}