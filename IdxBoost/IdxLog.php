<?php

class IdxLog
{
    const type_error = 1;
    const type_confirmation = 2;
    const type_info = 3;
    const type_success = 4;

    protected $type;
    protected $msg;
    protected $color;

    public function __construct($msg, int $type)
    {
        $this->msg = json_encode($msg, JSON_FORCE_OBJECT);
        $this->type = $type;

        switch ($type) {
            case self::type_confirmation:
                $this->color = "\e[0;33m";
                break;
            case self::type_info:
                $this->color = "\e[1;34m";
                break;
            case self::type_error:
                $this->color = "\e[1;31m";
                break;
            case self::type_success:
                $this->color = "\e[0;37m";
                break;
            default:
                IdxLogger::setLog("Error type not allowed.", IdxLog::type_error);
                break;
        }
    }

    /**
     * Get the value of msg
     */
    public function getMsg()
    {
        return $this->msg;
    }

    /**
     * Get the value of type
     */
    public function getType()
    {
        return $this->type;
    }

    /**
     * Get the value of color
     */
    public function getColor()
    {
        return $this->color;
    }
}
