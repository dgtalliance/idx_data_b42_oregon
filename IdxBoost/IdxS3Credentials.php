<?php

class IdxS3Credentials
{
    protected $key;
    protected $secret;
    protected $bucket;

    public function __construct($key, $secret, $bucket)
    {
        $this->key = $key;
        $this->secret = $secret;
        $this->bucket = $bucket;
    }


    /**
     * Get the value of key
     */
    public function getKey()
    {
        return $this->key;
    }

    /**
     * Get the value of secret
     */
    public function getSecret()
    {
        return $this->secret;
    }

    /**
     * Get the value of bucket
     */
    public function getBucket()
    {
        return $this->bucket;
    }
}
