<?php

use GuzzleHttp\Client;

class WebHooks
{
    const DEFAULT_USER = "IdxBoard" . GlobalVariables::BOARD_ID . "Logger";
    const ENDPOINT = "T012FGYQD60/B04PFC72LAX/1vSz5JCVW2RgQ1XCpCN97qMV";

    protected $msg;

    protected $user;

    public function __construct($msg, $user = self::DEFAULT_USER)
    {
        $this->msg = $msg;
        $this->user = $user;
    }

    public function send()
    {
        $httpClient = new Client();
        $response = $httpClient->request('POST', "https://hooks.slack.com/services/" . self::ENDPOINT, [
            'body' => json_encode(['text' => $this->msg ?? 'empty_msg', 'username' => $this->user]),
            'headers' => [
                'Content-Type' => 'application/json'
            ]
        ]);
        return $response->getStatusCode();
    }
}
