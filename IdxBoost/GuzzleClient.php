<?php

use GuzzleHttp\Client;
use GuzzleHttp\Exception\GuzzleException;

class GuzzleClient implements ClientInterface
{
    protected Client $client;
    protected int $iterationNumber;
    protected int $maxIterationNumber;

    public function __construct(array $config = [], $maxIterationNumber = 5)
    {
        $this->client = new Client($config);
        $this->iterationNumber = 0;
        $this->maxIterationNumber = $maxIterationNumber;
    }

    private function canIterate(): bool
    {
        sleep(5);
        $this->iterationNumber++;
        return ($this->iterationNumber <= $this->maxIterationNumber);
    }

    /**
     * @throws Exception
     * @throws GuzzleException
     * @throws Throwable
     */
    public function request(string $method, $uri = '', array $options = [])
    {
        try {
            $response = $this->client->request($method, $uri, $options);
        } catch (Throwable $th) {
            if ($this->canIterate()) {
                return $this->request($method, $uri, $options);
            }

            throw $th;
        }

        if ($response->getStatusCode() != 200) {
            throw new Exception("Unauthorized");
        }

        return $response;
    }

    /**
     * @throws GuzzleException
     * @throws Throwable
     */
    public function parseResquestFromJson(string $method, $uri = '', array $options = [])
    {
        $response = $this->request($method, $uri, $options);

        if (!preg_match('/json/i', $this->getHeader($response))) {
            throw new \Exception("The response is not JSON type");
        }

        $content = $response->getBody()->getContents();
        $data = json_decode($content, true);

        if (empty($data)) {
            throw new Exception("Request data is empty");
        }

        return $data;
    }

    /**
     * @throws Throwable
     * @throws GuzzleException
     */
    public function parseResquestFromXml(string $method, $uri = '', array $options = [])
    {
        $response = $this->request($method, $uri, $options);

        if (!preg_match('/xml/i', $this->getHeader($response))) {
            throw new Exception("The response is not XML type");
        }

        return $response;;
    }

    private function getHeader($response)
    {
        return $response->getHeader('Content-Type')[0] ?? null;
    }
}
