<?php

interface ClientInterface
{
    function request(string $method, $uri, array $options);

    function parseResquestFromJson(string $method, $uri, array $options);

    function parseResquestFromXml(string $method, $uri, array $options);
}
