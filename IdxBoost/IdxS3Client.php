<?php

use Aws\S3\S3Client;

class IdxS3Client
{
    protected $client;
    protected $credentials;

    public function __construct(IdxS3Credentials $credentials)
    {
        $this->client = new S3Client([
            'version' => 'latest',
            'region' => 'nyc3',
            'endpoint' => 'https://nyc3.digitaloceanspaces.com',
            'credentials' => [
                'key' => $credentials->getKey(),
                'secret' => $credentials->getSecret(),
            ]
        ]);

        $this->credentials = $credentials;
    }

    public function put(IdxFile $file)
    {
        $key = [];
        $key[] = $file->getPrefix();
        $key[] = $file->getFilename();
        $key = implode('/', $key);

        $this->client->putObject([
            'ContentLength' => (int)filesize($file->getPathToFile()),
            'ContentType' => 'image/jpeg',
            'CacheControl' => 'max-age=31536000',
            'Bucket' => $this->credentials->getBucket(),
            'Key' => $key,
            'Body' => file_get_contents($file->getPathToFile()),
            'ACL' => 'public-read'
        ]);

        $file->destroy();
        return $file->getFilename();
    }

    public function list($path)
    {
        $listObjects = [
            'Bucket' => $this->credentials->getBucket(),
            'Prefix' => "{$path}"
        ];
        $listObjects = $this->client->listObjects($listObjects);
        return $listObjects->get('Contents');
    }

    public function delete($objects)
    {
        $deleteObjects = $this->client->deleteObjects([
            'Bucket' => $this->credentials->getBucket(),
            'Delete' => [
                'Objects' => $objects,
                'Quiet' => false,
            ],
        ]);

        return $deleteObjects->get('Deleted');
    }
}
