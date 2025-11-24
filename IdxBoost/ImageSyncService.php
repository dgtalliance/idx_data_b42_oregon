<?php

class ImageSyncService
{

    protected Helpers $helpers;
    protected PDO $connection;

    /**
     * @param Helpers $helpers
     * @param DbConnection $dbConnection
     */
    public function __construct(Helpers $helpers, DbConnection $dbConnection)
    {
        $this->helpers = $helpers;
        $this->connection = $dbConnection->WorkaConnection();
    }

    public function sync(array $params)
    {
        try {
            [$images, $mls, $sysId, $status] = $params;

            $table = ($status === 'Closed') ? 'Closed_Property' : 'Active_Property';

            if (is_array($images) && count($images) > 0) {
                $split = count($images);

                if ($table == 'Closed_Property') {
                    if (count($images) > 0) {
                        $split = 1;
                    }
                }

                for ($i = 0; $i < $split; $i++) {
                    $data = $images[$i] ?? null;
                    if (empty($data)) continue;

                    $imageId = $i + 1;
                    $fileName = "{$mls}_{$imageId}.jpeg";
                    $temp = "../uploads/{$fileName}";

                    file_put_contents($temp, file_get_contents($data));

                    $obj = [
                        'path_to_file' => $temp,
                        'filename' => $fileName,
                        'prefix' => substr($mls, -2)
                    ];

                    if ($imageId == 1) {
                        $this->helpers->imageResize($mls, $temp, $obj['prefix'], 600);
                        $this->helpers->imageResize($mls, $temp, $obj['prefix'], 300);
                    }

                    $this->helpers->uploadImage($obj['path_to_file'], $obj['filename'], $obj['prefix']);
                    unlink($obj['path_to_file']);
                }

                $this->helpers->updateMediaStatus("'" . $mls . "'", $status, 1);
            }
        } catch (Throwable $throwable) {
            $this->helpers->registerFailure([
                'mls_num' => $mls,
                'sysid' => $sysId,
                'message' => $throwable->getMessage(),
                'process' => 1,
                'boardId' => 36
            ]);
            $this->helpers->updateMediaStatus("'" . $mls . "'", $status, 3);

        }
    }

}