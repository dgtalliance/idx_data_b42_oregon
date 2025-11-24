<?php

class IdxImageFactory
{
    const RESIZE = ['300', '600'];
    const DEFAULT_SOURCE = ['s3image', 's3image_reduced'];

    protected $s3image;
    protected $s3image_reduced;
    protected $path;

    public function __construct(string $class)
    {
        try {
            $this->s3image = new IdxS3Client(new IdxS3Credentials(
                $class::DO_SPACES_ACCESS_KEY,
                $class::DO_SPACES_SECRET_KEY,
                $class::BUCKET_NAME,
            ));
            $this->s3image_reduced = new IdxS3Client(new IdxS3Credentials(
                $class::DO_SPACES_ACCESS_KEY_REDUCED,
                $class::DO_SPACES_SECRET_KEY_REDUCED,
                $class::BUCKET_NAME_REDUCED,
            ));
            $this->path = $class::UPLOAD_DIR;
        } catch (\Throwable $th) {
            throw $th;
        }
    }

    public function downloadImages(array $media, string $mls): array
    {
        $id = 1;
        $imageByMls = [];
        foreach ($media as $url) {
            $filename = "{$mls}_{$id}.jpeg";
            $prefix = substr($mls, -2);

            $file = new IdxFile($this->path, $filename, $prefix);
            $file->create($url);

            if ($id == 1) {
                foreach (self::RESIZE as  $size) {
                    $file_reduced = new IdxFile($this->path, $filename, $prefix);
                    $file_reduced->resize($size);
                    // $imageByMls[] = $this->s3image_reduced->put($file_reduced);
                    $this->s3image_reduced->put($file_reduced);
                }
            }

            $imageByMls[] = $this->s3image->put($file);
            $id++;
        }

        return  $imageByMls;
    }

    public function cleanImages(array $mls_num): void
    {
        foreach ($mls_num as $mls) {
            $prefix = substr($mls, -2);

            foreach (self::DEFAULT_SOURCE as  $source) {
                $images = $this->{$source}->list("{$prefix}/{$mls}_");

                if (!is_array($images)) {
                    IdxLogger::setLog("{$source} no need clean for MLS:{$mls}", IdxLog::type_info);
                    continue;
                }

                $images = array_column($images, 'Key');
                array_walk($images, [__CLASS__, 'mixedKey'], 'Key');

                IdxLogger::setLog("Clean {$source} for MLS:{$mls}");

                $delet = $this->{$source}->delete($images);
                // IdxLogger::setLog($delet, IdxLog::type_error);
            }
        }
    }

    static function mixedKey(&$item, $key, $filter)
    {
        $item = [$filter => $item];
    }
}
