<?php

class IdxFile
{
    const FIX_HEIGHT = 1;
    const FIX_WIDTH = 2;

    protected $path_to_file;
    protected $filename;
    protected $prefix;

    public function __construct(string $path_to_file, string $filename, string $prefix)
    {
        $this->path_to_file = $path_to_file;
        $this->filename = $filename;
        $this->prefix = $prefix;
    }

    /**
     * Get the value of path_to_file
     */
    public function getPathToFile()
    {
        return $this->path_to_file . "/{$this->filename}";
    }

    /**
     * Get the value of filename
     */
    public function getFilename()
    {
        return $this->filename;
    }

    /**
     * Get the value of prefix
     */
    public function getPrefix()
    {
        return $this->prefix;
    }

    public function create($url)
    {
        try {
            $img = file_get_contents($url);
            file_put_contents($this->getPathToFile(), $img);
        } catch (\Throwable $th) {
            throw new Exception("Error while downloading.");
        }
    }

    public function destroy()
    {
        unlink($this->getPathToFile());
    }

    public function resize(int $width, int $height = null)
    {
        $file_origin = $this->getPathToFile();

        $prefix = explode('_', $this->filename);
        $prefix = $prefix[0];
        $filename_reduced = "{$prefix}_x{$width}.jpeg";

        $this->filename = $filename_reduced;

        $height = $height ?? $width;
        $this->redimensionarJPEG($file_origin, $width, $height, self::FIX_WIDTH);
    }

    private function redimensionarJPEG($file_origin, int $ancho_max, int $alto_max, $fix = null)
    {
        $info_imagen = getimagesize($file_origin);
        $ancho = $info_imagen[0];
        $alto = $info_imagen[1];

        if ($ancho >= $alto) {
            $nuevo_alto = round($alto * $ancho_max / $ancho, 0);
            $nuevo_ancho = $ancho_max;
        } else {
            $nuevo_ancho = round($ancho * $alto_max / $alto, 0);
            $nuevo_alto = $alto_max;
        }

        switch ($fix) {
            case IdxFile::FIX_WIDTH:
                $nuevo_alto = round($alto * $ancho_max / $ancho, 0);
                $nuevo_ancho = $ancho_max;
                break;
            case IdxFile::FIX_HEIGHT:
                $nuevo_ancho = round($ancho * $alto_max / $alto, 0);
                $nuevo_alto = $alto_max;
                break;
            default:
                $nuevo_ancho = $nuevo_ancho;
                $nuevo_alto = $nuevo_alto;
                break;
        }

        $imagen_nueva = imagecreatetruecolor($nuevo_ancho, $nuevo_alto);
        $imagen_vieja = imagecreatefromjpeg($file_origin);
        imagecopyresampled($imagen_nueva, $imagen_vieja, 0, 0, 0, 0, $nuevo_ancho, $nuevo_alto, $ancho, $alto);
        imagejpeg($imagen_nueva, $this->getPathToFile());
        imagedestroy($imagen_nueva);
        imagedestroy($imagen_vieja);
    }
}
