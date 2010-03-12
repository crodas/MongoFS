<?php
/*
  +---------------------------------------------------------------------------------+
  | Copyright (c) 2010 ActiveMongo                                                  |
  +---------------------------------------------------------------------------------+
  | Redistribution and use in source and binary forms, with or without              |
  | modification, are permitted provided that the following conditions are met:     |
  | 1. Redistributions of source code must retain the above copyright               |
  |    notice, this list of conditions and the following disclaimer.                |
  |                                                                                 |
  | 2. Redistributions in binary form must reproduce the above copyright            |
  |    notice, this list of conditions and the following disclaimer in the          |
  |    documentation and/or other materials provided with the distribution.         |
  |                                                                                 |
  | 3. All advertising materials mentioning features or use of this software        |
  |    must display the following acknowledgement:                                  |
  |    This product includes software developed by César D. Rodas.                  |
  |                                                                                 |
  | 4. Neither the name of the César D. Rodas nor the                               |
  |    names of its contributors may be used to endorse or promote products         |
  |    derived from this software without specific prior written permission.        |
  |                                                                                 |
  | THIS SOFTWARE IS PROVIDED BY CÉSAR D. RODAS ''AS IS'' AND ANY                   |
  | EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED       |
  | WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE          |
  | DISCLAIMED. IN NO EVENT SHALL CÉSAR D. RODAS BE LIABLE FOR ANY                  |
  | DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES      |
  | (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;    |
  | LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND     |
  | ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT      |
  | (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS   |
  | SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE                     |
  +---------------------------------------------------------------------------------+
  | Authors: César Rodas <crodas@php.net>                                           |
  +---------------------------------------------------------------------------------+
*/

require "MongoFS.php";

/* Ejemplo */
MongoFS::connect("mongofs", "localhost");


try {
    $localfile  = generate_test_file();
    $remotefile = "gridfs://testing.bin";
    $tmpfile    = tempnam("/tmp/", "mongofs");

    print "Uploading file to MongoDB\n\t";
    do_stream_copy($localfile, $remotefile);
    print "OK\n";

    print "Downloading file\n\t";
    do_stream_copy($remotefile, $tmpfile);
    print "OK\n";

    print "Comparing local and remote files\n\t";
    echo stream_cmp($tmpfile, $remotefile) ? "OK\n" : "FAILED\n";

    print "Random reading\n\t";
    echo partial_reading($localfile, $remotefile) ? "OK\n" : "FAILED\n";

    print "Random writing\n\t";
    echo partial_writing($localfile, $remotefile) ? "OK\n" : "FAILED\n";

} catch (Exception $e) {
    echo "FAILED\n";
    echo "\t".$e->getMessage()."\n";
}

/* delete files  */
unlink($remotefile);
unlink($tmpfile);
unlink($localfile);

function partial_reading($file1, $file2)
{
    $fi = fopen($file1, "r");
    $fp = fopen($file2, "r");

    $max = filesize($file1);

    for ($i=0; $i < 3000; $i++) {
        /* random offset */
        $offset = rand(0, $max);
        fseek($fp, $offset, SEEK_SET);
        fseek($fi, $offset, SEEK_SET);
       
        /* random data */
        $bytes = rand(1, 1024);
        $data1 = fread($fp, $bytes);
        $data2 = fread($fi, $bytes);
        if ($data1 !== $data2) {
            return false;
        }
    }
    fclose($fp);
    fclose($fi);

    return true;
}

function partial_writing($file1, $file2)
{
    $fi = fopen($file1, "r+");
    $fp = fopen($file2, "r+");

    $max = filesize($file1);

    for ($i=0; $i < 5000; $i++) {
        /* random offset */
        $offset = rand(0, $max+100); 
        fseek($fp, $offset, SEEK_SET);
        fseek($fi, $offset, SEEK_SET);
       
        /* random data */
        $data = sha1(microtime(),false);

        fwrite($fi, $data);
        fwrite($fp, $data);
    }

    fclose($fp);
    fclose($fi);

    echo "Comparing remote and local files: ";

    return stream_cmp($file1, $file2);
}

function stream_cmp($file1, $file2, $exhaustive=false)
{
    $f1 = fopen($file1, "r");
    $f2 = fopen($file2, "r");

    $bytes = $exhaustive ? 1 : 8096;

    while (!feof($f2) && !feof($f1)) {
        $data2 = fread($f2, $bytes);
        $data1 = fread($f1, $bytes);
        if ($data1 != $data2) {
            throw new Exception("File mismatch at ".ftell($f1).", ".ftell($f2));
        }
    }

    return feof($f2) === feof($f1);
}

function do_stream_copy($source, $dest) 
{
    $fi = fopen($source, "r");
    $fp = fopen($dest, "w");
    while ($data = fread($fi, 7000)) {
        fwrite($fp, $data);
    }
    fclose($fp);
    fclose($fi);
}

function generate_test_file()
{
    echo "Creating random file\n\t";
    $fname = tempnam("/tmp/", "mongofs");
    $fp    = fopen($fname, "w");
    if (!$fp) {
        throw new Exception("Error while creating testing file");
    }
    $size =rand(40000, 800000);
    for ($i=0; $i < $size; $i++) {
        fwrite($fp, sha1(($i * $size), false));
    }
    fclose($fp);
    echo "Done\n";
    return $fname;
}

/*
 * Local variables:
 * tab-width: 4
 * c-basic-offset: 4
 * End:
 * vim600: sw=4 ts=4 fdm=marker
 * vim<600: sw=4 ts=4
 */
