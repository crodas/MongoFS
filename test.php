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

define("RAND_OPERATIONS", 30000);

try {
    $localfile  = generate_test_file();
    $remotefile = "gridfs://testing.bin";
    $tmpfile    = tempnam("/tmp/", "mongofs");

    print "Uploading file to MongoDB\n";
    do_stream_copy($localfile, $remotefile);
    print "\tOK\n";

    print "Downloading file\n";
    do_stream_copy($remotefile, $tmpfile);
    print "\tOK\n";

    print "Comparing local and remote files\n";
    stream_cmp($tmpfile, $localfile);
    print "\tOK\n";


    print "Random reading\n";
    partial_reading($tmpfile, $remotefile);
    print "\tOK\n";

    print "Random writing\n";
    partial_writing($tmpfile, $remotefile);
    print "\tOK\n";

} catch (Exception $e) {
    echo "\tFAILED:";
    echo $e->getMessage()."\n";
}

/* delete files  */
unlink($remotefile);
unlink($tmpfile);
unlink($localfile);

// do_stream_copy($source, $dest) {{{
function do_stream_copy($source, $dest)
{
    $f1 = fopen($source, "r");
    $f2 = fopen($dest, "w");

    while ($data = fread($f1, 8096)) {
        fwrite($f2, $data);
    }
    fclose($f1);
    fclose($f2);
}
// }}}

// bool partial_reading(string $file1, string $file2) {{{
function partial_reading($file1, $file2)
{
    $fi = fopen($file1, "r");
    $fp = fopen($file2, "r");

    $max = filesize($file1);

    for ($i=0; $i < RAND_OPERATIONS; $i++) {
        /* random offset */
        $offset = rand(0, $max);
        fseek($fp, $offset, SEEK_SET);
        fseek($fi, $offset, SEEK_SET);
       
        /* random data */
        $bytes = rand(1, 1024);
        $data1 = fread($fp, $bytes);
        $data2 = fread($fi, $bytes);
        if ($data1 !== $data2) {
            throw new Exception("File mismatch at position $offset");
        }
    }
    fclose($fp);
    fclose($fi);
}
// }}}

// bool partial_writing(string $file1, string $file2) {{{
function partial_writing($file1, $file2)
{
    $fi = fopen($file1, "r+");
    $fp = fopen($file2, "r+");

    $max = filesize($file1);

    for ($i=0; $i < RAND_OPERATIONS; $i++) {
        /* random offset */
        $offset = rand(0, $max); 
        fseek($fp, $offset, SEEK_SET);
        fseek($fi, $offset, SEEK_SET);
       
        /* random data */
        $data  = strtoupper(sha1(microtime()));
        $data .= strtoupper(sha1(microtime()));

        fwrite($fi, $data);
        fwrite($fp, $data);

    }

    fclose($fp);
    fclose($fi);

    return stream_cmp($file1, $file2, 50);
}
// }}}

// bool stream_cmp($file1, $file2, $bytes) {{{
function stream_cmp($file1, $file2, $bytes = 8096)
{
    $size1 = filesize($file1);
    $size2 = filesize($file2);
    if ($size1 != $size2) {
        throw new Exception("file size mismatch {$size1} != {$size2}");
    }

    $f1 = fopen($file1, "r");
    $f2 = fopen($file2, "r");

    while (!feof($f2) && !feof($f1)) {
        $data2 = fread($f2, $bytes);
        $data1 = fread($f1, $bytes);
        if ($data1 !== $data2) {
            for ($i=0; $i < $bytes; $i++) {
                if ($data1[$i] != $data2[$i]) {
                    break;
                }
            }
            var_dump(array($data1, $data2));
            throw new exception("File mismatch at position ".(ftell($f1)+$i));
        }
    }

    if (feof($f2) !== feof($f1)) {
        var_dump("Unexpected offset error");
        //throw new Exception("Unexpected offset error");
    }
    if (sha1_file($file1) !== sha1_file($file2)) {
        throw new Exception("SHA1 mismatch");
    }
    fclose($f2);
    fclose($f1);
}
// }}}

// string generate_test_file() {{{
function generate_test_file()
{
    echo "Creating random file\n\t";
    $fname = tempnam("/tmp/", "mongofs");
    $fp    = fopen($fname, "w");
    if (!$fp) {
        throw new Exception("Error while creating testing file");
    }
    $size = rand(40000, 1000000);
    for ($i=0; $i < $size; $i++) {
        fwrite($fp, sha1(($i * $size)));
    }
    fclose($fp);
    echo "Done\n";
    return $fname;
}
// }}}

/*
 * Local variables:
 * tab-width: 4
 * c-basic-offset: 4
 * End:
 * vim600: sw=4 ts=4 fdm=marker
 * vim<600: sw=4 ts=4
 */
