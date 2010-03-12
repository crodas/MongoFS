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

class MongoFS
{   
    /* constants {{{ */
    const OP_READ=1;
    const OP_READWRITE=2;
    const OP_WRITE=3;
    const OP_APPEND=4;
    const E_EXCEPTION=1;
    const E_USER_ERROR=2;
    /* }}} */

    // static properties {{{
    /** 
     *  Current databases objects
     *
     *  @type array
     */
    private static $_dbs;
    /**
     *  Current connection to MongoDB
     *
     *  @type MongoConnection
     */
    private static $_conn;
    /**
     *  Database name
     *
     *  @type string
     */
    private static $_db;
    /**
     *  Host name
     *
     *  @type string
     */
    private static $_host;
    // }}}

    /* file metadata */
    protected $filename;
    protected $mode;
    protected $size;
    protected $file_id;
    protected $chunk_size;
         
    /* IO Cache (read/write) */
    protected $cache;
    protected $cache_offset;
    protected $cache_size;
    protected $cache_dirty;

    /* offsets */
    protected $offset;

    /* mongo infromation */
    protected $grid;
    protected $chunks;
    protected $chunk;
    protected $cursor;
    protected $chunk_id;
    protected $total_chunks;

    private static $_err_reporting = self::E_EXCEPTION;

    // void connection($db, $host) {{{
    /**
     *  Connect
     *
     *  This method setup parameters to connect to a MongoDB
     *  database. The connection is done when it is needed.
     *
     *  @param string $db   Database name
     *  @param string $host Host to connect
     *
     *  @return void
     */
    final public static function connect($db, $host='localhost')
    {
        self::$_host = $host;
        self::$_db   = $db;
    }
    // }}}

    // MongoConnection _getConnection() {{{
    /**
     *  Get Connection
     *
     *  Get a valid database connection
     *
     *  @return MongoConnection
     */
    final protected function _getConnection()
    {
        if (is_null(self::$_conn)) {
            if (is_null(self::$_host)) {
                self::$_host = 'localhost';
            }
            self::$_conn = new Mongo(self::$_host);
        }
        $dbname = self::$_db;
        if (!isSet(self::$_dbs[$dbname])) {
            self::$_dbs[$dbname] = self::$_conn->selectDB($dbname);
        }
        return self::$_dbs[$dbname];
    }
    // }}}

    // void set_error(string $error) {{{
    /**
     *  set_error($error)
     *
     *  Report a given error by the selected method. By
     *  default it would throw an exception, but it can be
     *  changed to generate a PHP Error.
     *  
     */
    final protected function set_error($error)
    {
        if (self::$_err_reporting == self::E_EXCEPTION) {
            throw new Exception($error);
        } else {
            trigger_error($error);
        }
    }
    // }}}

    // bool set_openmode(string $mode_str) {{{
    /**
     *  set_openmode
     *
     *  This methods parses and validates the 'mode'
     *  in which the current file is opened.
     *
     */
    final protected function set_openmode($mode_str)
    {
        $mode_str = strtoupper($mode_str);
        $mode     = null;
        $error    = false;

        for($i=0; $i < strlen($mode_str); $i++) {
            switch ($mode_str[$i]) {
            case 'B':
                if ($mode == null) {
                    $error = true;
                    break 2;
                }
                break;
            case 'R':
                switch ($mode) {
                case null:
                    $mode = self::OP_READ;
                    break;
                case self::OP_WRITE:
                    $mode = OP_READWRITE;
                    break;
                default:
                    $error = true;
                    break 2;
                }
                break;
            case 'A':
                $mode = self::OP_APPEND;
                break;
            case '+':
                switch ($mode) {
                case self::OP_WRITE:
                case self::OP_READ:
                    $mode = self::OP_READWRITE;
                    break;
                default:
                    $error = true;
                    break 2;
                }
                break;
            case 'W':
                switch ($mode) {
                case null:
                    $mode = self::OP_WRITE;
                    break;
                case self::OP_READ:
                    $mode = self::OP_READWRITE;
                    break;
                case self::OP_READWRITE:
                    break;
                default:
                    $error = true;
                    break 3;
                }
                break;
            default:
                $error = true;
                break;
            }
        }

        if ($error) {
            $this->set_error("Error while parsing file mode, unexpected ({$mode_str[$i-1]})");
            return false;
        }
        $this->mode = $mode;
        return true;
    }
    // }}}

    // bool stream_open(string $filename, int $mode) {{{
    final public function stream_open($filename, $mode)
    {
        /* Parse $mode */
        if (!$this->set_openmode($mode)) {
            return false;
        }

        /* Open file or create */
        if (!$this->mongo_fs_open($filename)) {
            return false;
        }

        /* Set initial fp position */
        if (!$this->stream_seek(0, $this->mode==self::OP_APPEND ? SEEK_END : SEEK_SET)) {
            $this->set_error("Initial offset falied");
            return false;
        }

        /* succeed */
        return true;
    }
    // }}}

    // stream_read(int $bytes) {{{
    /**
     *  stream_read
     *
     */
    final function stream_read($bytes)
    {
        $cache      = & $this->cache;
        $offset     = & $this->cache_offset; 
        $chunk_size = $this->chunk_size;
        $cache_size = & $this->cache_size;
        $data       = "";

        if ($offset + $bytes >= $chunk_size) {
            $data  .= substr($cache, $offset);
            $bytes -= strlen($data);
            $this->stream_seek($chunk_size * ($this->chunk_id+1), SEEK_SET);
        }

        if ($bytes > 0) {
            $data  .= substr($cache, $offset, $bytes);
            $bytes = strlen($data);
            $offset       += $bytes; 
            $this->offset += $bytes;
        }

        return $data;
    }
    // }}}

    // bool mongo_fs_open($filename) {{{
    /**
     *  Open a File stored on MongoDB
     *
     *  This method opens a file stored on MongoDB, 
     *  if the file doesn't exists and  it is opened
     *  in write or append mode an empty file is created
     *
     *  @param filename
     *
     *  @return bool
     */
    protected function mongo_fs_open($filename)
    {
        $pos = strpos($filename, "://");
        if ($pos === false) {
            return false;
        }


        $filename = substr($filename, $pos + 3);

        if ($filename[0] != '/') {
            $filename = "/{$filename}";
        }

        $filter   = array(
            'filename' => $filename
        );

        $db   = $this->_getConnection();
        $grid = $db->getGridFS();

        $grid->ensureIndex(array("filename" => 1), array("unique" => 1));

        $attr = $grid->findOne($filter);

        if (!$attr) {
            switch ($this->mode) {
            case self::OP_WRITE:
            case self::OP_READWRITE:
                /* TODO, this is very simple to do with a simple
                 * insert.
                 */
                $grid->storeBytes("", array("filename" => $filename));
                $attr = $grid->findOne($filter);
                break;
            default:
                $this->set_error("{$filename} doesn't exists");
                return false;
            }
        } else if ($this->mode == self::OP_WRITE) {
            $document = array(
                '$set' => array(
                    'length' => 0,
                )
            );

            $grid->update($filter, array($document));
            $grid->chunks->remove(array("files_id" => $attr->file['_id']));
            /* The file is reset, so we set empty length */
            $attr->file['length'] = 0;
        }

        /* Load file metadata. */
        $this->filename  = $attr->file['filename'];
        $this->size      = $attr->file['length'];
        $this->chunk_size = $attr->file['chunkSize'];
        $this->file_id   = $attr->file['_id'];

        /* load grid and chunks references */
        $this->grid     = $grid;
        $this->chunks   = $grid->chunks; 
        $this->cursor   = $this->chunks->find(array("files_id" => $this->file_id));
        $this->chunk_id = -1;
        $this->offset   = 0;
        $this->cache_offset = 0; 
        $this->total_chunks = $this->cursor->count();


        return true;
    }
    // }}}

    // int stream_seek($offset, $whence) {{{
    /**
     *
     *
     */
    final public function stream_seek($offset, $whence)
    {
        $size = $this->size;
        if ($this->mode != self::OP_READ) {
            /* We might want go to the next new chunk */
            $size += 1;
            if ($this->chunk==null) {
                /* if the current chunk is not synced */
                /* yet we might want to move to the next chunk */
                /* (of course this function call flush() ) */
                $size += $this->chunk_size;
            }
        }
        switch ($whence) {
        case SEEK_SET:
            if ($offset > $size+1 || $offset < 0) {
                return false;
            }
            break;
        case SEEK_CUR:
            $offset += $this->offset;
            if ($offset > $size+1) {
                return false;
            }
            break;
        case SEEK_END:
            $offset += $this->size;
            if ($offset > $size) {
                return false;
            }
            break;
        default:
            return false;
        }
        
        $chunk_new = floor($offset / $this->chunk_size);
        $chunk_cur = $this->chunk_id;

        if ($chunk_new != $chunk_cur) {
            /* Save the old chunk, if any */
            if ($this->mode != self::OP_READ) {
                $this->stream_flush();
            }

            /* Delete current cursor and re-query it */
            $this->cursor->reset();

            $this->cursor = $this->chunks->find(array("files_id" => $this->file_id, "n" => $chunk_new));
            if ($this->cursor->count() == 0) {
                /* The requested chunk doesn't exits */
                if ($this->mode == self::OP_READ) {
                    $this->set_error("Fatal error while reading file chunk {$chunk_new}");
                    return false;
                }
                $this->cache      = str_repeat("X", $this->chunk_size);
                $this->cache_size = 0;
                $this->chunk      = null;
                $this->total_chunks++;
            } else {
                $this->cursor->next();
                $this->chunk      = $this->cursor->current();
                $this->cache      = $this->chunk['data']->bin;
                $this->cache_size = strlen($this->cache);
            }
            /* New Chunk ID */
            $this->chunk_id = $chunk_new; 
        }
        $this->cache_offset = $offset%$this->chunk_size;
        $this->offset       = $offset;

        return true;
    }
    // }}}

    // bool stream_flush() {{{
    /**
     *  If the file is opened in write mode and the 
     *  IO cache had changed this function will put 
     *  replace the file chunk at MongoDB.
     *
     */
    final function stream_flush()
    {
        if ($this->mode == self::OP_READ) {
            return false;
        }

        if ($this->chunk_id < 0 || !$this->cache_dirty) {
            return true;
        } 

        $cache = substr($this->cache, 0, $this->cache_size);

        if ($this->chunk == null) {
            $document = array(
                'files_id' => $this->file_id,
                'n' => $this->chunk_id,
                'data' => new MongoBinData($cache),
            );

            /* save the current chunk */
            $this->chunks->insert($document, true);
            $this->chunk = $document;
            
            $this->size += $this->cache_size;
        } else {
            $document = array(
                '$set' => array(
                    'data' => new MongoBinData($cache),
                ),
            );
            $filter = array(
                '_id' => $this->chunk['_id']
            );

            $this->chunks->update($filter, $document);

            if ($this->total_chunks == $this->chunk_id+1) {
                $this->size = ($this->chunk_id) * $this->chunk_size + $this->cache_size;
            }
        }

        /* flag the current cache as not-dirty */
        $this->cache_dirty = false;

        return true;
    }
    // }}}

    // stream_close() {{{
    /**
     *  fclose($fp):
     *
     *  This close the current file, also if the file is opened in 
     *  write, append or read/write mode, and the file had changed
     *  it would regenerate the md5 checksum and update it
     *
     */
    final function stream_close()
    {
        if ($this->mode == self::OP_READ) {
            return true;
        }
        $this->stream_flush();
        $command = array(
            "filemd5" => $this->file_id, 
            "root" => "fs",
        );
        $result = $this->_getConnection()->command( $command );

        if (true) {
            /* silly test to see if we count the size correctly */
            /* when it becames more stable I'll remove it */
            $size = $this->chunks->group(array(), array("size" => 0), new MongoCode("function (b,a) { a.size += b.data.len-4; }"), array("files_id" => $this->file_id)); 

            if ($size['retval'][0]['size'] != $this->size) {
                print_r(array($size['retval'][0]['size'], $this->size));
            }
        }

        if ($result['ok'] != 1) {
            $this->set_error("Imposible to get MD5 from MongoDB".$result['errmsg']);
            return false;
        }

        $document = array(
            '$set' => array(
                'length' => $this->size,
                'md5' => $result['md5'],
            ),
        );

        $this->grid->update(array('_id' => $this->file_id), $document);
        return true;
    }
    // }}}

    // stream_write($data) {{{
    /**
     *  Write into $data in the current file
     */
    final function stream_write($data)
    {
        if ($this->mode == self::OP_READ) {
            $this->set_error("Impossible to write in READ mode");
            return false;
        }
        $cache      = & $this->cache;
        $offset     = & $this->cache_offset; 
        $chunk_size = $this->chunk_size;
        $cache_size = & $this->cache_size;
        $data_size  = strlen($data);
        $wrote      = 0;

        if ($offset + $data_size > $chunk_size) {
            $wrote        += $chunk_size - $cache_size;
            $cache         = substr($cache, 0, $offset);
            $cache        .= substr($data, 0, $wrote);
            $cache_size    = strlen($cache);
            $this->offset += $chunk_size -  $cache_size;

            /* Move to the next chunk, stream_seek */
            /* will automatically sync it to mongodb */
            if (!$this->stream_seek($chunk_size * ($this->chunk_id+1), SEEK_SET)) {
                throw new MongoException("Offset falied");
            }

            $data      = substr($data, $wrote);
            $data_size = strlen($data);
        }

        if ($data_size > 0) {
            $left    = substr($cache, 0, $offset);
            $right   = substr($cache, $offset + $data_size);
            $cache   = $left.$data.$right;
            $offset += $data_size; 
            $wrote  += $data_size;
            $this->offset += $data_size;
        }

        if($offset > $cache_size) {
            $cache_size = $offset;
        }

        /* flag the current cache as dirty */
        $this->cache_dirty = true;

        return $wrote;
    }
    // }}}

    // stream_tell() {{{
    /**
     *  Return the current file pointer position
     */
    final function stream_tell()
    {
        return $this->offset;
    }
    // }}}

    // stream_eof() {{{
    /**
     *  Tell if the file pointer is at the end
     */
    final function stream_eof()
    {
        return $this->offset >= $this->size;
    }
    // }}}

    // stream_fstat() {{{
    /**
     *  Return stat info about the current file
     */
    final function stream_stat()
    {
        return array(
            'size' => $this->size,
        );
    }
    // }}}

    // unlink($file) {{{
    /**
     *  Remove the given file
     */
    final function unlink($file)
    {
        /* Set a fake mode, in order to see if the file exists */
        $this->mode = self::OP_READ;

        /* Open file or create */
        if (!$this->mongo_fs_open($file)) {
            return false;
        }

        $this->grid->remove(array("_id" => $this->file_id));
        $this->chunks->remove(array("files_id" => $this->file_id));

        return true;
    }
    // }}}

    // storeFile($filename, $name) {{{
    /**
     *  Simple wrap to the native "storeFile" method
     *
     *  @param string $filename File to upload
     *  @param string $name     Name for the uploaded file
     *
     *  @return 
     */
    public static function uploadFile($filename, $name=null) 
    {
        if ($name == null) {
            $name = basename($filename);
        }
        $f = new ActiveMongoFS;
        $db = $f->_getConnection();
        return $db->getGridFS()->storeFile($filename, array('filename' => $name));
    }
    // }}}

    function url_stat($file)
    {
        /* Set a fake mode, in order to see if the file exists */
        $this->mode = self::OP_READ;

        /* Open file or create */
        if (!$this->mongo_fs_open($file)) {
            return false;
        }
        return $this->stream_stat();
    }

}

/* Register the STREAM class */
stream_wrapper_register("gridfs", "MongoFS")
    or die("Failed to register protocol");

/*
 * Local variables:
 * tab-width: 4
 * c-basic-offset: 4
 * End:
 * vim600: sw=4 ts=4 fdm=marker
 * vim<600: sw=4 ts=4
 */
