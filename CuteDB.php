<?php

/**
 * Tiny DataBase implements in PHP using HashTable algorithm
 * @author: Liexusong <liexusong@qq.com>
 */

class CuteDB
{
    const CUTEDB_ENTRIES = 1048576;
    const CUTEDB_VERSION = 2;
    const CUTEDB_HEADER_SIZE = 20;
    const CUTEDB_LINK_OFFSET = 12;

    private $_idxfile = null;
    private $_datfile = null;
    private $_headoff = 0;
    private $_tailoff = 0;
    private $_iterator = null;

    private function initDB()
    {
        /**
         * DB index file header:
         * 4 bytes for "CUTE"
         * 4 bytes for version
         * 4 bytes for hash bucket entries
         * 4 bytes for link list head
         * 4 bytes for link list tail
         */
        $header = pack('C4L4', 67, 85, 84, 69,
                       CuteDB::CUTEDB_VERSION,
                       CuteDB::CUTEDB_ENTRIES,
                       0, 0);

        if (fwrite($this->_idxfile, $header) != CuteDB::CUTEDB_HEADER_SIZE) {
            return false;
        }

        $block = pack('L128',
                       0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                       0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                       0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                       0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                       0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                       0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                       0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                       0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0);

        $blockSize = strlen($block);
        $bucketSize = CuteDB::CUTEDB_ENTRIES * 4;

        for ($i = 0; $i < $bucketSize; $i += $blockSize) {
            if (fwrite($this->_idxfile, $block) != $blockSize) {
                return false;
            }
        }

        return true;
    }

    private function validateDB()
    {
        $header = fread($this->_idxfile, CuteDB::CUTEDB_HEADER_SIZE);

        if (!$header || strlen($header) != CuteDB::CUTEDB_HEADER_SIZE) {
            return false;
        }

        $sign = unpack('C4', substr($header, 0, 4));

        if ($sign[1] != 67 || $sign[2] != 85 ||
            $sign[3] != 84 || $sign[4] != 69)
        {
            return false;
        }

        $sign = unpack('L4', substr($header, 4));

        if ($sign[1] != CuteDB::CUTEDB_VERSION ||
            $sign[2] != CuteDB::CUTEDB_ENTRIES)
        {
            return false;
        }

        $this->_headoff = $sign[3];
        $this->_tailoff = $sign[4];

        $this->_iterator = $this->_headoff;

        return true;
    }

    public function open($dbname)
    {
        if ($this->_idxfile) {
            fclose($this->_idxfile);
        }

        if ($this->_datfile) {
            fclose($this->_datfile);
        }

        $idxFileName = sprintf('%s.idx', $dbname);
        $datFileName = sprintf('%s.dat', $dbname);

        $init = false;

        if (!file_exists($idxFileName)) {
            $init = true;
            $mode = 'w+b';
        } else {
            $mode = 'r+b';
        }

        $this->_idxfile = fopen($idxFileName, $mode);
        $this->_datfile = fopen($datFileName, $mode);

        if (!$this->_idxfile || !$this->_datfile) {
            if ($this->_idxfile) {
                fclose($this->_idxfile);
            }

            if ($this->_datfile) {
                fclose($this->_datfile);
            }

            return false;
        }

        if ($init) {
            return $this->initDB();
        }

        return $this->validateDB();
    }

    private function getIndexOffset($key)
    {
        $hash = crc32($key);
        if ($hash < 0) { /* 32bit system may be return negative */
            $hash = -$hash;
        }

        $index = $hash % CuteDB::CUTEDB_ENTRIES;

        return CuteDB::CUTEDB_HEADER_SIZE + $index * 4;
    }

    private function unpackItem($item)
    {
        $package = unpack('L5', substr($item, 0, 20));

        $offset = $package[1];
        $preoff = $package[2];
        $nexoff = $package[3];
        $datoff = $package[4];
        $datlen = $package[5];

        $package = unpack('C2', substr($item, 20, 2));

        $keylen = $package[1];
        $delete = $package[2];

        $keyval = substr($item, 22, $keylen);

        return [
            $offset,
            $preoff,
            $nexoff,
            $datoff,
            $datlen,
            $delete,
            $keyval,
        ];
    }

    private function packItem(
        $offset, $preoff, $nexoff, $datoff,
        $datlen, $keylen, $delete, $keyval
    ) {
        $keyItem = pack('L5C2', $offset, $preoff, $nexoff,
                        $datoff, $datlen, $keylen, $delete);

        $keyItem .= $keyval;

        if (strlen($keyItem) > 128) {
            return false;
        }

        if (strlen($keyItem) < 128) {
            $zero = pack('x');
            for ($i = 128 - strlen($keyItem); $i > 0; $i--) {
                $keyItem .= $zero;
            }
        }

        return $keyItem;
    }

    public function set($key, $value, $replace = false)
    {
        $indexoffset = $this->getIndexOffset($key);

        if (fseek($this->_idxfile, $indexoffset, SEEK_SET) == -1) {
            return false;
        }

        $item = fread($this->_idxfile, 4);
        if (strlen($item) != 4) {
            return false;
        }

        $headoffset = unpack('L', $item)[1];
        $curroffset = 0;

        $update = false;
        $offset = $headoffset;

        while ($offset) {

            if (fseek($this->_idxfile, $offset, SEEK_SET) == -1) {
                return false;
            }

            $curroffset = $offset;

            $item = fread($this->_idxfile, 128);
            if (strlen($item) != 128) {
                return false;
            }

            $package = $this->unpackItem($item);

            $offset = $package[0];
            $preoff = $package[1];
            $nexoff = $package[2];
            $datoff = $package[3];
            $datlen = $package[4];
            $delete = $package[5];
            $keyval = $package[6];

            if ($keyval == $key) {
                $update = true;
                break;
            }
        }

        if ($update && !$replace) {
            return false;
        }

        /* store data and get offset */

        if (!$update || $datlen < strlen($value)) {
            if (fseek($this->_datfile, 0, SEEK_END) == -1) {
                return false;
            }
            $datoff = ftell($this->_datfile);
        } else {
            if (fseek($this->_datfile, $datoff, SEEK_SET) == -1) {
                return false;
            }
        }

        $datlen = strlen($value);

        if (fwrite($this->_datfile, $value) != $datlen) {
            return false;
        }

        /* store key index and get offset */

        if (!$update) {
            $offset = $headoffset;
            $preoff = $this->_tailoff;
            $nexoff = 0;
        }

        $keylen = strlen($key);

        $keyItem = $this->packItem($offset, $preoff, $nexoff,
                                   $datoff, $datlen, $keylen, 0, $key);

        if (!$keyItem) {
            return false;
        }

        if (!$update) { /* new key */

            $tailoff = $this->_tailoff; /* prev key offset */

            if (fseek($this->_idxfile, 0, SEEK_END) == -1) {
                return false;
            }

            $this->_tailoff = ftell($this->_idxfile);

            if (!$this->_headoff) {
                $this->_headoff = $this->_tailoff;
                $this->_iterator = $this->_headoff;
            }

            if (fwrite($this->_idxfile, $keyItem) != 128) {
                return false;
            }

            /* update bucket index */

            if (fseek($this->_idxfile, $indexoffset, SEEK_SET) == -1) {
                return false;
            }

            $lastoff = pack('L', $this->_tailoff);

            if (fwrite($this->_idxfile, $lastoff) != 4) {
                return false;
            }

            /* update link list */

            if ($tailoff > 0) { /* update last node next pointer */

                if (fseek($this->_idxfile, $tailoff, SEEK_SET) == -1) {
                    return false;
                }

                $item = fread($this->_idxfile, 128);
                if (strlen($item) != 128) {
                    return false;
                }

                $package = $this->unpackItem($item);

                $offset = $package[0];
                $preoff = $package[1];
                $nexoff = $package[2];
                $datoff = $package[3];
                $datlen = $package[4];
                $delete = $package[5];
                $keyval = $package[6];
                $keylen = strlen($keyval);

                $keyItem = $this->packItem($offset, $preoff, $this->_tailoff,
                                           $datoff, $datlen, $keylen, $delete,
                                           $keyval);

                if (!$keyItem) {
                    return false;
                }

                if (fseek($this->_idxfile, $tailoff, SEEK_SET) == -1) {
                    return false;
                }

                if (fwrite($this->_idxfile, $keyItem) != 128) {
                    return false;
                }
            }

            if (fseek($this->_idxfile, CuteDB::CUTEDB_LINK_OFFSET) == -1) {
                return false;
            }

            $link = pack('L2', $this->_headoff, $this->_tailoff);

            if (fwrite($this->_idxfile, $link) != 8) {
                return false;
            }

        } else {
            if (fseek($this->_idxfile, $curroffset, SEEK_SET) == -1) {
                return false;
            }

            if (fwrite($this->_idxfile, $keyItem) != 128) {
                return false;
            }
        }

        return true;
    }

    public function get($key)
    {
        $indexoffset = $this->getIndexOffset($key);

        if (fseek($this->_idxfile, $indexoffset, SEEK_SET) == -1) {
            return false;
        }

        $item = fread($this->_idxfile, 4);
        if (strlen($item) != 4) {
            return false;
        }

        $found = false;
        $datlen = 0;
        $datoff = 0;
        $delete = 0;
        $offset = unpack('L', $item)[1];

        while ($offset) {

            if (fseek($this->_idxfile, $offset, SEEK_SET) == -1) {
                return false;
            }

            $item = fread($this->_idxfile, 128);
            if (strlen($item) != 128) {
                return false;
            }

            $package = $this->unpackItem($item);

            $offset = $package[0];
            $preoff = $package[1];
            $nexoff = $package[2];
            $datoff = $package[3];
            $datlen = $package[4];
            $delete = $package[5];
            $keyval = $package[6];

            if ($keyval == $key) {
                $found = true;
                break;
            }
        }

        if (!$found || $delete) {
            return false;
        }

        if (fseek($this->_datfile, $datoff, SEEK_SET) == -1) {
            return false;
        }

        return fread($this->_datfile, $datlen);
    }

    public function delete($key)
    {
        $indexoffset = $this->getIndexOffset($key);

        if (fseek($this->_idxfile, $indexoffset, SEEK_SET) == -1) {
            return false;
        }

        $item = fread($this->_idxfile, 4);
        if (strlen($item) != 4) {
            return false;
        }

        $headoffset = unpack('L', $item)[1];
        $curroffset = 0;

        $found = false;
        $offset = $headoffset;

        while ($offset) {

            if (fseek($this->_idxfile, $offset, SEEK_SET) == -1) {
                return false;
            }

            $curroffset = $offset;

            $item = fread($this->_idxfile, 128);
            if (strlen($item) != 128) {
                return false;
            }

            $package = $this->unpackItem($item);

            $offset = $package[0];
            $preoff = $package[1];
            $nexoff = $package[2];
            $datoff = $package[3];
            $datlen = $package[4];
            $delete = $package[5];
            $keyval = $package[6];

            if ($keyval == $key) {
                $found = true;
                break;
            }
        }

        if (!$found || $delete) {
            return false;
        }

        $keylen = strlen($key);
        $delete = 1;

        $keyItem = $this->packItem($offset, $preoff, $nexoff, $datoff,
                                   $datlen, $keylen, $delete, $key);

        if (!$keyItem) {
            return false;
        }

        if (fseek($this->_idxfile, $curroffset, SEEK_SET) == -1) {
            return false;
        }

        if (fwrite($this->_idxfile, $keyItem) != 128) {
            return false;
        }

        return true;
    }

    public function moveHead()
    {
        $this->_iterator = $this->_headoff;
    }

    public function moveTail()
    {
        $this->_iterator = $this->_tailoff;
    }

    public function next()
    {
        if (!$this->_iterator) {
            return false;
        }

        if (fseek($this->_idxfile, $this->_iterator) == -1) {
            return false;
        }

        $item = fread($this->_idxfile, 128);
        if (strlen($item) != 128) {
            return false;
        }

        $package = $this->unpackItem($item);

        $offset = $package[0];
        $preoff = $package[1];
        $nexoff = $package[2];
        $datoff = $package[3];
        $datlen = $package[4];
        $delete = $package[5];
        $keyval = $package[6];

        $this->_iterator = $nexoff;

        if ($delete) {
            return $this->next();
        }

        if (fseek($this->_datfile, $datoff) == -1) {
            return false;
        }

        $datval = fread($this->_datfile, $datlen);
        if (strlen($datval) != $datlen) {
            return false;
        }

        return [$keyval, $datval];
    }

    public function prev()
    {
        if (!$this->_iterator) {
            return false;
        }

        if (fseek($this->_idxfile, $this->_iterator) == -1) {
            return false;
        }

        $item = fread($this->_idxfile, 128);
        if (strlen($item) != 128) {
            return false;
        }

        $package = $this->unpackItem($item);

        $offset = $package[0];
        $preoff = $package[1];
        $nexoff = $package[2];
        $datoff = $package[3];
        $datlen = $package[4];
        $delete = $package[5];
        $keyval = $package[6];

        $this->_iterator = $preoff;

        if ($delete) {
            return $this->next();
        }

        if (fseek($this->_datfile, $datoff) == -1) {
            return false;
        }

        $datval = fread($this->_datfile, $datlen);
        if (strlen($datval) != $datlen) {
            return false;
        }

        return [$keyval, $datval];
    }

    public function flush()
    {
        fflush($this->_idxfile);
        fflush($this->_datfile);
    }

    public function close()
    {
        if ($this->_idxfile) {
            fclose($this->_idxfile);
        }

        if ($this->_datfile) {
            fclose($this->_datfile);
        }

        $this->_idxfile = null;
        $this->_datfile = null;
    }
}
