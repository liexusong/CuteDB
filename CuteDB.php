<?php

/**
 * Tiny DB implements in PHP
 * Using HashTable algorithm
 * @author: Liexusong <liexusong@qq.com>
 */

class CuteDB
{
	const CUTEDB_ENTRIES = 1048576;
	const CUTEDB_VERSION = 1;
	const CUTEDB_HEADER_SIZE = 12;

	private $_idxfile = null;
	private $_datfile = null;

	private function initDB()
	{
		/**
		 * DB index file header:
		 * 4 bytes for "CUTE"
		 * 4 bytes for version
		 * 4 bytes for hash bucket entries
		 */
		$header = pack('C4L2', 67, 85, 84, 69,
			           CuteDB::CUTEDB_VERSION,
			           CuteDB::CUTEDB_ENTRIES);

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

		$sign = unpack('L2', substr($header, 4));

		if ($sign[1] != CuteDB::CUTEDB_VERSION ||
		    $sign[2] != CuteDB::CUTEDB_ENTRIES)
		{
			return false;
		}

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
		if ($hash < 0) {
			$hash = -$hash;
		}

		$index = $hash % CuteDB::CUTEDB_ENTRIES;

		return CuteDB::CUTEDB_HEADER_SIZE + $index * 4;
	}

	public function set($key, $value)
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

			$offset = unpack('L', substr($item, 0, 4))[1];
			$datoff = unpack('L', substr($item, 4, 4))[1];
			$datlen = unpack('L', substr($item, 8, 4))[1];
			$delete = unpack('C', substr($item, 12, 1))[1];

			$cmpkey = rtrim(substr($item, 13), "\0");

			if ($cmpkey == $key) {
				$update = true;
				break;
			}
		}

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

		if ($update) {
			$keyItem = pack('L3C', $offset, $datoff, $datlen, 0);
		} else {
			$keyItem = pack('L3C', $headoffset, $datoff, $datlen, 0);
		}

		$keyItem .= $key;
		if (strlen($keyItem) > 128) {
			return false;
		}

		if (strlen($keyItem) < 128) {
			$zero = pack('x');
			for ($i = 128 - strlen($keyItem); $i > 0; $i--) {
				$keyItem .= $zero;
			}
		}

		if (!$update) {
			if (fseek($this->_idxfile, 0, SEEK_END) == -1) {
				return false;
			}

			$prevoffset = ftell($this->_idxfile);

			if (fwrite($this->_idxfile, $keyItem) != 128) {
				return false;
			}

			if (fseek($this->_idxfile, $indexoffset, SEEK_SET) == -1) {
				return false;
			}

			if (fwrite($this->_idxfile, pack('L', $prevoffset)) != 4) {
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

			$offset = unpack('L', substr($item, 0, 4))[1];
			$datoff = unpack('L', substr($item, 4, 4))[1];
			$datlen = unpack('L', substr($item, 8, 4))[1];
			$delete = unpack('C', substr($item, 12, 1))[1];

			$cmpkey = rtrim(substr($item, 13), "\0");

			if ($cmpkey == $key) {
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

			$offset = unpack('L', substr($item, 0, 4))[1];
			$datoff = unpack('L', substr($item, 4, 4))[1];
			$datlen = unpack('L', substr($item, 8, 4))[1];
			$delete = unpack('C', substr($item, 12, 1))[1];

			$cmpkey = rtrim(substr($item, 13), "\0");

			if ($cmpkey == $key) {
				$found = true;
				break;
			}
		}

		if (!$found || $delete) {
			return false;
		}

		$keyItem = pack('L3C', $offset, $datoff, $datlen, 1);

		$keyItem .= $key;
		if (strlen($keyItem) > 128) {
			return false;
		}

		if (strlen($keyItem) < 128) {
			$zero = pack('x');
			for ($i = 128 - strlen($keyItem); $i > 0; $i--) {
				$keyItem .= $zero;
			}
		}

		if (fseek($this->_idxfile, $curroffset, SEEK_SET) == -1) {
			return false;
		}

		if (fwrite($this->_idxfile, $keyItem) != 128) {
			return false;
		}

		return true;
	}
}
