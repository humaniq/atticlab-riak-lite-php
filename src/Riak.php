<?php

namespace Atticlab\RiakLite;

class Riak
{
    const MAX_FILE_BYTESIZE = 1024 * 1024 * 2;

    private $host;

    private $ssl_path;
    private $ssl_pass;
    private $user_name;
    private $user_pass;

    public function __construct($host, $ssl_path = null, $ssl_pass = null, $user_name = null, $user_pass = null)
    {
        if (!filter_var($host, FILTER_VALIDATE_URL)) {
            throw new \Exception('Riak error: invalid host provided');
        }

        if (!empty($ssl_path)) {
            $this->ssl_path = $ssl_path;
        }

        if (!empty($ssl_pass)) {
            $this->ssl_pass = $ssl_pass;
        }

        if (!empty($user_name)) {
            $this->user_name = $user_name;
        }

        if (!empty($user_pass)) {
            $this->user_pass = $user_pass;
        }

        $this->host = rtrim($host, '/');
    }

    public function search(Riak\Query $query)
    {
        $params = http_build_query([
            'wt'    => 'json',
            'q'     => $query->buildQuery(),
            'start' => $query->offset,
            'rows'  => $query->limit,
            'sort'  => '_yz_id desc', //default sort
        ]);

        $curl = $this->initCurl('/search/query/' . $query->bucket . '?' . $params);
        curl_setopt($curl, CURLOPT_RETURNTRANSFER, true);
        $resp = curl_exec($curl);
        curl_close($curl);

        if (!empty($resp)) {
            return json_decode($resp, 1)['response'];
        }

        return $resp;
    }

    /**
     * Write to bucket
     * @param $bucket
     * @param $key
     * @param $data -- can be either string or an array
     * @param $bucket_type -- your own bucket type
     * @return mixed
     */
    public function set($bucket, $key, $data, $bucket_type = 'default')
    {
        $curl = $this->initCurl('/types/' . $bucket_type . '/buckets/' . $bucket . '/keys/' . $key, $data);
        $success = curl_exec($curl);
        curl_close($curl);

        return $success;
    }

    /**
     * Create counter with default value 0
     * @param $bucket
     * @param $key
     * @return mixed
     */
    private function createCounter($bucket, $key)
    {
        $curl = $this->initCurl('/types/counters/buckets/' . $bucket . '/datatypes/' . $key);
        curl_setopt_array($curl, [
            CURLOPT_CUSTOMREQUEST => 'POST',
            CURLOPT_HTTPHEADER    => ['Content-type: application/json'],
            CURLOPT_POSTFIELDS    => json_encode(['increment' => 0])
        ]);
        $success = curl_exec($curl);
        curl_close($curl);

        return $success;
    }

    /**
     * Get from bucket
     * @param $bucket
     * @param $key
     * @param $bucket_type -- your own bucket type
     * @return mixed
     */
    public function get($bucket, $key, $bucket_type = 'default')
    {
        $curl = $this->initCurl('/types/' . $bucket_type . '/buckets/' . $bucket . '/keys/' . $key);
        curl_setopt($curl, CURLOPT_RETURNTRANSFER, true);
        $resp = curl_exec($curl);
        curl_close($curl);

        return json_decode($resp, 1);
    }

    /**
     * Get counter from bucket
     * @param $bucket
     * @param $key
     * @return mixed
     */
    public function getCounter($bucket, $key)
    {
        $curl = $this->initCurl('/types/counters/buckets/' . $bucket . '/datatypes/' . $key);
        curl_setopt($curl, CURLOPT_RETURNTRANSFER, true);
        $resp = curl_exec($curl);
        curl_close($curl);

        return json_decode($resp, 1);
    }

    /**
     * Delete from bucket
     * @param $bucket
     * @param $key
     * @return mixed
     */
    public function delete($bucket, $key, $bucket_type = 'default')
    {
        $curl = $this->initCurl('/types/' . $bucket_type . '/buckets/' . $bucket . '/keys/' . $key);
        curl_setopt($curl, CURLOPT_CUSTOMREQUEST, 'DELETE');

        $success = curl_exec($curl);
        curl_close($curl);

        return $success;
    }

    /**
     * Increment counter value (for bucket type "counter")
     * @param $bucket
     * @param $key
     * @param $increment
     * @return mixed
     */
    public function increment($bucket, $key, $increment = 1)
    {
        $counter = $this->getCounter($bucket, $key);

        if (!$counter) {
            $result = $this->createCounter($bucket, $key);

            if (!$result) {
                throw new \Exception('Riak error: can not create counter');
            }
        }

        $curl = $this->initCurl('/types/counters/buckets/' . $bucket . '/datatypes/' . $key);
        curl_setopt_array($curl, [
            CURLOPT_CUSTOMREQUEST => 'POST',
            CURLOPT_HTTPHEADER    => ['Content-type: application/json'],
            CURLOPT_POSTFIELDS    => json_encode(['increment' => $increment])
        ]);
        $success = curl_exec($curl);
        curl_close($curl);

        return $success;
    }

    /**
     * Decrement counter value (for bucket type "counter")
     * @param $bucket
     * @param $key
     * @param $decrement
     * @return mixed
     */
    public function decrement($bucket, $key, $decrement = 1)
    {
        $counter = $this->getCounter($bucket, $key);

        if (!$counter) {
            $result = $this->createCounter($bucket, $key);

            if (!$result) {
                throw new \Exception('Riak error: can not create counter');
            }
        }

        $curl = $this->initCurl('/types/counters/buckets/' . $bucket . '/datatypes/' . $key);
        curl_setopt_array($curl, [
            CURLOPT_CUSTOMREQUEST => 'POST',
            CURLOPT_HTTPHEADER    => ['Content-type: application/json'],
            CURLOPT_POSTFIELDS    => json_encode(['decrement' => $decrement])
        ]);
        $success = curl_exec($curl);
        curl_close($curl);

        return $success;
    }

    public function uploadBinary($bucket, $filepath, $key = null, $content_type = null)
    {
        $filepath = realpath($filepath);
        if (!file_exists($filepath) || !is_readable($filepath)) {
            throw new \Exception('Riak error: cannot read file');
        }

        if (filesize($filepath) > self::MAX_FILE_BYTESIZE) {
            throw new \Exception('Riak error: file size exceeds allowed maximum: ' . self::MAX_FILE_BYTESIZE);
        }

        $key = $key ?? pathinfo($filepath, PATHINFO_BASENAME);
        if (empty($key)) {
            throw new \Exception('Empty key');
        }

        $curl = $this->initCurl('/buckets/' . $bucket . '/keys/' . $key);

        // Potentially this should work, but after php5.6 they add multiparts which break everthing
        // TODO: find a workaround for this
        //curl_setopt($curl, CURLOPT_POSTFIELDS, [
        //  'file' => new \CurlFile($filepath)
        //]);

        curl_setopt($curl, CURLOPT_POSTFIELDS, file_get_contents($filepath));

        $success = curl_exec($curl);
        curl_close($curl);

        return $success;
    }

    /**
     * Get binary from riak and save to file
     * @param $bucket
     * @param $key
     * @param $filepath
     * @return string
     */
    public function downloadBinary($bucket, $key, $filepath)
    {
        $dir = pathinfo($filepath, PATHINFO_DIRNAME);
        if (!is_writable($dir)) {
            throw new \Exception('Riak error: path is not writable ' . $dir);
        }

        $fp = fopen($filepath, 'w');
        $curl = $this->initCurl('/buckets/' . $bucket . '/keys/' . $key);
        curl_setopt($curl, CURLOPT_FILE, $fp);

        $success = curl_exec($curl);
        curl_close($curl);
        fclose($fp);

        return $success;
    }

    /**
     * Get schema from riak
     * @param $schema_name
     * @return string|bool
     */
    public function fetchSchema($schema_name)
    {
        $curl = $this->initCurl('/search/schema/' . $schema_name);
        curl_setopt($curl, CURLOPT_CUSTOMREQUEST, 'GET');
        curl_setopt($curl, CURLOPT_RETURNTRANSFER, 1);

        $schema = curl_exec($curl);
        curl_close($curl);

        return $schema;
    }

    public function createSchema($schema_name, $filepath)
    {
        if (!file_exists($filepath) || !is_readable($filepath)) {
            throw new \Exception('Riak error: cannot read file');
        }

        if (filesize($filepath) > self::MAX_FILE_BYTESIZE) {
            throw new \Exception('Riak error: file size exceeds allowed maximum: ' . self::MAX_FILE_BYTESIZE);
        }

        $key = pathinfo($filepath, PATHINFO_BASENAME);
        if (empty($key)) {
            throw new \Exception('Cannot retreive filename');
        }

        $curl = $this->initCurl('/search/schema/' . $schema_name);
        curl_setopt($curl, CURLOPT_CUSTOMREQUEST, 'PUT');
        curl_setopt($curl, CURLOPT_HTTPHEADER, ['Content-type: application/xml']);
        curl_setopt($curl, CURLOPT_POSTFIELDS, file_get_contents($filepath));

        $success = curl_exec($curl);
        curl_close($curl);

        return $success;
    }

    public function createIndex($index_name, $schema = '_yz_default', $n_val = 3)
    {
        $n_val = intval($n_val);

        $curl = $this->initCurl('/search/index/' . $index_name, [
            'schema' => $schema,
            'n_val' => $n_val ? $n_val : 3
        ]);

        $success = curl_exec($curl);
        curl_close($curl);

        return $success;
    }

    public function setBucketProperty($bucket, $propery_name, $index_name)
    {
        $curl = $this->initCurl('/buckets/' . $bucket . '/props', [
            'props' => [
                $propery_name => $index_name
            ]
        ]);

        $success = curl_exec($curl);
        curl_close($curl);

        return $success;
    }

    public function associateIndex($bucket, $index_name)
    {
        return $this->setBucketProperty($bucket, 'search_index', $index_name);
    }

    private function initCurl($route, $data = null)
    {
        $curl = curl_init();

        if ($this->user_name && $this->user_pass) {
            curl_setopt($curl, CURLOPT_USERPWD, $this->user_name . ":" . $this->user_pass);
        }

        curl_setopt_array($curl, [
            CURLOPT_RETURNTRANSFER => 0,
            CURLOPT_FAILONERROR    => 1,
            CURLOPT_URL            => $this->host . $route
        ]);

        if ($this->ssl_path) {
            curl_setopt_array($curl, [
                CURLOPT_SSL_VERIFYHOST => false,
                CURLOPT_SSL_VERIFYPEER => false,
                CURLOPT_SSLCERT => $this->ssl_path
            ]);

            if ($this->ssl_pass) {
                curl_setopt($curl, CURLOPT_SSLCERTPASSWD, $this->ssl_pass);
            }
        }

        if (!empty($data)) {
            curl_setopt_array($curl, [
                CURLOPT_CUSTOMREQUEST => 'PUT',
                CURLOPT_HTTPHEADER    => ['Content-type: application/json'],
                CURLOPT_POSTFIELDS    => json_encode($data)
            ]);
        }

        return $curl;
    }
}