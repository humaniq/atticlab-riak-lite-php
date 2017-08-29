<?php

namespace Atticlab\RiakLite\Riak;

class Query
{
    const TYPE_WHERE = 'w';
    const TYPE_ORWHERE = 'ow';

    private $query;
    public $bucket;
    public $offset = 0;
    public $limit = 10;

    public function __construct($bucket)
    {
        $this->bucket = $bucket;
        $this->query = [];
    }

    /**
     * @param      $key
     * @param null $value can be string, supports wildcards e.g: foo* or foo*bar and range queries
     * @return $this
     */
    public function where($key, $value = null)
    {
        $this->buildWhere(self::TYPE_WHERE, $key, $value);

        return $this;
    }

    public function orWhere($key, $value = null)
    {
        $this->buildWhere(self::TYPE_ORWHERE, $key, $value);

        return $this;
    }

    public function offset($offset)
    {
        $this->offset = $offset;

        return $this;
    }

    public function limit($limit)
    {
        $this->limit = $limit;

        return $this;
    }

    public static function escapeValue($string)
    {
        $match = array('\\', '+', '-', '&', '|', '!', '(', ')', '{', '}', '[', ']', '^', '~', '*', '?', ':', '"', ';', ' ');
        $replace = array('\\\\', '\\+', '\\-', '\\&', '\\|', '\\!', '\\(', '\\)', '\\{', '\\}', '\\[', '\\]', '\\^', '\\~', '\\*', '\\?', '\\:', '\\"', '\\;', '\\ ');
        $string = str_replace($match, $replace, $string);

        return $string;
    }

    private function buildWhere($type, $key, $value)
    {
        if (is_callable($key)) {
            $expr = $key(new self($this->bucket));
        } else {
            if (empty($value)) {
                throw new \Exception('Riak search error: search value cannot be empty');
            }

            // Check range queries
            if (is_array($value) && count($value) != 2) {
                throw new \Exception('Riak search error: range query should have exactly 2 keys');
            }

            $expr = [$key, $value];
        }

        $this->query[] = [$type, $expr];

        return $this;
    }

    public function buildQuery()
    {
        if (empty($this->query)) {
            return '*:*';
        }

        $query = '';
        foreach ($this->query as $key => $expr) {
            $type = $expr[0];
            $q = $expr[1];

            switch ($type) {
                case self::TYPE_ORWHERE:
                    if (!empty($key)) {
                        $query .= 'OR ';
                    }
                    break;

                case self::TYPE_WHERE:
                    if (!empty($key)) {
                        $query .= 'AND ';
                    }
                    break;
            }

            if (is_object($q) && $q instanceof self) {
                $query .= "({$q->buildQuery()})";
            } else {
                $value = $q[1];

                if (is_array($value)) {
                    $value = '[' . $value[0] . ' TO ' . $value[1] . ']';
                }

                $query .= "$q[0]:$value";
            }

            $query .= ' ';
        }

        return trim($query);
    }
}