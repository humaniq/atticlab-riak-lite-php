<?php
namespace Atticlab\RiakLite;

use Exception;

class ModelBase
{
    public function pickProperties($fields)
    {
        $save = [];

        if (empty($fields) || !is_array($fields)) {
            return $save;
        }

        foreach ($fields as $field) {
            if (property_exists($this, $field)) {
                $save[$field] = $this->$field;
            }
        }

        return $save;
    }

    public function fillProperties($data, array $allowed_only = [])
    {
        if (!empty($data) && is_array($data)) {
            foreach ($data as $field => $value) {
                if (property_exists($this, $field) && (empty($allowed_only) || in_array($field, $allowed_only))) {
                    $this->$field = $value;
                }
            }
        }
    }

    public static function generateId()
    {
        return time() . '-' . mt_rand();
    }
}