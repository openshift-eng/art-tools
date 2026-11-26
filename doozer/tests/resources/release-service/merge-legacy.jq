def merge_objects(a; b):
      a as $a | b as $b |
      ($a | keys) + ($b | keys) | unique | map({
        key: .,
        value: (
          if ($a[.] | type) == "object" and ($b[.] | type) == "object" then
            merge_objects($a[.]; $b[.])
          elif ($a[.] | type) == "array" and ($b[.] | type) == "array" then
            ($a[.] + $b[.]) | unique
          else
            if ($b[.] != null) then $b[.] else $a[.] end
          end
        )
      }) | from_entries;
    .[0] as $first | .[1] as $second | merge_objects($first; $second)
