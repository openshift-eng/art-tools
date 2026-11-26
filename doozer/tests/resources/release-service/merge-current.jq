def is_named:   type == "object" and has("name");
    def is_unnamed: is_named | not;
    def has_named_objects: any(.[]; is_named);

    def merge_objects(base; overlay):
      def merge_named_arrays(base_arr; overlay_arr):
        # build index by merging same-named base entries, not last-wins
        (base_arr | map(select(is_named))
                  | reduce .[] as $item (
                      {};
                      .[$item.name] = merge_objects((.[$item.name] // {}); $item)
                    )) as $base_index |
        # unnamed items are kept as-is from both sides
        (base_arr    | map(select(is_unnamed))) as $base_unnamed |
        (overlay_arr | map(select(is_unnamed))) as $overlay_unnamed |
        # merge each overlay entry with its base counterpart; new names start from {}
        (overlay_arr | map(select(is_named))
                     | reduce .[] as $item (
                         $base_index;
                         .[$item.name] = merge_objects((.[$item.name] // {}); $item)
                       )) as $result |
        [$result[]] + $base_unnamed + $overlay_unnamed;
      base as $base | overlay as $overlay |
      ($base | keys) + ($overlay | keys) | unique | map({
        key: .,
        value: (
          if ($base[.] | type) == "object" and ($overlay[.] | type) == "object" then
            merge_objects($base[.]; $overlay[.])
          elif ($base[.] | type) == "array" and ($overlay[.] | type) == "array" then
            if ($base[.] | has_named_objects) or ($overlay[.] | has_named_objects) then
              merge_named_arrays($base[.]; $overlay[.])
            else
              ($base[.] + $overlay[.]) | unique
            end
          else
            if ($overlay[.] != null) then $overlay[.] else $base[.] end
          end
        )
      }) | from_entries;
    .[0] as $first | .[1] as $second | merge_objects($first; $second)
