xquery version "1.0-ml";

for $URI in cts:uris( (), ('limit=1000') )
return fn:concat($URI,"~~~",
    if ($URI and fn:doc-available($URI))
    then (
        xdmp:md5(fn:concat(
            xdmp:quote(fn:doc($URI)),
            xdmp:quote(xdmp:document-properties($URI)),
            for $i in xdmp:quote(xdmp:document-get-collections($URI))
            order by $i
            return $i)))
    else())