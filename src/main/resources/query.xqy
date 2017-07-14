xquery version "1.0-ml";

for $URI in cts:uris( (), ('limit=1000') )
return
    if ($URI and fn:doc-available($URI))
    then (
    fn:concat($URI,"~~~",
        xdmp:md5(fn:concat(
            xdmp:quote(fn:doc($URI)),
            xdmp:quote(xdmp:document-properties($URI)),
            for $i in xdmp:quote(xdmp:document-get-collections($URI))
            order by $i
            return $i))))
    else()