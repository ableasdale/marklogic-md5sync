xquery version "1.0-ml";

for $URI in cts:uris( (), ('limit=5000') )
return
    if ($URI and fn:doc-available($URI))
    then (fn:concat($URI,"~~~", xdmp:md5(fn:concat(xdmp:quote(fn:doc($URI)),xdmp:quote(xdmp:document-properties($URI)),(xdmp:quote(for $i in xdmp:document-get-permissions($URI) order by $i//sec:role-id, $i//sec:capability return $i)),(for $j in xdmp:quote(xdmp:document-get-collections($URI)) order by $j return $j)))))
    else()