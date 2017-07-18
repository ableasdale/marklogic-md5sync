package com.marklogic.support.md5sync;

/**
 * Created by ableasdale on 12/07/2017.
 */
public class Config {

    public static final int THREAD_POOL_SIZE = 64;

    public static final String ANSI_RED = "\u001B[31m";
    public static final String ANSI_GREEN = "\u001B[32m";
    public static final String ANSI_BLUE = "\u001B[34m";
    public static final String ANSI_RESET = "\u001B[0m";

    public static final String CTS_URIS_QUERY = "src/main/resources/query.xqy";
    public static final String MD5_ONELINE = "xdmp:md5(fn:concat(xdmp:quote(fn:doc($URI)),xdmp:quote(xdmp:document-properties($URI)),(xdmp:quote(for $i in xdmp:document-get-permissions($URI) order by $i//sec:role-id, $i//sec:capability return $i)),(for $j in xdmp:quote(xdmp:document-get-collections($URI)) order by $j return $j)))";
    public static final String PERMISSIONS_QUERY = "let $perms := for $i in xdmp:document-get-permissions(\"%s\") return xdmp:quote($i) return fn:concat('(',fn:string-join($perms, ','),')')";
    public static final String PROPERTIES_QUERY = "xdmp:document-properties(\"%s\")/prop:properties/*";
    public static final String COLLECTIONS_QUERY = "(string-join(xdmp:document-get-collections(\"%s\"),'~')))";
    public static final String FN_DOC = "(fn:doc(\"%s\")";

}
