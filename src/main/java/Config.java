/**
 * Created by ableasdale on 12/07/2017.
 */
public class Config {
    public static final String ANSI_RED = "\u001B[31m";
    public static final String ANSI_GREEN = "\u001B[32m";
    public static final String ANSI_BLUE = "\u001B[34m";
    public static final String ANSI_RESET = "\u001B[0m";

    public static final String MD5_ONELINE = "xdmp:md5(fn:concat(xdmp:quote(fn:doc($URI)), xdmp:quote(xdmp:document-properties($URI)),for $i in xdmp:quote(xdmp:document-get-collections($URI)) order by $i return $i))";

    public static final String INPUT_XCC_URI = "xcc://admin:admin@engrlab-128-208.engrlab.marklogic.com:9000/Documents";
    public static final String OUTPUT_XCC_URI = "xcc://q:q@localhost:8000/18854";
}
