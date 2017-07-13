import com.marklogic.xcc.*;
import com.marklogic.xcc.exceptions.RequestException;
import com.marklogic.xcc.exceptions.XccConfigException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by ableasdale on 12/07/2017.
 */
public class MD5Sync {

    private static Logger LOG = LoggerFactory.getLogger("MD5Sync");
    private static String lastProcessedURI = null;
    private static String batchQuery = null;

    private static ContentSource csSource = null;
    private static ContentSource csTarget = null;

    private static ResultSequence getBatch(String uri, Session sourceSession) {


        ResultSequence rs = null;
        Request request = sourceSession.newAdhocQuery(batchQuery.replace("(),", "\""+uri+"\","));
        try {
            rs = sourceSession.submitRequest(request);
        } catch (RequestException e) {
            e.printStackTrace();
        }
        return rs;
    }

    public static void main(String[] args) {
        Map<String, MarkLogicDocument> documentMap = new HashMap<String, MarkLogicDocument>();

        try {
            batchQuery = new String(Files.readAllBytes(Paths.get("src/main/resources/query.xqy")));
        } catch (IOException e) {
            LOG.error("Exception caught: ", e);
        }

        try {
            csSource = ContentSourceFactory.newContentSource(URI.create(Config.INPUT_XCC_URI));
            csTarget = ContentSourceFactory.newContentSource(URI.create(Config.OUTPUT_XCC_URI));
            Session sourceSession = csSource.newSession();
            Session targetSession = csTarget.newSession();

            ResultSequence rs = getBatch("/", sourceSession);
            // LOG.info(rs.asString());

            processResultSequence(documentMap, sourceSession, targetSession, rs);

            LOG.debug(String.format("Sequence size: %s%d%s", Config.ANSI_GREEN, rs.size(), Config.ANSI_RESET));


            rs = getBatch(lastProcessedURI, sourceSession);
            processResultSequence(documentMap, sourceSession, targetSession, rs);

            sourceSession.close();
            targetSession.close();

        } catch (XccConfigException e) {
            LOG.error("Exception caught: ", e);
        } catch (RequestException e) {
            LOG.error("Exception caught: ", e);
        }


    }

    private static void processResultSequence(Map<String, MarkLogicDocument> documentMap, Session sourceSession, Session targetSession, ResultSequence rs) throws RequestException {

        while (rs.hasNext()) {
            ResultItem i = rs.next();
            MarkLogicDocument md = new MarkLogicDocument();
            md.setUri(i.asString().substring(0, i.asString().lastIndexOf("~~~")));
            md.setSourceMD5(i.asString().substring(i.asString().lastIndexOf("~~~") + 3));

            // Check target
            Request targetRequest = targetSession.newAdhocQuery(String.format("fn:doc-available(\"%s\")", md.getUri()));
            ResultSequence rsT = targetSession.submitRequest(targetRequest);
            LOG.debug("Is the doc available? " + rsT.asString());

            if (rsT.asString().equals("false")) {
                if(md.getUri().equals("/")){
                    LOG.info("Don't need to replicate an empty directory node: "+md.getUri());
                } else {
                    LOG.debug(String.format("We need to copy this doc (%s) over", md.getUri()));
                    Request sourceDocReq = sourceSession.newAdhocQuery("fn:doc(\"" + md.getUri() + "\")");
                    ResultSequence rsS = sourceSession.submitRequest(sourceDocReq);
                    Content content = ContentFactory.newContent(md.getUri(), rsS.resultItemAt(0).asString(), null);
                    targetSession.insertContent(content);
                }

            } else {
                LOG.debug(String.format("We don't need to copy this doc(%s) over - TODO - MD5", md.getUri()));

            }


            documentMap.put(md.getUri(), md);
            //LOG.info("idx"+i.asString().lastIndexOf("~~~"));
            //LOG.info(i.asString().split("~~~")[1]);
            //LOG.info(i.asString().split("~~~")[2]);
            if (!rs.hasNext()) {
                LOG.info(String.format("Last item in batch: %s%s%s", Config.ANSI_BLUE, md.getUri(), Config.ANSI_RESET));
                lastProcessedURI = md.getUri();
            }
        }
    }

}
