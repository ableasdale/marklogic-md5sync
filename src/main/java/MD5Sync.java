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
        Request request = sourceSession.newAdhocQuery(batchQuery.replace("(),", String.format("\"%s\",", uri)));
        try {
            rs = sourceSession.submitRequest(request);
        } catch (RequestException e) {
            e.printStackTrace();
        }
        return rs;
    }

    public static void main(String[] args) {
        Map<String, MarkLogicDocument> documentMap = new HashMap<>();

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

            runFinalReport(documentMap);

        } catch (XccConfigException e) {
            LOG.error("Exception caught: ", e);
        } catch (RequestException e) {
            LOG.error("Exception caught: ", e);
        }


    }

    private static void runFinalReport(Map<String, MarkLogicDocument> documentMap) {
        LOG.info("Generating report");
        // TODO - fails if the copy just took place as part of the run.
        for (String s : documentMap.keySet()) {
            MarkLogicDocument m = documentMap.get(s);
            StringBuilder sb = new StringBuilder();
            sb.append("URI:\t").append(Config.ANSI_BLUE).append(m.getUri()).append(Config.ANSI_RESET).append("\tSource MD5:\t").append(m.getSourceMD5());
            if (m.getSourceMD5().equals(m.getTargetMD5())) {
                LOG.info("We have a match");
            } else {
                sb.append("\tTarget MD5:\t").append(Config.ANSI_RED).append(m.getTargetMD5()).append(Config.ANSI_RESET);
                LOG.info(sb.toString());
            }
            // LOG.info("URI: " + Config.ANSI_BLUE + m.getUri() + Config.ANSI_RESET + " Source MD5: "+ m.getSourceMD5()+ " Target MD5: "+ m.getTargetMD5());
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
                if (md.getUri().equals("/")) {
                    LOG.info("Don't need to replicate an empty directory node: " + md.getUri());
                } else {
                    LOG.debug(String.format("We need to copy this doc (%s) over", md.getUri()));
                    Request sourceDocReq = sourceSession.newAdhocQuery(String.format("(fn:doc(\"%s\"), xdmp:document-properties(\"%s\")/prop:properties, (string-join(xdmp:document-get-collections(\"%s\"),'~')))", md.getUri(), md.getUri(), md.getUri()));
                    ResultSequence rsS = sourceSession.submitRequest(sourceDocReq);
                    LOG.info("Collection size: " +rsS.size());
                    // TODO - collections, properties, permissions etc... ?
                    ContentCreateOptions co = ContentCreateOptions.newXmlInstance();
                    co.setCollections(rsS.resultItemAt(2).asString().split("~"));

                    //co.setMetadata();
                    //co.setPermissions();

                    Content content = ContentFactory.newContent(md.getUri(), rsS.resultItemAt(0).asString(), co);
                    targetSession.insertContent(content);
                    LOG.info("xdmp:document-set-properties(\""+md.getUri()+"\", "+ rsS.resultItemAt(1).asString() +")");

                    Request targetProps = targetSession.newAdhocQuery("xdmp:document-set-properties(\""+md.getUri()+"\", "+ rsS.resultItemAt(1).asString() +")");
                    targetSession.submitRequest(targetProps);
                }

            }
            LOG.debug(String.format("Doc (%s) exists - getting the MD5 hash", md.getUri()));
            Request targetDocReq = targetSession.newAdhocQuery(Config.MD5_ONELINE.replace("$URI", String.format("\"%s\"", md.getUri())));

            //"fn:doc(\"" + md.getUri() + "\")");
            ResultSequence rsT2 = targetSession.submitRequest(targetDocReq);
            String md5sum = rsT2.asString();
            LOG.debug("MD5 on target: " + md5sum);
            md.setTargetMD5(md5sum);


            if (!md.getUri().equals("/")) {
                documentMap.put(md.getUri(), md);
            }
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
