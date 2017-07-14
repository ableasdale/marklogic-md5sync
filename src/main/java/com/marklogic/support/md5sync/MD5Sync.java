package com.marklogic.support.md5sync;

import com.marklogic.xcc.*;
import com.marklogic.xcc.exceptions.RequestException;
import com.marklogic.xcc.exceptions.XccConfigException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Map;
import java.util.concurrent.*;

/**
 * MD5Sync
 *
 * Created by ableasdale on 12/07/2017.
 */
public class MD5Sync {

    private static Logger LOG = LoggerFactory.getLogger("com.marklogic.support.md5sync.MD5Sync");
    private static String lastProcessedURI = null;
    private static String batchQuery = null;
    private static boolean complete = false;
    private static ExecutorService es = Executors.newFixedThreadPool(Config.THREAD_POOL_SIZE);
    private static ExecutorCompletionService<Integer> completionService;
    private static ContentSource csSource = null;
    private static ContentSource csTarget = null;

    private static ResultSequence getBatch(String uri, Session sourceSession) {
        String query = "fn:count(cts:uris( \""+uri+"\", ('limit=10')))";
        LOG.debug("Query: "+query);
        Request request = sourceSession.newAdhocQuery(query);
        ResultSequence rs = null;
        try {
            rs = sourceSession.submitRequest(request);
        } catch (RequestException e) {
            e.printStackTrace();
        }
        boolean moreThanOne = (Integer.parseInt(rs.asString()) > 1);

        if(moreThanOne) {
            request = sourceSession.newAdhocQuery(batchQuery.replace("(),", String.format("\"%s\",", uri)));
            try {
                rs = sourceSession.submitRequest(request);
            } catch (RequestException e) {
                e.printStackTrace();
            }
        } else {
            // Down to last item, so close the result sequence
            complete = true;
            rs.close();
            rs = null;
        }
        return rs;
    }

    public static void main(String[] args) {
        Map<String, MarkLogicDocument> documentMap = new ConcurrentHashMap<>();
        completionService = new ExecutorCompletionService<>(es);

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
            processResultSequence(documentMap, sourceSession, targetSession, rs);
            // LOG.debug(String.format("Sequence size: %s%d%s", Config.ANSI_GREEN, rs.size(), Config.ANSI_RESET));
            rs.close();

            while(!complete) {
                rs = getBatch(lastProcessedURI, sourceSession);
                processResultSequence(documentMap, sourceSession, targetSession, rs);
            }

            sourceSession.close();
            targetSession.close();

            runFinalReport(documentMap);

        } catch (XccConfigException | RequestException e) {
            LOG.error("Exception caught: ", e);
        }
    }

    private static void runFinalReport(Map<String, MarkLogicDocument> documentMap) {
        LOG.info("Generating report");
        // TODO - output to file?
        // TODO - fails if the copy just took place as part of the run.
        for (String s : documentMap.keySet()) {
            MarkLogicDocument m = documentMap.get(s);
            StringBuilder sb = new StringBuilder();
            sb.append("URI:\t").append(Config.ANSI_BLUE).append(m.getUri()).append(Config.ANSI_RESET).append("\tSource MD5:\t").append(m.getSourceMD5());
            if (m.getSourceMD5().equals(m.getTargetMD5())) {
                sb.append("\tTarget MD5:\t").append(Config.ANSI_GREEN).append(m.getTargetMD5()).append(Config.ANSI_RESET);
                LOG.info(sb.toString());
            } else {
                sb.append("\tTarget MD5:\t").append(Config.ANSI_RED).append(m.getTargetMD5()).append(Config.ANSI_RESET);
                LOG.info(sb.toString());
            }
        }
    }

    private static void processResultSequence(Map<String, MarkLogicDocument> documentMap, Session sourceSession, Session targetSession, ResultSequence rs) throws RequestException {
        if(rs != null) {
            LOG.debug(String.format("Starting with a batch of %d documents", rs.size()));

            while (rs.hasNext()) {
                ResultItem i = rs.next();

                if (rs.size() <= 1) {
                    LOG.info("Only one item returned - is this the end? " + i.asString());
                }

                MarkLogicDocument md = new MarkLogicDocument();
                md.setUri(i.asString().substring(0, i.asString().lastIndexOf("~~~")));
                md.setSourceMD5(i.asString().substring(i.asString().lastIndexOf("~~~") + 3));

                // Check target
                Request targetRequest = targetSession.newAdhocQuery(String.format("fn:doc-available(\"%s\")", md.getUri()));
                ResultSequence rsT = targetSession.submitRequest(targetRequest);
                LOG.debug("Is the doc available? " + rsT.asString());

                if (rsT.asString().equals("false")) {
                    if (md.getUri().equals("/")) {
                        LOG.debug("Don't need to replicate an empty directory node: " + md.getUri());
                    } else {
                        LOG.debug("Doc not available in destination: " + md.getUri());
                        completionService.submit(new DocumentCopier(md));
                    }
                } else {
                    LOG.debug(String.format("Doc (%s) exists - getting the MD5 hash", md.getUri()));
                    Request targetDocReq = targetSession.newAdhocQuery(Config.MD5_ONELINE.replace("$URI", String.format("\"%s\"", md.getUri())));

                    ResultSequence rsT2 = targetSession.submitRequest(targetDocReq);
                    String md5sum = rsT2.asString();
                    LOG.debug(String.format("MD5 on target: %s MD5 on source: %s", md5sum, md.getSourceMD5()));
                    md.setTargetMD5(md5sum);

                    // Sychronise if the hashes don't match
                    if (!md.getTargetMD5().equals(md.getSourceMD5()) && !md.getUri().equals("/")) {
                        LOG.debug("MD5 hashes do not match for " + md.getUri() + " - copying document over");
                        completionService.submit(new DocumentCopier(md));
                    }
                }
                if (!md.getUri().equals("/")) {
                    documentMap.put(md.getUri(), md);
                }

                if (!rs.hasNext()) {
                    LOG.info(String.format("Last URI in batch of %s URI(s): %s%s%s", rs.size(), Config.ANSI_BLUE, md.getUri(), Config.ANSI_RESET));
                    lastProcessedURI = md.getUri();
                }
            }
        }

    }


    public static class DocumentCopier implements Callable {

        private MarkLogicDocument md;

        DocumentCopier(MarkLogicDocument md) {
            LOG.debug("working on: "+md.getUri());
            this.md = md;
        }

        int writeDocument() throws Exception {
            LOG.debug("Writing Document "+md.getUri());
            Session s = csSource.newSession();
            Session t = csTarget.newSession();

            LOG.debug(String.format("We need to copy this doc (%s) over", md.getUri()));
            Request sourceDocReq = s.newAdhocQuery(String.format("(fn:doc(\"%s\"), xdmp:document-properties(\"%s\")/prop:properties/*, (string-join(xdmp:document-get-collections(\"%s\"),'~')))", md.getUri(), md.getUri(), md.getUri()));
            ResultSequence rsS = s.submitRequest(sourceDocReq);
            LOG.debug("Collection size: " +rsS.size());
            // TODO - collections, properties, permissions etc... ?
            ContentCreateOptions co = ContentCreateOptions.newXmlInstance();
            co.setCollections(rsS.resultItemAt(2).asString().split("~"));
            // TODO - not all operations - such as perms - are in this routine
            //co.setMetadata();
            //co.setPermissions();

            Content content = ContentFactory.newContent(md.getUri(), rsS.resultItemAt(0).asString(), co);
            t.insertContent(content);
            LOG.debug("xdmp:document-set-properties(\""+md.getUri()+"\", "+ rsS.resultItemAt(1).asString() +")");

            Request targetProps = t.newAdhocQuery("xdmp:document-set-properties(\""+md.getUri()+"\", "+ rsS.resultItemAt(1).asString() +")");
            t.submitRequest(targetProps);

            s.close();
            t.close();

            return 1;
        }

        public Object call() throws Exception {
            return writeDocument();
        }
    }

}
