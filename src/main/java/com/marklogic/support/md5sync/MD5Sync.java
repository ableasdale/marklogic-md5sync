package com.marklogic.support.md5sync;

import com.marklogic.xcc.*;
import com.marklogic.xcc.exceptions.RequestException;
import com.marklogic.xcc.exceptions.XccConfigException;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Map;
import java.util.concurrent.*;

/**
 * MarkLogic MD5Sync
 * <p>
 * Created by ableasdale on 12/07/2017.
 */
public class MD5Sync {

    private static Logger LOG = LoggerFactory.getLogger("com.marklogic.support.md5sync.MD5Sync");
    private static String lastProcessedURI = "/";
    private static String batchQuery = null;
    private static boolean complete = false;
    private static ExecutorService es = Executors.newFixedThreadPool(Config.THREAD_POOL_SIZE);
    private static ExecutorCompletionService<Integer> completionService;
    private static ContentSource csSource = null;
    private static ContentSource csTarget = null;

    private static ResultSequence getBatch(String uri, Session sourceSession) {
        String query = String.format("fn:count(cts:uris( \"%s\", ('limit=10')))", uri);
        LOG.debug(String.format("Query: %s", query));
        Request request = sourceSession.newAdhocQuery(query);
        ResultSequence rs = null;
        try {
            rs = sourceSession.submitRequest(request);
        } catch (RequestException e) {
            e.printStackTrace();
        }
        boolean moreThanOne = (Integer.parseInt(rs.asString()) > 1);

        if (moreThanOne) {
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

            while (!complete) {
                ResultSequence resSeq = getBatch(lastProcessedURI, sourceSession);
                processResultSequence(documentMap, resSeq);
                if(resSeq != null) {resSeq.close();}
            }

            // Stop the thread pool
            es.shutdown();
            // Drain the queue
            while (!es.isTerminated()) {
                // the take() blocks until any of the jobs complete
                // this joins with the jobs in the order they _finish_
                Future<Integer> future = completionService.take();
                // this get() won't block
                int i = future.get();
                LOG.debug(String.format("Future returned: %d", i));
            }

            // TODO - xdmp:estimate on both master and target?

            // When the completion service is done..
            sourceSession.close();
            targetSession.close();

            LOG.info("About to run the report...");
            runFinalReport(documentMap);

        } catch (XccConfigException | RequestException | InterruptedException | ExecutionException e) {
            LOG.error("Exception caught: ", e);
        }
    }

    private static void runFinalReport(Map<String, MarkLogicDocument> documentMap) {
        LOG.info("Generating report");
        for (String s : documentMap.keySet()) {
            MarkLogicDocument m = documentMap.get(s);
            StringBuilder sb = new StringBuilder();
            sb.append("URI:\t").append(Config.ANSI_BLUE).append(m.getUri()).append(Config.ANSI_RESET).append("\tSource MD5:\t").append(m.getSourceMD5());
            if (m.getSourceMD5().equals(m.getTargetMD5())) {
                sb.append("\tTarget MD5:\t").append(Config.ANSI_GREEN).append(m.getTargetMD5()).append(Config.ANSI_RESET);
            } else if (StringUtils.isEmpty(m.getTargetMD5())) {
                sb.append(Config.ANSI_GREEN).append("\tURI synchronised").append(Config.ANSI_RESET);
            } else {
                sb.append("\tTarget MD5:\t").append(Config.ANSI_RED).append(m.getTargetMD5()).append(Config.ANSI_RESET);
            }
            LOG.info(sb.toString());
        }
    }

    private static void processResultSequence(Map<String, MarkLogicDocument> documentMap, ResultSequence rs) throws RequestException {
        if (rs != null) {
            LOG.debug(String.format("Starting with a batch of %d documents", rs.size()));
            Session tS = csTarget.newSession();
            while (rs.hasNext()) {
                ResultItem i = rs.next();

                if (rs.size() <= 1) {
                    LOG.info("Only one item returned - is this the end? " + i.asString());
                }

                if (i.asString().substring(0, i.asString().lastIndexOf("~~~")).equals(lastProcessedURI)) {
                    LOG.debug(String.format("Skipping this URI %s as it has already been processed", lastProcessedURI));
                    rs.next();
                }

                MarkLogicDocument md = new MarkLogicDocument();
                md.setUri(i.asString().substring(0, i.asString().lastIndexOf("~~~")));
                md.setSourceMD5(i.asString().substring(i.asString().lastIndexOf("~~~") + 3));

                // Check target
                Request targetRequest = tS.newAdhocQuery(String.format("fn:doc-available(\"%s\")", md.getUri()));
                ResultSequence rsT = tS.submitRequest(targetRequest);
                LOG.debug(String.format("Is the doc available? %s", rsT.asString()));

                if (rsT.asString().equals("false")) {
                    if (md.getUri().equals("/")) {
                        LOG.info(String.format("Don't need to replicate an empty directory node: %s", md.getUri()));
                    } else {
                        LOG.debug("Doc not available in destination: " + md.getUri());
                        completionService.submit(new DocumentCopier(md));
                    }
                } else {
                    LOG.debug(String.format("Doc (%s) exists - getting the MD5 hash", md.getUri()));
                    Request targetDocReq = tS.newAdhocQuery(Config.MD5_ONELINE.replace("$URI", String.format("\"%s\"", md.getUri())));

                    ResultSequence rsT2 = tS.submitRequest(targetDocReq);
                    String md5sum = rsT2.asString();
                    LOG.debug(String.format("MD5 on target: %s MD5 on source: %s", md5sum, md.getSourceMD5()));
                    md.setTargetMD5(md5sum);
                    rsT2.close();

                    // Sychronise if the hashes don't match
                    if (!md.getTargetMD5().equals(md.getSourceMD5()) && !md.getUri().equals("/")) {
                        LOG.debug(String.format("MD5 hashes do not match for %s - copying document over", md.getUri()));
                        completionService.submit(new DocumentCopier(md));
                    }
                }
                rsT.close();
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
            LOG.debug("working on: " + md.getUri());
            this.md = md;
        }

        int writeDocument() throws Exception {
            LOG.debug("Writing Document " + md.getUri());
            Session s = csSource.newSession();
            Session t = csTarget.newSession();

            LOG.debug(String.format("We need to copy this doc (%s) over", md.getUri()));
            Request sourceDocReq = s.newAdhocQuery(String.format("(fn:doc(\"%s\"), xdmp:document-properties(\"%s\")/prop:properties/*, (string-join(xdmp:document-get-collections(\"%s\"),'~')))", md.getUri(), md.getUri(), md.getUri()));
            ResultSequence rsS = s.submitRequest(sourceDocReq);
            LOG.debug("Collection size: " + rsS.size());
            // TODO - collections, properties, permissions etc... ?
            ContentCreateOptions co = ContentCreateOptions.newXmlInstance();
            co.setCollections(rsS.resultItemAt(2).asString().split("~"));
            // TODO - not all operations - such as perms - are in this routine
            //co.setMetadata();
            //co.setPermissions();

            Content content = ContentFactory.newContent(md.getUri(), rsS.resultItemAt(0).asString(), co);
            t.insertContent(content);
            LOG.debug("xdmp:document-set-properties(\"" + md.getUri() + "\", " + rsS.resultItemAt(1).asString() + ")");

            Request targetProps = t.newAdhocQuery("xdmp:document-set-properties(\"" + md.getUri() + "\", " + rsS.resultItemAt(1).asString() + ")");
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
