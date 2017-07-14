package com.marklogic.support.md5sync;

/**
 * Created by ableasdale on 12/07/2017.
 */
public class MarkLogicDocument {

    private String uri;
    private String sourceMD5;
    private String targetMD5;

    public String getUri() {
        return uri;
    }

    public void setUri(String uri) {
        this.uri = uri;
    }

    public String getSourceMD5() {
        return sourceMD5;
    }

    public void setSourceMD5(String sourceMD5) {
        this.sourceMD5 = sourceMD5;
    }

    public String getTargetMD5() {
        return targetMD5;
    }

    public void setTargetMD5(String targetMD5) {
        this.targetMD5 = targetMD5;
    }
}
