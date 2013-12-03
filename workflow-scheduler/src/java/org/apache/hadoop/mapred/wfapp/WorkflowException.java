package org.apache.hadoop.mapred.wfapp;

public class WorkflowException extends Exception {

    /**
     * Create an workflow exception from a XException.
     *
     * @param cause the XException cause.
     */
    public WorkflowException(Exception cause) {
        super(cause);
    }

    /**
     * Create a workflow exception.
     *
     * @param errorCode error code.
     * @param params parameters for the error code message template.
     */
    public WorkflowException(ErrorCode errorCode, Object... params) {
        super(errorCode.getTemplate()+" " + params);
    }

}