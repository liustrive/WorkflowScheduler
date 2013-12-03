package org.apache.hadoop.mapred.wfapp;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * This node definition is serialized object and should provide readFields() and write() for read and write of fields in
 * this class.
 */
public class NodeDef implements Writable {
    public static final String NODE_DEF_VERSION_0 = "_oozie_inst_v_0";
    public static final String NODE_DEF_VERSION_1 = "_oozie_inst_v_1";
    private String nodeDefVersion = null;
    private String name = null;
    private Class<? extends NodeHandler> handlerClass;
    private String conf = null;
    private List<String> transitions = new ArrayList<String>();
    private String cred = "null";
    private String userRetryMax = "null";
    private String userRetryInterval = "null";
    private int rank;
    private int jobId;
    private boolean started = false;
    private boolean finished = false;
 

    NodeDef() {
    }
    
    NodeDef(String name, String conf, Class<? extends NodeHandler> handlerClass, List<String> transitions) {
        this.name = ParamChecker.notEmpty(name, "name");
        this.conf = conf;
        this.handlerClass = ParamChecker.notNull(handlerClass, "handlerClass");
        this.transitions = Collections.unmodifiableList(ParamChecker.notEmptyElements(transitions, "transitions"));
    }

    NodeDef(String name, String conf, Class<? extends NodeHandler> handlerClass, List<String> transitions, String cred) {
        this(name, conf, handlerClass, transitions);
        if (cred != null) {
            this.cred = cred;
        }
    }

    NodeDef(String name, String conf, Class<? extends NodeHandler> handlerClass, List<String> transitions, String cred,
            String userRetryMax, String userRetryInterval) {
        this(name, conf, handlerClass, transitions, cred);
        if (userRetryMax != null) {
            this.userRetryMax = userRetryMax;
        }
        if (userRetryInterval != null) {
            this.userRetryInterval = userRetryInterval;
        }
    }
    public boolean isStarted(){
    	return started;
    }
    public boolean isFinished(){
    	return finished;
    }
    public void start(){
    	started = true;
    }
    public void finish(){
    	finished = true;
    }
    public boolean equals(NodeDef other) {
        return !(other == null || getClass() != other.getClass() || !getName().equals(other.getName()));
    }

    @Override
    public int hashCode() {
        return name.hashCode();
    }

    public String getName() {
        return name;
    }

    public String getCred() {
        return cred;
    }

    public Class<? extends NodeHandler> getHandlerClass() {
        return handlerClass;
    }

    public List<String> getTransitions() {
        return transitions;
    }

    public String getConf() {
        return conf;
    }

    public String getUserRetryMax() {
        return userRetryMax;
    }

    public String getUserRetryInterval() {
        return userRetryInterval;
    }
    public int getRank(){
    	return rank;
    }
    public void setRank(int r){
    	rank = r;
    }
    public int getJobId(){
    	return jobId;
    }
    public void setJobId(int id){
    	jobId = id;
    }
    
//    public String getNodeDefDefaultVersion()  {
//        Configuration conf = Services.get().get(ConfigurationService.class).getConf();
//        String ret = conf.get(CONF_NODE_DEF_VERSION);
//        if (ret == null) {
//            ret = NODE_DEF_VERSION_1;
//        }
//        return ret;
//    }
//    public String getNodeDefVersion() {
//        if (nodeDefVersion == null) {
//            try {
//                nodeDefVersion = LiteWorkflowStoreService.getNodeDefDefaultVersion();
//            }
//            catch (WorkflowException e) {
//                nodeDefVersion = LiteWorkflowStoreService.NODE_DEF_VERSION_1;
//            }
//        }
//        return nodeDefVersion;
//    }

    @SuppressWarnings("unchecked")
    private void readVersionZero(DataInput dataInput, String firstField) throws IOException {
        if (firstField.equals(NODE_DEF_VERSION_0)) {
            name = dataInput.readUTF();
        } else {
            name = firstField;
        }
        nodeDefVersion = NODE_DEF_VERSION_0;
        cred = dataInput.readUTF();
        String handlerClassName = dataInput.readUTF();
        if ((handlerClassName != null) && (handlerClassName.length() > 0)) {
            try {
                handlerClass = (Class<? extends NodeHandler>) Class.forName(handlerClassName);
            }
            catch (ClassNotFoundException ex) {
                throw new IOException(ex);
            }
        }
        conf = dataInput.readUTF();
        if (conf.equals("null")) {
            conf = null;
        }
        int numTrans = dataInput.readInt();
        transitions = new ArrayList<String>(numTrans);
        for (int i = 0; i < numTrans; i++) {
            transitions.add(dataInput.readUTF());
        }
    }
    @SuppressWarnings("unchecked")
    private void readVersionOne(DataInput dataInput, String firstField) throws IOException {
        nodeDefVersion = NODE_DEF_VERSION_1;
        name = dataInput.readUTF();
        cred = dataInput.readUTF();
        String handlerClassName = dataInput.readUTF();
        if ((handlerClassName != null) && (handlerClassName.length() > 0)) {
            try {
                handlerClass = (Class<? extends NodeHandler>) Class.forName(handlerClassName);
            }
            catch (ClassNotFoundException ex) {
                throw new IOException(ex);
            }
        }
        conf = dataInput.readUTF();
        if (conf.equals("null")) {
            conf = null;
        }
        int numTrans = dataInput.readInt();
        transitions = new ArrayList<String>(numTrans);
        for (int i = 0; i < numTrans; i++) {
            transitions.add(dataInput.readUTF());
        }
        userRetryMax = dataInput.readUTF();
        userRetryInterval = dataInput.readUTF();
    }

    /* (non-Javadoc)
     * @see org.apache.hadoop.io.Writable#readFields(java.io.DataInput)
     */
    @Override
    public void readFields(DataInput dataInput) throws IOException {
        String firstField = dataInput.readUTF();
        if (!firstField.equals(NODE_DEF_VERSION_1)) {
            readVersionZero(dataInput, firstField);
        } else {
            //since oozie version 3.1
            readVersionOne(dataInput, firstField);
        }
    }

    private void writeVersionZero(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(nodeDefVersion);
        dataOutput.writeUTF(name);
        if (cred != null) {
            dataOutput.writeUTF(cred);
        }
        else {
            dataOutput.writeUTF("null");
        }
        dataOutput.writeUTF(handlerClass.getName());
        if (conf != null) {
            dataOutput.writeUTF(conf);
        }
        else {
            dataOutput.writeUTF("null");
        }
        dataOutput.writeInt(transitions.size());
        for (String transition : transitions) {
            dataOutput.writeUTF(transition);
        }
    }

    /**
     * Write as version one format, this version was since 3.1.
     *
     * @param dataOutput data output to serialize node def
     * @throws IOException thrown if fail to write
     */
    private void writeVersionOne(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(nodeDefVersion);
        dataOutput.writeUTF(name);
        if (cred != null) {
            dataOutput.writeUTF(cred);
        }
        else {
            dataOutput.writeUTF("null");
        }
        dataOutput.writeUTF(handlerClass.getName());
        if (conf != null) {
            dataOutput.writeUTF(conf);
        }
        else {
            dataOutput.writeUTF("null");
        }
        dataOutput.writeInt(transitions.size());
        for (String transition : transitions) {
            dataOutput.writeUTF(transition);
        }
        if (userRetryMax != null) {
            dataOutput.writeUTF(userRetryMax);
        }
        else {
            dataOutput.writeUTF("null");
        }
        if (userRetryInterval != null) {
            dataOutput.writeUTF(userRetryInterval);
        }
        else {
            dataOutput.writeUTF("null");
        }
    }

    /* (non-Javadoc)
     * @see org.apache.hadoop.io.Writable#write(java.io.DataOutput)
     */
    @Override
    public void write(DataOutput dataOutput) throws IOException {
////        if (!getNodeDefVersion().equals(NODE_DEF_VERSION_1)) {
//            writeVersionZero(dataOutput);
////        } else {
////            //since oozie version 3.1
////            writeVersionOne(dataOutput);
////        }
    	String newLine = System.getProperty("line.separator");
    	dataOutput.writeBytes(name+newLine);
    	dataOutput.writeBytes("started: " + started+newLine);
    	dataOutput.writeBytes("finished: "+ finished+newLine);
    	dataOutput.writeBytes("transitions: "+ transitions.size()+newLine);
    	for(String transition : transitions){
    		dataOutput.writeBytes(transition+newLine);
    	}
    }

}
