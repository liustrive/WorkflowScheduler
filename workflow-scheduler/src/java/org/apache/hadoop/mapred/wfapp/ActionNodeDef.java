package org.apache.hadoop.mapred.wfapp;

import java.util.Arrays;

/**
 * Node definition for workflow action. This node definition is serialized object and should provide
 * readFields() and write() for read and write of fields in this class.  
 *
 */
public class ActionNodeDef extends NodeDef {

    ActionNodeDef() {
    }

    public ActionNodeDef(String name, String conf, Class<? extends ActionNodeHandler> actionHandlerClass, String onOk,
                         String onError) {
        super(name, ParamChecker.notNull(conf, "conf"), actionHandlerClass, Arrays.asList(onOk, onError));
    }
    
    public ActionNodeDef(String name, String conf, Class<? extends ActionNodeHandler> actionHandlerClass, String onOk,
            String onError, String cred) {
        super(name, ParamChecker.notNull(conf, "conf"), actionHandlerClass, Arrays.asList(onOk, onError), cred);
    }
    
    public ActionNodeDef(String name, String conf, Class<? extends ActionNodeHandler> actionHandlerClass, String onOk,
            String onError, String cred, String userRetryMax, String userRetryInterval) {
        super(name, ParamChecker.notNull(conf, "conf"), actionHandlerClass, Arrays.asList(onOk, onError), cred, userRetryMax, userRetryInterval);
    }
}