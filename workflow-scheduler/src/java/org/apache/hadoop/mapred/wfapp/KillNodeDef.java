package org.apache.hadoop.mapred.wfapp;

import java.util.Collections;

/**
 * Node definition for KILL control node.
 */
public class KillNodeDef extends ControlNodeDef {

    KillNodeDef() {
    }

    @SuppressWarnings("unchecked")
    public KillNodeDef(String name, String message, Class<? extends ControlNodeHandler> klass) {
        super(name, message, klass, Collections.EMPTY_LIST);
    }

    public static class KillNodeHandler extends NodeHandler {

        public boolean enter(Context context) {
            return true;
        }

        public String exit(Context context) {
            context.killJob();
            return null;
        }

        public void kill(Context context) {
        }

        public void fail(Context context) {
        }
    }

}