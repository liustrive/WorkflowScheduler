package org.apache.hadoop.mapred.wfapp;
import java.util.Collections;

/**
 * Node definition for END control node.
 */
public class EndNodeDef extends ControlNodeDef {

    EndNodeDef() {
    }

    @SuppressWarnings("unchecked")
    public EndNodeDef(String name, Class<? extends ControlNodeHandler> klass) {
        super(name, "", klass, Collections.EMPTY_LIST);
    }

    public static class EndNodeHandler extends NodeHandler {

        public boolean enter(Context context) {
            return true;
        }

        public String exit(Context context) {
            context.completeJob();
            return null;
        }

        public void kill(Context context) {
        }

        public void fail(Context context) {
        }

    }

}
