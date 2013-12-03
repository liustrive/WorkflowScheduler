package org.apache.hadoop.mapred.wfapp;
public enum ErrorCode {
    E0000( "System property 'oozie.home.dir' not defined"),
    E0001( "Could not create runtime directory, {0}"),
    E0002( "System is in safe mode"),
    E0003( "Oozie home directory must be an absolute path [{0}]"),
    E0004( "Oozie home directory does not exist [{0}]"),

    E0010( "Could not initialize log service, {0}"),
    E0011( "Log4j file must be a file name [{0}]"),
    E0012( "Log4j file must be a '.properties' file [{0}]"),
    E0013( "Log4j file [{0}] not found in configuration dir [{1}] neither in classpath"),

    E0020( "Environment variable {0} not defined"),
    E0022( "Configuration file must be a file name [{0}]"),
    E0023( "Default configuration file [{0}] not found in classpath"),
    E0024( "Oozie configuration directory does not exist [{0}]"),
    E0025( "Configuration service internal error, it should never happen, {0}"),
    E0026( "Missing required configuration property [{0}]"),

    E0100( "Could not initialize service [{0}], {1}"),
    E0101( "Service [{0}] does not implement declared interface [{1}]"),
    E0102( "Could not instantiate service class [{0}], {1}"),
    E0103( "Could not load service classes, {0}"),
    E0110( "Could not parse or validate EL definition [{0}], {1}"),
    E0111( "class#method not found [{0}#{1}]"),
    E0112( "class#method does not have PUBLIC or STATIC modifier [{0}#{1}]"),
    E0113( "class not found [{0}]"),
    E0114( "class#constant does not have PUBLIC or STATIC modifier [{0}#{1}]"),
    E0115( "class#constant not found"),
    E0116( "class#constant does not have PUBLIC or STATIC modifier [{0}#{1}]"),
    E0120( "UUID, invalid generator type [{0}]"),
    E0130( "Could not parse workflow schemas file/s, {0}"),
    E0131( "Could not read workflow schemas file/s, {0}"),
    E0140( "Could not access database, {0}"),
    E0141( "Could not create DataSource connection pool, {0}"),
    E0150( "Actionexecutor type already registered [{0}]"),
    E0160( "Could not read admin users file [{0}], {1}"),

    E0300( "Invalid content-type [{0}]"),
    E0301( "Invalid resource [{0}]"),
    E0302( "Invalid parameter [{0}]"),
    E0303( "Invalid parameter value, [{0}] = [{1}]"),
    E0304( "Invalid parameter type, parameter [{0}] expected type [{1}]"),
    E0305( "Missing parameter [{0}]"),
    E0306( "Invalid parameter"),
    E0307( "Runtime error [{0}]"),
    E0308( "Could not parse date range parameter [{0}]"),


    E0401( "Missing configuration property [{0}]"),
    E0402( "Invalid callback ID [{0}]"),
    E0403( "Invalid callback data, {0}"),
    E0404( "Only one of the properties are allowed [{0}]"),
    E0405( "Submission request doesn't have any application or lib path"),

    E0420( "Invalid jobs filter [{0}], {1}"),
    E0421( "Invalid job filter [{0}], {1}"),

    E0500( "Not authorized, {0}"),
    E0501( "Could not perform authorization operation, {0}"),
    E0502( "User [{0}] does not belong to group [{1}]"),
    E0503( "User [{0}] does not have admin privileges"),
    E0504( "App directory [{0}] does not exist"),
    E0505( "App definition [{0}] does not exist"),
    E0506( "App definition [{0}] is not a file"),
    E0507( "Could not access to [{0}], {1}"),
    E0508( "User [{0}] not authorized for WF job [{1}]"),
    E0509( "User [{0}] not authorized for Coord job [{1}]"),
    E0510( "Unable to get Credential [{0}]"),

    E0550( "Could not normalize host name [{0}], {1}"),
    E0551( "Missing [{0}] property"),

    E0600( "Could not get connection, {0}"),
    E0601( "Could not close connection, {0}"),
    E0602( "Could not commit connection, {0}"),
    E0603( "SQL error in operation, {0}"),
    E0604( "Job does not exist [{0}]"),
    E0605( "Action does not exist [{0}]"),
    E0606( "Could not get lock [{0}], timed out [{1}]ms"),
    E0607( "Other error in operation [{0}], {1}"),
    E0608( "JDBC setup error [{0}], {1}"),
    E0609( "Missing [{0}] ORM file [{1}]"),
    E0610( "Missing JPAService, StoreService cannot run without a JPAService"),
    E0611( "SQL error in operation [{0}], {1}"),

    E0700( "XML error, {0}"),
    E0701( "XML schema error, {0}"),
    E0702( "IO error, {0}"),
    E0703( "Invalid workflow element [{0}]"),
    E0704( "Definition already complete, application [{0}]"),
    E0705( "Nnode already defined, node [{0}]"),
    E0706( "Node cannot transition to itself node [{0}]"),
    E0707( "Loop detected at parsing, node [{0}]"),
    E0708( "Invalid transition, node [{0}] transition [{1}]"),
    E0709( "Loop detected at runtime, node [{0}]"),
    E0710( "Could not read the workflow definition, {0}"),
    E0711( "Invalid application URI [{0}], {1}"),
    E0712( "Could not create lib paths list for application [{0}], {1}"),
    E0713( "SQL error, {0}"),
    E0714( "Workflow lib error, {0}"),
    E0715( "Invalid signal value for the start node, signal [{0}]"),
    E0716( "Workflow not running"),
    E0717( "Workflow not suspended"),
    E0718( "Workflow already completed"),
    E0719( "Job already started"),
    E0720( "Fork/join mismatch, node [{0}]"),
    E0721( "Invalid signal/transition, decision node [{0}] signal [{1}]"),
    E0722( "Action signals can only be OK or ERROR, action node [{0}]"),
    E0723( "Unsupported action type, node [{0}] type [{1}]"),
    E0724( "Invalid node name, {0}"),
    E0725( "Workflow instance can not be killed, {0}"),
    E0726( "Workflow action can not be killed, {0}"),
    E0727( "Workflow Job can not be suspended as its not in running state, {0}, Status: {1}"),
    E0728( "Coordinator Job can not be suspended as job finished or failed or killed, id : {0}, status : {1}"),
    E0729( "Kill node message [{0}]"),
    E0730( "Fork/Join not in pair"),
    E0731( "Fork node [{0}] cannot have less than two paths"),
    E0732( "Fork [{0}]/Join [{1}] not in pair (join should have been [{2}])"),
    E0733( "Fork [{0}] without a join"),
    E0734( "Invalid transition from node [{0}] to node [{1}] while using fork/join"),
    E0735( "There was an invalid \"error to\" transition to node [{1}] while using fork/join"),
    E0736( "Workflow definition length [{0}] exceeded maximum allowed length [{1}]"),
    E0737( "Invalid transition from node [{0}] to node [{1}] -- nodes of type 'end' are not allowed within Fork/Join"),
    E0738("The following {0} parameters are required but were not defined and no default values are available: {1}"),
    E0739( "Parameter name cannot be empty"),
    E0740( "Invalid node type encountered (node [{0}])"),
    E0741( "Cycle detected transitioning to [{0}] via path {1}"),
    E0742( "No Fork for Join [{0}] to pair with"),
    E0743( "Multiple \"ok to\" transitions to the same node, [{0}], are not allowed"),
    E0744( "A fork, [{0}], is not allowed to have multiple transitions to the same node, [{1}]"),

    E0800( "Action it is not running its in [{1}] state, action [{0}]"),
    E0801( "Workflow already running, workflow [{0}]"),
    E0802( "Invalid action type [{0}]"),
    E0803( "IO error, {0}"),
    E0804( "Disallowed default property [{0}]"),
    E0805("Workflow job not completed, status [{0}]"),
    E0806( "Action did not complete in previous run, action [{0}]"),
    E0807( "Some skip actions were not executed [{0}]"),
    E0808( "Disallowed user property [{0}]"),
    E0809( "Workflow action is not eligible to start [{0}]"),
    E0810( "Job state is not [{0}]. Skipping ActionStart Execution"),
    E0811( "Job state is not [{0}]. Skipping ActionEnd Execution"),
    E0812("Action pending=[{0}], status=[{1}]. Skipping ActionEnd Execution"),
    E0813( "Workflow not RUNNING, current status [{0}]"),
    E0814( "SignalCommand for action id=[{0}] is already processed, status=[{1}], , pending=[{2}]"),
    E0815( "Action pending=[{0}], status=[{1}]. Skipping ActionCheck Execution"),
    E0816("Action pending=[{0}], status=[{1}]. Skipping ActionStart Execution"),
    E0817("The wf action [{0}] has been udated recently. Ignoring ActionCheck."),
    E0818( "Action [{0}] status is running but WF Job [{1}] status is [{2}]. Expected status is RUNNING."),
    E0819( "Unable to delete the temp dir of job WF Job [{0}]."),
    E0820( "Action user retry max [{0}] is over system defined max [{1}], re-assign to use system max."),

    E0900( "Jobtracker [{0}] not allowed, not in Oozie's whitelist"),
    E0901( "Namenode [{0}] not allowed, not in Oozie's whitelist"),
    E0902( "Exception occured: [{0}]"),
    E0903( "Invalid JobConf, it has not been created by HadoopAccessorService"),
    E0904( "Scheme [{0}] not supported in uri [{1}]"),
    E0905( "Scheme not present in uri [{0}]"),
    E0906( "URI parsing error : {0}"),

    E1001( "Could not read the coordinator job definition, {0}"),
    E1002( "Invalid coordinator application URI [{0}], {1}"),
    E1003( "Invalid coordinator application attributes, {0}"),
    E1004( "Expression language evaluation error, {0}"),
    E1005( "Could not read the coordinator job configuration read from DB, {0}"),
    E1006( "Invalid coordinator application [{0}], {1}"),
    E1007( "Unable to add record for SLA. [{0}], {1}"),
    E1008( "Not implemented. [{0}]"),
    E1009( "Unable to parse XML response. [{0}]"),
    E1010( "Invalid data in coordinator xml. [{0}]"),
    E1011( "Cannot update coordinator job [{0}], {1}"),
    E1012( "Coord Job Materialization Error: {0}"),
    E1013( "Coord Job Recovery Error: {0}"),
    E1014("Coord job change command not supported"),
    E1015( "Invalid coordinator job change value {0}, {1}"),
    E1016( "Cannot change a killed coordinator job"),
    E1017( "Cannot change a workflow job"),
    E1018( "Coord Job Rerun Error: {0}"),
    E1019( "Could not submit coord job, [{0}]"),
    E1020( "Could not kill coord job, this job either finished successfully or does not exist , [{0}]"),
    E1021( "Coord Action Input Check Error: {0}"),
    E1022( "Cannot delete running/completed coordinator action: [{0}]"),

    E1100( "Command precondition does not hold before execution, [{0}]"),

    E1101( "SLA <{0}> cannot be empty."),

    E1201( "State [{0}] is invalid for job [{1}]."),

    E1301( "Could not read the bundle job definition, [{0}]"),
    E1302( "Invalid bundle application URI [{0}], {1}"),
    E1303( "Invalid bundle application attributes [{0}], {1}"),
    E1304( "Duplicate bundle application coordinator name [{0}]"),
    E1305( "Empty bundle application coordinator name."),
    E1306( "Could not read the bundle job configuration, [{0}]"),
    E1307( "Could not read the bundle coord job configuration, [{0}]"),
    E1308( "Bundle Action Status  [{0}] is not matching with coordinator previous status [{1}]."),
    E1309( "Bundle Action for bundle ID  [{0}] and Coordinator [{1}] could not be update by BundleStatusUpdateXCommand"),
    E1310( "Bundle Job submission Error: [{0}]"),
    E1311("Bundle Action for bundle ID  [{0}] not found"),
    E1312( "Bundle Job can not be suspended as job finished or failed or killed, id : {0}, status : {1}"),
    E1313( "Bundle Job can not be changed as job finished, {0}, Status: {1}"),
    E1314( "Bundle Job can not be changed as job does not exist, {0}"),
    E1315( "Bundle job can not be paused, {0}"),
    E1316( "Bundle job can not be unpaused, {0}"),
    E1317( "Invalid bundle job change value {0}, {1}"),
    E1318( "No coord jobs for the bundle=[{0}], fail the bundle"),
    E1319( "Invalid bundle coord job namespace, [{0}]"),

    E1400( "doAs (proxyuser) failure"),

    E1501( "Error in getting HCat Access [{0}]"),

    E1601( "Cannot retrieve JMS connection info [{0}]"),
    E1602( "Cannot retrieve Topic name [{0}]"),

    E1700( "Issue communicating with ZooKeeper: {0}"),
    ETEST( "THIS SHOULD HAPPEN ONLY IN TESTING, invalid job id [{0}]"),;

    private String template;
    private int logMask;

    /**
     * Create an error code.
     *
     * @param template template for the exception message.
     * @param logMask log mask for the exception.
     */
    private ErrorCode( String template) {
        this.template = template;
    }

    /**
     * Return the message (StringFormat) template for the error code.
     *
     * @return message template.
     */
    public String getTemplate() {
        return template;
    }

//    /**
//     * Return the log mask (to which log the exception should be logged) of the error.
//     *
//     * @return error log mask.
//     */
//    public int getLogMask() {
//        return logMask;
//    }

//    /**
//     * Return a templatized error message for the error code.
//     *
//     * @param args the parameters for the templatized message.
//     * @return error message.
//     */
//    public String format(Object... args) {
//        return XLog.format("{0}: {1}", toString(), XLog.format(getTemplate(), args));
//    }

}