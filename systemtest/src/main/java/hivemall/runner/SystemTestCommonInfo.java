package hivemall.runner;

public class SystemTestCommonInfo {
    public final String baseDir;
    public final String caseDir;
    public final String answerDir;
    public final String initDir;
    public final String dbName;


    public SystemTestCommonInfo(Class clazz) {
        baseDir = clazz.getName().replace(".", "/");
        caseDir = baseDir + "/case/";
        answerDir = baseDir + "/answer/";
        initDir = baseDir + "/init/";
        dbName = clazz.getName().replace(".", "_");
    }
}
