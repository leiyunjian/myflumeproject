package custom_source;

/**
 * Created by Lei on 2016/12/7.
 */
public class FlumeContext {
	//用于存储所有采集完成的文件 文件内容包括文件名和采集的行数
	public static final String File_finishFile = "./finish.txt";
	//用以实时存储采集的进度及采集到 哪一个文件第几行记录
	public static final String File_RecordPosition = "./File_RecordPosition.txt";
	//每一次向channel推送event的数量
	public static  final int DEFULT_BATCH_SIZE = 30;
	//采集文件的格式默认正则表达式
	public static  final String DEFULT_COLLECT_FILE_FARMAT = ".*";

}
