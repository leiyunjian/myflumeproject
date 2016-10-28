package source;

/**
 * Created by Lei on 2016/10/9.
 */
public class FlumeContext {
	//用于存储所有采集完成的文件 文件内容包括文件名和采集的行数
	public static final String File_finishFile = "E:\\test\\flume-test\\finish.txt";
	//用于存储对采集目录排序后找出的最大时间对应文件的文件名
	public static final String _lastModifiedFile = "E:\\test\\flume-test\\_lastModifiedFile.txt";
	//用于配置采集目录
	public static final String Collect_Dir = "E:\\test\\test";
	//用以实时存储采集的进度及采集到 哪一个文件第几行记录
	public static final String File_RecordPosition = "E:\\test\\flume-test\\File_RecordPosition.txt";
	//设置记录的长度用以删选合适的记录
	public static final int RECORD_LENGTH = 15;
	//用以存储 程序采集的开始文件位置：文件内容包括一个文件名，程序将从该文件开始向下采集
	public static final String Set_StartPoint = "E:\\test\\flume-test\\Set_StartPoint";
}
