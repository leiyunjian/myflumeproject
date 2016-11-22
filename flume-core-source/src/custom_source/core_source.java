package custom_source;

import org.apache.flume.Context;
import org.apache.flume.EventDrivenSource;
import org.apache.flume.conf.Configurable;
import org.apache.flume.source.AbstractSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by Lei on 2016/11/21.
 */
public class Core_Source extends AbstractSource implements EventDrivenSource,Configurable{
	private static final Logger logger = LoggerFactory.getLogger(Core_Source.class);
	private String monitor_Path;
	private String file_Match;
	@Override
	public void configure(Context context) {
		//读取配置 对必要属性进行判空以及格式检查处理
		monitor_Path = context.getString(CoreSource_Context.MONITOR_PATH);
		if (monitor_Path.isEmpty()){
			logger.error("config " +"< "+ CoreSource_Context.MONITOR_PATH +" >"+ " wrong"+",process exit");
		}
		file_Match = context.getString(file_Match);
		if (file_Match.isEmpty()){
			logger.error("config " +"< "+ CoreSource_Context.MONITOR_PATH +" >"+ " wrong"+",process exit");
		}
	}

	@Override
	public synchronized void start() {
		super.start();
	}

	@Override
	public synchronized void stop() {
		super.stop();
	}
}

class CoreSource_Context{
	public static final String MONITOR_PATH = "mointot_Path";
	public static final String FILE_MATCH = "file_Match";
}