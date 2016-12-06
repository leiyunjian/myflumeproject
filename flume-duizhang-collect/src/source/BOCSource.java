package source;

import org.apache.flume.ChannelFactory;
import org.apache.flume.Context;
import org.apache.flume.EventDrivenSource;
import org.apache.flume.channel.ChannelProcessor;
import org.apache.flume.conf.Configurable;
import org.apache.flume.source.AbstractSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by Lei on 2016/11/2.
 */
public class BOCSource extends AbstractSource implements EventDrivenSource, Configurable {
	private static final Logger logger = LoggerFactory.getLogger(BOCSource.class);
	private BOCSourceHandler sourceHandler;
	ChannelProcessor channelProcessor;
	private int record_Length;//合格的每条记录的长度用以过滤
	private String collect_File_Format;//需要采集的文件的格式
	private int batch_Size;//一次送往channel的event数量
	private String collect_Dir;//采集目录

	@Override
	public void configure(Context context) {
		//可配项为batch_Size（一次送往channel的event数量），collect_Dir（采集目录），collect_File_Format(采集文件的格式)
		record_Length = context.getInteger("record_Length");
		batch_Size = context.getInteger("batch_Size",FlumeContext.DEFULT_BATCH_SIZE);
		collect_Dir = context.getString("collect_Dir");
		collect_File_Format = context.getString("collect_File_Format",FlumeContext.DEFULT_COLLECT_FILE_FARMAT);
	}

	@Override
	public synchronized void start() {
		channelProcessor = this.getChannelProcessor();
		sourceHandler = new BOCSourceHandler(channelProcessor,batch_Size,collect_Dir,record_Length,collect_File_Format);
		sourceHandler.start();
		super.start();	}

	@Override
	public synchronized void stop() {
		super.stop();
	}
}
