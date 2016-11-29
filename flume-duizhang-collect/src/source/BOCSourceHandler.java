package source;

import myutil.FileUtil;
import org.apache.flume.Event;
import org.apache.flume.channel.ChannelProcessor;
import org.apache.flume.event.EventBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.lang.reflect.Array;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Created by Lei on 2016/10/9.
 * 测试版本：一些参数由FlumeContext类 提供
 */
public class BOCSourceHandler extends Thread{
	private String collect_Dir;
	private FileUtil myFileUtil = new FileUtil();
	private ChannelProcessor channelProcessor;
	private int bacthSize;
	private Logger logger;
	private RandomAccessFile writer_File_RecordPosition;
	private byte[] position_Bytes;

	public BOCSourceHandler(){}

	public BOCSourceHandler(ChannelProcessor channelProcessor,int bacthSize,Logger logger,String collect_Dir){
		this.channelProcessor = channelProcessor;
		this.bacthSize = bacthSize;
		//this.logger = logger;
		this.logger = LoggerFactory.getLogger(BOCSourceHandler.class);
		this.collect_Dir = collect_Dir;
		File finished_File =new File(FlumeContext.File_finishFile);
		File position_File = new File(FlumeContext.File_RecordPosition);
		position_Bytes = new byte[50];
		try {
			this.writer_File_RecordPosition = new RandomAccessFile(FlumeContext.File_RecordPosition, "rw");
			if (!finished_File.exists()) {
				finished_File.createNewFile();
				System.out.println("File_finishFile not exit,create it");
			}
			if (!position_File.exists()) {
				position_File.createNewFile();
				System.out.println("position_File not exit,create it");
			}
		}catch (FileNotFoundException e){
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
//读取数据，推送到Channel中，line表示从第几行开始读取，file表示读取的文件，
	private void pushto_Channel(ChannelProcessor channelProcessor,File file,int line,int batch_Size){
		File collect_Dir = new File(FlumeContext.Collect_Dir);
		try {
			BufferedReader reader = new BufferedReader(new FileReader(file));
			String record;
			//找到指定位置
			for(int i=1;i<=line;i++){
				reader.readLine();
			}
			//从指定位置续读
			int batchCount = 0;
			Event event;
			List<Event> eventList = new ArrayList<Event>();
			FileUtil fileUtil = new FileUtil();

			while(true) {//(record = reader.readLine())!=null
				record = reader.readLine();
				if (record != null&&record.length()==FlumeContext.RECORD_LENGTH){

					line++;
					batchCount++;
					//将数据发送到channel：
					//event = EventBuilder.withBody(record, Charset.forName("UTF-8"));
					event = EventBuilder.withBody(record.getBytes());
					eventList.add(event);
					if (batchCount == bacthSize) {
						channelProcessor.processEventBatch(eventList);
						//记录读取进度
						write_to_positionFile(file,line);
						batchCount=0;
						eventList.clear();
					}//当读到空的时候，如果是当前正在读的文件，则读取线程睡1秒，否则读取结束，进入下个文件
				}else if(record==null){
					if(fileUtil.listAndSort(FlumeContext.Collect_Dir).length==0){//没有更新的文件说明它是最后一个文件
						if(!eventList.isEmpty()) {
							channelProcessor.processEventBatch(eventList);

							write_to_positionFile(file,line);
							batchCount = 0;
							eventList.clear();
							sleep(1000);
						}else {
							logger.info("in "+file.getName()+" wiating for new data 1S");
							sleep(1000);
						}
					}else{
						if(eventList.isEmpty()){
							write_to_finishFile(file, line);
							break;
						}else {
							channelProcessor.processEventBatch(eventList);
							write_to_positionFile(file,line);
							write_to_finishFile(file, line);
							break;
						}
					}
				}
			}
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}


	// 该方法用于断点续读，根据File_RecordPosition文件中记录的文件名和行数 开始往下读取剩余的内容
	private void readFrombreakpint(){
		File file = new File(FlumeContext.File_RecordPosition);
		if(!file.exists()||file.length()==0){return;}
		File collect_Dir = new File(FlumeContext.Collect_Dir);
		try {
			BufferedReader reader = new BufferedReader(new FileReader(file));
			//读取断点信息
			String breakpoint_record = reader.readLine();
			reader.close();
			String[] record_split = breakpoint_record.split(" ");
			int lines =Integer.parseInt(record_split[1].trim());
			File breakPointFile = new File(record_split[0]);

			pushto_Channel(channelProcessor,breakPointFile,lines,FlumeContext.BATCH_SIZE);
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}


	private void readFiles(File[] files){
		if(files==null){return;}
		for (File file:files
			 ) {
			pushto_Channel(channelProcessor,file,0,FlumeContext.BATCH_SIZE);
		}

	}

	private void write_to_finishFile(File finishedFile,int Count){
		//将读取完成的文件记录到配置的文件中
		File record_File =new File(FlumeContext.File_finishFile);
		try {
			FileWriter writer = new FileWriter(record_File, true);
			BufferedWriter bufferedWriter = new BufferedWriter(writer);
			bufferedWriter.append(finishedFile.getAbsolutePath() + " " + Count);
			bufferedWriter.newLine();
			bufferedWriter.close();
			logger.info(finishedFile.getAbsolutePath()+" completed");
		}catch (IOException e){
			e.printStackTrace();
		}
	}

	private void write_to_positionFile(File file,int lines){
		//将读取位置记录到配置的文件中
		try {
			Arrays.fill(position_Bytes, (byte) ' ');
			byte[] loc_bytes = (file.getAbsolutePath()+" "+lines).getBytes();
			int length = loc_bytes.length;
			for(int i=0;i<length;i++){
				position_Bytes[i]=loc_bytes[i];
			}
			writer_File_RecordPosition.seek(0L);
			writer_File_RecordPosition.write(position_Bytes);
			String separator = System.getProperty("line.separator");
			writer_File_RecordPosition.write(separator.getBytes());
		}catch (IOException e){
			e.printStackTrace();
		}
	}

	@Override
	public void run() {
		/*读文件的策略是:
		  1进行断点续读
		  2将采集目录下所有的文件先进行一次升序排序（考虑了断点的有无），按顺序读取
		  3将排序后的文件读取完，再进行排序，再读取。读取filelist时，读取到最后一个文件的时候，等待文件更新数据，
		  或者产生新的文件。产生了新的文件则filelist读取完成，进行下一轮文件（比在FlumeContext。File_RecordPosition
		  中记录下来的读取过的文件新的文件）排序和读取，如此反复。
		 */
		//断点续读
		readFrombreakpint();
		//将采集目录下有的文件先进行一次升序排序
		File[] files = myFileUtil.listAndSort(FlumeContext.Collect_Dir);
		while (true){
			//按顺序读取
			readFiles(files);
			files = myFileUtil.listAndSort(FlumeContext.Collect_Dir);
		}
	}

}
