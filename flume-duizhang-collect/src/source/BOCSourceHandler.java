package source;

import myutil.FileUtil;
import org.apache.flume.Event;
import org.apache.flume.channel.ChannelProcessor;
import org.apache.flume.event.EventBuilder;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by Lei on 2016/10/9.
 */
public class BOCSourceHandler extends Thread{
	private FileUtil myFileUtil = new FileUtil();
	private ChannelProcessor channelProcessor;
	private int bacthSize;

	public BOCSourceHandler(ChannelProcessor channelProcessor,int bacthSize){
		this.channelProcessor = channelProcessor;
		this.bacthSize = bacthSize;
	}

	private void pushto_Channel(ChannelProcessor channelProcessor,int bacthSize){

	}


	// 该方法用于断点续读，根据File_RecordPosition文件中记录的文件名和行数 开始往下读取剩余的内容
	private void readFrombreakpint(){
		File file = new File(FlumeContext.File_RecordPosition);
		if(!file.exists()){return;}
		try {
			BufferedReader reader = new BufferedReader(new FileReader(file));
			//读取断点信息
			String breakpoint_record = reader.readLine();
			reader.close();
			String[] record_split = breakpoint_record.split(" ");
			int line =Integer.parseInt(record_split[1].trim());
			File breakPointFile = new File(record_split[0]);

			//根据断点信息往下续读
			BufferedReader reader_breakpoint = new BufferedReader(new FileReader(breakPointFile));
			String record;
			//找到断点
			for(int i=1;i<=line;i++){
				reader_breakpoint.readLine();
			}
			//用于实时记录采集位置
			RandomAccessFile writer_File_RecordPosition = new RandomAccessFile(FlumeContext.File_RecordPosition,"rw");
		    //从断点续读
			byte[] bytes = new byte[50];
			int batchCount = 0;
			Event event;
			List<Event> eventList = new ArrayList<Event>();


			while(true) {//(record = reader_breakpoint.readLine())!=null
				record = reader_breakpoint.readLine();
				if (record != null&&record.length()==FlumeContext.RECORD_LENGTH){
					//用以测试的打印
					System.out.println(record);
					line++;
					batchCount++;
					//将数据发送到channel：
					event = EventBuilder.withBody(record.getBytes());
					eventList.add(event);
					if (batchCount == bacthSize) {
						channelProcessor.processEventBatch(eventList);
						batchCount=0;
						eventList.clear();
						//记录读取进度
						writer_File_RecordPosition.seek(0L);
						byte[] breakPointFile_bytes = (breakPointFile.getAbsolutePath() + " " + line).getBytes();
						int length = breakPointFile_bytes.length;
						for (int y = 0; y < length; y++) {
							bytes[y] = breakPointFile_bytes[y];
						}
						writer_File_RecordPosition.write(bytes,0,50);
					}
				}else if(record==null){//当读到空的时候，如果是当前正在读的文件，则读取线程睡1秒，否则读取结束，进入下个文件
					if(myFileUtil.listAndSort(FlumeContext.Collect_Dir).length==0){
						channelProcessor.processEventBatch(eventList);
						sleep(1000);
					}else{
						channelProcessor.processEventBatch(eventList);
						writer_File_RecordPosition.write(bytes,0,50);
						write_to_finishFile(breakPointFile,line);
						writer_File_RecordPosition.close();
						break;
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


	private void readFiles(File[] files){
		if(files==null){return;}
		for (File file:files
			 ) {
			try {
				BufferedReader br = new BufferedReader(new FileReader(file));
				String record;//用于记录存储内容
				int count=0;//用于记录读取记录的条数
				File recordFile = new File(FlumeContext.File_RecordPosition);
				if(!recordFile.exists()){
					recordFile.createNewFile();
				}
				RandomAccessFile writer_File_RecordPosition = new RandomAccessFile(FlumeContext.File_RecordPosition,"rw");

				byte[] bytes = new byte[50];
				int batchCount = 0;
				Event event;
				List<Event> eventList = new ArrayList<Event>();
				while(true) {//(record = reader_breakpoint.readLine())!=null
					record = br.readLine();
					if (record != null&&record.length()==FlumeContext.RECORD_LENGTH){
						//用以测试的打印
						System.out.println(record);
						count++;
						batchCount++;
						//将数据发送到channel：
						event = EventBuilder.withBody(record.getBytes());
						eventList.add(event);
						if (batchCount == bacthSize) {
							channelProcessor.processEventBatch(eventList);
							batchCount=0;
							eventList.clear();
							//记录读取进度
							writer_File_RecordPosition.seek(0L);
							byte[] breakPointFile_bytes = (file.getAbsolutePath() + " " + count).getBytes();
							int length = breakPointFile_bytes.length;
							for (int y = 0; y < length; y++) {
								bytes[y] = breakPointFile_bytes[y];
							}
							writer_File_RecordPosition.write(bytes,0,50);
						}
					}else if(record==null){//当读到空的时候，如果是当前正在读的文件，则读取线程睡1秒，否则读取结束，进入下个文件
						if(myFileUtil.listAndSort(FlumeContext.Collect_Dir).length==0){
							channelProcessor.processEventBatch(eventList);
							sleep(1000);
						}else{
							channelProcessor.processEventBatch(eventList);
							writer_File_RecordPosition.write(bytes,0,50);
							write_to_finishFile(file,count);
							writer_File_RecordPosition.close();
							break;
						}
					}
				}
//				while (true){
//					record = br.readLine();
//					if(record!=null){
//						//读取文件
//						count++;
//						System.out.println(record);
//						//记录读取进度，便于断点续读
//						writer_File_RecordPosition.seek(0L);
//						byte[] breakPointFile_bytes = (file.getAbsolutePath()+" "+count).getBytes();
//						int length =breakPointFile_bytes.length;
//						for(int y=0;y<length;y++){
//							bytes[y]=breakPointFile_bytes[y];
//						}
//						writer_File_RecordPosition.write(bytes,0,50);
//					} else{
//						br.close();
//						//将读取完成的文件记录到配置的文件中
//						write_to_finishFile(file,count);
//						break;
//					}
//				}
			} catch (FileNotFoundException e) {
				e.printStackTrace();
			}catch (IOException e){
				e.printStackTrace();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}

	}

	private void write_to_finishFile(File finishFile,int Count){
		//将读取完成的文件记录到配置的文件中
		try {
			if (!finishFile.exists()) {
				finishFile.createNewFile();
			}
			FileWriter writer = new FileWriter(finishFile, true);
			BufferedWriter bufferedWriter = new BufferedWriter(writer);
			bufferedWriter.append(finishFile.getAbsolutePath() + " " + Count);
			bufferedWriter.newLine();
			bufferedWriter.close();
		}catch (IOException e){
			e.printStackTrace();
		}
	}

	@Override
	public void run() {
		/*读文件的策略是:
		  1进行断点续读
		  2将采集目录下有的文件先进行一次升序排序（考虑了断点的有无），按顺序读取
		  3读取完原有的文件，程序进入休息和唤醒排序的循环状态，由于排序策略会将新更新进来的文件纳入file[]中，然后继续进行采集
		 */
		readFrombreakpint();
		//将采集目录下有的文件先进行一次升序排序
		int count = 0;
		File[] files = myFileUtil.listAndSort(FlumeContext.Collect_Dir);
		while (true){
			//按顺序读取
			readFiles(files);
			count++;
			files = myFileUtil.listAndSort(FlumeContext.Collect_Dir);
			if(files.length==0){
				System.out.println("completed all files, waiting for more file");
				try {
					sleep(10000);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		}
	}

	public static void main(String[] args) {
//		BOCSourceHandler handler = new BOCSourceHandler();
//		handler.start();
	//	System.out.println("is ok");
	}
}
