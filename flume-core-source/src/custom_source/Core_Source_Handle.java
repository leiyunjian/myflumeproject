package custom_source;

import file_util.FileUtil;
import org.apache.flume.Event;
import org.apache.flume.channel.ChannelProcessor;
import org.apache.flume.event.EventBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Created by Lei on 2016/12/9.
 */
public class Core_Source_Handle extends Thread{
	private String collect_Dir;
	private FileUtil file_Unit;
	private ChannelProcessor channelProcessor;
	private int bacth_Size;
	private Logger logger;
	private RandomAccessFile writer_File_RecordPosition;
	private byte[] position_Bytes;
	private int record_Length;
	private String collect_File_Format;

	public Core_Source_Handle(ChannelProcessor channelProcessor,int bacthSize,String collect_Dir,int record_Length
			,String collect_File_Format){
		this.channelProcessor = channelProcessor;
		this.bacth_Size = bacthSize;
		this.logger = LoggerFactory.getLogger(Core_Source_Handle.class);
		this.collect_Dir = collect_Dir;
		this.record_Length = record_Length;
		this.collect_File_Format = collect_File_Format;
		this.file_Unit = new FileUtil(collect_File_Format);

		//创建记录文件
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
	//weather_To_Get_Files 表示是否需要更新或者说提取未读取的files【】，该程序中是读取玩一个files【】然后再
	//更新一个files【】，以供读取
	private File[] pushto_Channel(ChannelProcessor channelProcessor,File file,int line,int batch_Size,boolean weather_To_Get_Files){
		File[] new_Files = null;
		int pointline = line;
		if(file==null){
			return new_Files;
		}
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
			//FileUtil fileUtil = new FileUtil();

			while(true) {//(record = reader.readLine())!=null
				record = reader.readLine();
				if (record != null&&record.length()==record_Length){

					line++;
					batchCount++;
					//将数据发送到channel：
					//event = EventBuilder.withBody(record, Charset.forName("UTF-8"));
					event = EventBuilder.withBody(record.getBytes());
					eventList.add(event);
					if (batchCount == bacth_Size) {
						channelProcessor.processEventBatch(eventList);
						//记录读取进度
						write_to_positionFile(file,line);
						batchCount=0;
						eventList.clear();
					}
				}else if(record==null){//当读到空的时候，如果不要给出新文件排序，说明在读列表内的文件，可以直接向下切。
					if(weather_To_Get_Files==false){
						channelProcessor.processEventBatch(eventList);
						//记录读取进度
						write_to_finishFile(file,line);
						write_to_positionFile(file,line);
						eventList.clear();
						break;
					}else{
						//现将消息列表处理完
						channelProcessor.processEventBatch(eventList);
						//记录读取进度
						write_to_positionFile(file,line);
						batchCount=0;
						eventList.clear();

						new_Files = file_Unit.get_Files(collect_Dir,collect_File_Format);
						if(new_Files==null){//如果没有新文件则线程等待1秒，等待数据在当前文件更新
							logger.info("in "+file.getName()+" wiating for new data 1S");
							sleep(1000);
						}else{
							//下面这个判断是为了防止断点恰好在一个文件末尾时，将文件重复记录到finishFile。
							if(pointline<line) {
								write_to_finishFile(file, line);
							}
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
		return new_Files;
	}

	// 该方法用于断点续读，根据File_RecordPosition文件中记录的文件名和行数 开始往下读取剩余的内容
	private File[] readFrombreakpint(){
		File[] new_Files = null;
		File file = new File(FlumeContext.File_RecordPosition);
		if(!file.exists()||file.length()==0){
			new_Files = file_Unit.get_Files(collect_Dir,collect_File_Format);
			return new_Files;
		}
		try {
			BufferedReader reader = new BufferedReader(new FileReader(file));
			//读取断点信息
			String breakpoint_record = reader.readLine();
			reader.close();
			String[] record_split = breakpoint_record.split(" ");
			int lines =Integer.parseInt(record_split[1].trim());
			File breakPointFile = new File(record_split[0]);

			new_Files = pushto_Channel(channelProcessor,breakPointFile,lines,bacth_Size,true);
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return new_Files;
	}


	private File[] readFiles(File[] files){
		File[] new_Files = null;
		if(files.length==0||files==null){
			System.out.println("files is empty");
			try {
				sleep(1000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			return null;
		}
		for(int i=0;i<files.length-1;i++){
			pushto_Channel(channelProcessor,files[i],0,bacth_Size,false);
		}
		System.out.println("begin pushto..."+files[files.length-1].getName());

		new_Files = pushto_Channel(channelProcessor,files[files.length-1],0,bacth_Size,true);


		return new_Files;
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
		/**读文件的策略是:
		* 无论是断点读取还是非断点读取，都返回一个File[],该File[]为接下来要读的文件数组。如果没有，则读取过程等待。
		 */
		//断点续读
		File[] new_Files = null;
		new_Files = readFrombreakpint();
		while (true){
			//按顺序读取
			new_Files = readFiles(new_Files);
		}
	}

	public static void main(String[] args) {
		String regex = "[a-z]{3}[0-9]{1}";
		System.out.println("dir1".matches(regex));
	}
}
