package source;

import myutil.FileUtil;

import java.io.*;

/**
 * Created by Lei on 2016/10/9.
 */
public class BOCSourceHandler extends Thread{
	FileUtil myFileUtil = new FileUtil();

	// 该方法用于断点续读，根据File_RecordPosition文件中记录的文件名和行数 开始往下读取剩余的内容
	void readFrombreakpint(){
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
			while((record = reader_breakpoint.readLine())!=null){
				line++;
				writer_File_RecordPosition.seek(0L);
				byte[] bytes = new byte[50];
				byte[] breakPointFile_bytes = (breakPointFile.getName()+" "+line).getBytes();
				int length =breakPointFile_bytes.length;
				for(int y=0;y<length;y++){
					bytes[y]=breakPointFile_bytes[y];
				}
			    writer_File_RecordPosition.write(bytes,0,50);
			}
			writer_File_RecordPosition.close();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}


	}

	void readFiles(File[] files){
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

				while (true){
					record = br.readLine();
					if(record!=null){
						//读取文件
						System.out.println(record);
						count++;
						//记录读取进度，便于断点续读
						writer_File_RecordPosition.seek(0L);
						byte[] bytes = new byte[50];
						byte[] breakPointFile_bytes = (file.getName()+" "+count).getBytes();
						int length =breakPointFile_bytes.length;
						for(int y=0;y<length;y++){
							bytes[y]=breakPointFile_bytes[y];
						}
						writer_File_RecordPosition.write(bytes,0,50);
					}else{
						br.close();
						//将读取完成的文件记录到配置的文件中
						File FinishFile = new File(FlumeContext.File_finishFile);
						if(!FinishFile.exists()){
							FinishFile.createNewFile();
						}
						FileWriter writer = new FileWriter(FinishFile,true);
						BufferedWriter bufferedWriter = new BufferedWriter(writer);
						bufferedWriter.append(file.getName()+" "+count);
						bufferedWriter.newLine();
						bufferedWriter.close();
						break;
					}
				}
			} catch (FileNotFoundException e) {
				e.printStackTrace();
			}catch (IOException e){
				e.printStackTrace();
			}
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
		File[] files = myFileUtil.listAndSort(FlumeContext.Collect_Dir);
		while (true){
			//按顺序读取
			readFiles(files);
			files = myFileUtil.listAndSort(FlumeContext.Collect_Dir);
			if(files==null){
				System.out.println("completed all files, waiting for more file");
				try {
					sleep(1000);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		}
	}

	public static void main(String[] args) {
		BOCSourceHandler handler = new BOCSourceHandler();
		handler.start();
		System.out.println("is ok");
	}
}
