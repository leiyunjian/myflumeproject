package myutil;

import java.io.*;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Comparator;

/**
 * Created by Lei on 2016/10/17.
 */
import source.*;

/**
 * Created by Lei on 2016/10/9.
 * 因为文件在时间上的连续性，我们可以以最后修改时间为标的进行排序，从最早的时间开始采集。由于有后续更新，
 *将最后修改时间在它之后的文件筛选出来，
 * 并做排序，进行采集，仍将最后修改时间最大的文件记录下来。如此反复。如果程序中断我们可以从File_RecordPosition文件中
 //提取类似的信息

 并且将所有采集完成的文件名记录到一个独立的文件中便于查询。

 */

public class FileUtil {
	private Long _LastModified;
	private String file_Format;

	public FileUtil(String file_Format){
		this.file_Format = file_Format;
	}
//对指定目录下符合要求的文件进行升序排序
	public  File[] listAndSort(String dirpath){
		File dir = new File(dirpath);
		if(!dir.exists()){
			return null;
		}
		File File_RecordPosition = new File(FlumeContext.File_RecordPosition);
		if(!File_RecordPosition.exists()||File_RecordPosition.length()==0){
			_LastModified = 0L;//如果没有已读信息，则_LastModified设置为零，也即是选取所有文件
			System.out.println("File_RecordPosition not exists,_LastModified:"+_LastModified);
		}else {
			try {
				BufferedReader br = new BufferedReader(new FileReader(FlumeContext.File_RecordPosition));
				String str = br.readLine().split(" ")[0];//最后读到的文件，也是离现在时间最近的文件的文件名。
				File file = new File(str);
				_LastModified = file.lastModified();
				br.close();
			} catch (FileNotFoundException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		//选出更新的文件，及最后修改时间大于之前排序得到的最大时间。
		File[] files = dir.listFiles(new FileFilter() {
			@Override
			public boolean accept(File pathname) {
				boolean res = false;
					res = pathname.lastModified() > _LastModified && (pathname.getName().matches(file_Format));
				//return pathname.lastModified()>_LastModified;
				return res;
			}
		});
		//最后修改时间升序排序
		Arrays.sort(files, new Comparator<File>() {
			@Override
			public int compare(File o1, File o2) {
				return o1.lastModified()>o2.lastModified()?1:(o1.lastModified()==o2.lastModified()?0:-1);
			}
		});
		return files;
	}
	//返回符合要求的最新一个文件。
	public File last_File(String dir){
		File dir_File = new File(dir);
		File[] dir_Files =dir_File.listFiles(new FilenameFilter() {
			@Override
			public boolean accept(File dir, String name) {
				return name.matches(file_Format);
			}
		});
		Arrays.sort(dir_Files, new Comparator<File>() {
			@Override
			public int compare(File o1, File o2) {
				return o1.lastModified()>o2.lastModified()?1:(o1.lastModified()==o2.lastModified()?0:-1);
			}
		});
		return dir_Files[dir_Files.length-1];
	}
	public void publicmethod(int a,int b){int c = a+b;return;}
	private void privatemethod(){}

	public static void main(String[] args) {
		FileUtil util = new FileUtil(".*");
		String dirpath = "E:\\test\\test";
		System.out.println(util.last_File("E:\\test\\test").getName());
//		File[] files = util.listAndSort(dirpath);
//		TestUtil.printFiles(files);
//		files = util.listAndSort(dirpath);
//		TestUtil.printFiles(files);

//		File file1 = new File(dirpath);
//		File[] files1 = file1.listFiles();
//		SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:dd");
//		for (File file:files1
//			 ) {
//			System.out.println(file.getName()+" "+dateFormat.format(file.lastModified()));
//		}
	}
}