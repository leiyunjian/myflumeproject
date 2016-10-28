package myutil;

import java.io.*;
import java.util.Arrays;
import java.util.Comparator;

/**
 * Created by Lei on 2016/10/17.
 */
import source.*;

/**
 * Created by Lei on 2016/10/9.
 * 因为文件在时间上的连续性，我们可以以最后修改时间为标的进行排序，从最早的时间开始采集。由于有后续更新，
 * 我们可以将每次排序后最后的文件的文件名记录下来，在下次排序的时候先以它为标的，将最后修改时间在它之后的文件筛选出来，
 * 并做排序，进行采集，仍将最后修改时间最大的文件记录下来。如此反复。

 并且建议将所有采集完成的文件名记录到一个独立的文件中便于查询。

 */
public class FileUtil {
	private Long _LastModified;

	public  File[] listAndSort(String dirpath){
		File dir = new File(dirpath);
		if(!dir.exists()){
			return null;
		}
		File LastModifiedFile = new File(FlumeContext._lastModifiedFile);
		if(!LastModifiedFile.exists()){
			_LastModified = 0L;
		}else {
			try {
				BufferedReader br = new BufferedReader(new FileReader(FlumeContext._lastModifiedFile));
				String str = br.readLine();//该行记录中包括排序后的file[]中处于最后位置，也是离现在时间最近的文件的文件名。

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
				return pathname.lastModified()>_LastModified;
			}
		});
		//最后修改时间升序排序
		Arrays.sort(files, new Comparator<File>() {
			@Override
			public int compare(File o1, File o2) {
				return o1.lastModified()>o2.lastModified()?1:(o1.lastModified()==o2.lastModified()?0:-1);
			}
		});
		if(files.length!=0) {
			int i = files.length - 1;
			_LastModified = files[i].lastModified();
			try {
				File _LastModifiedFile = new File(FlumeContext._lastModifiedFile);
				if (!_LastModifiedFile.exists()) {
					_LastModifiedFile.createNewFile();
				}
				RandomAccessFile lastModifiedFile_writer = new RandomAccessFile(FlumeContext._lastModifiedFile,"rw");
				lastModifiedFile_writer.seek(0L);
				byte[] bytes = new byte[40];
				byte[] file_bytes = files[i].getName().getBytes();
				int length = file_bytes.length;
				for(int y=0;y<length;y++){
					bytes[y]=file_bytes[y];
				}
				lastModifiedFile_writer.write(bytes,0,40);
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		return files;
	}

	public static void main(String[] args) {
		FileUtil util = new FileUtil();
		String dirpath = "E:\\test\\test";
		File[] files = util.listAndSort(dirpath);

	}
}
