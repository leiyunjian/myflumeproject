package file_util;

import custom_source.FlumeContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;

/**
 * Created by Lei on 2016/12/5.
 * dir_Path_Format参数传递的是一个约定的格式字符串,该字符串表达的是一个目录结构，每一层目录都以正则表达式表示
 * 该层目录名所需匹配的格式。比如/root/[0-9]{4}/
 *
 * 根据是否有新文件判断读取到null的时候是否切换文件，我们是一次性读取一个不存在null值的File[] 中的文件，所以在
 * 读取的file不是数组最后一个文件的时候，读到null我们就可以直接切换文件，读取数组中的下一个文件，只有读取到数组
 * 最后一个文件并是null值的时候，我们直接调用查找更新文件的方法，如果能够找到(即返回的集合不为null，并且集合长
 * 度不为0)，则切换文件，完成读取File[] 的整个过程，并切到刚刚找到的新文件，它们同样被存储在一个File[] 中。如此循环。
 *读取文件的函数接受一个File[],返回一个新的File[],为更新的文件.
 *
 * 在实际的生产环境中目录是严格管理的不会有多余或者杂乱格式的文件或者目录，所以我们可以假设每一层目录在格式上
 * 都可以精确匹配（这一点没有关系，就算不是精确匹配，对寻找符合条件的文件也不造成影响）.
 *
 */
public  class FileUtil {
	private String file_Format;
	Logger logger = LoggerFactory.getLogger(FileUtil.class);

	public FileUtil(String file_Format){
		this.file_Format = file_Format;
	}
	//给出目录格式和文件格式，返回格式目录下所有满足文件名格式并比记录文件中记录的更加新的文件，做升序排序。
	public  File[] get_Files(String dir_Path_Format, String file_Format){
		  //读取记录文件中的 _LastModified。
		   Long _LastModified = 0L;
		   File current_File = null;
			//获取当前正在读取文件的信息
		   File File_RecordPosition = new File(FlumeContext.File_RecordPosition);
		   if(!File_RecordPosition.exists()||File_RecordPosition.length()==0){
			   _LastModified = 0L;//如果没有已读信息，则_LastModified设置为零，也即是选取所有文件
			   System.out.println("File_RecordPosition not exists,_LastModified:"+_LastModified);
		   }else {
			   try {
				   BufferedReader br = new BufferedReader(new FileReader(FlumeContext.File_RecordPosition));
				   String str = br.readLine().split(" ")[0];//最后读到的文件，也是离现在时间最近的文件的文件名。
				   current_File = new File(str);
				   _LastModified = current_File.lastModified();
				   br.close();
			   } catch (FileNotFoundException e) {
				   e.printStackTrace();
			   } catch (IOException e) {
				   e.printStackTrace();
			   }
		   }

		   ArrayList<String> str_List = analysis_Dir_Path(dir_Path_Format);
		   int num_Of_Dir_Plies = str_List.size();
		   ArrayList<File> file_List = new ArrayList<>();
		   file_List.add(new File(str_List.get(0)));
			//从第一次目录向下迭代至最后一层，得到所有的目录，储存在一个列表中如果没有则得到null 有问题
			//如果返回null，后续接不上
		   for(int i=0;i<num_Of_Dir_Plies-1;i++){
			   System.out.println("file_List: "+file_List+"file_Format: "+str_List.get(i+1));
			  file_List =  get_Files(file_List,str_List.get(i+1),true);//从第一层开始迭代。给出符合条件的所有目录
		   }
		   if(file_List!=null) {
			   //从目录列表中得到符合格式的所有文件,如果没有则得到null
			   file_List = get_Files(file_List, file_Format, false);
		   }else{
			   logger.error("can't find any file,dir is wrong!");
			   return null;
		   }
			   //对得到的文件进行过滤，将小于当前文件_LastModified的文件去除掉。
			if(File_RecordPosition.exists()&&File_RecordPosition.length()!=0) {//如果位置记录文件在，则过滤
				int list_Size = file_List.size();
				ArrayList<File> result_List = new ArrayList<>();
				for (int i = 0; i < list_Size; i++) {
					if (file_List.get(i).lastModified() > _LastModified) {
						result_List.add(file_List.get(i));
					}
				}
				if(result_List.size()==0||(result_List.size()==1&&result_List.get(0).getName().equals(current_File.getName()))){
					return null;//有一种情况是它自己，因为LastModified在文件每次写入的时候都会改变。
				}else{
					return result_List.toArray(new File[result_List.size()]);
				}
			}
		    return file_List.toArray(new File[file_List.size()]);
	   }

	ArrayList<String> analysis_Dir_Path(String dir_Path_Format){//解析原始参数，以目录层做分割，也就是"/"或者"\\\\"
		String[] strings = null;
		if(System.getProperty("os.name").contains("Windows")){
			strings = dir_Path_Format.split("\\\\");
			strings[0]+="\\";
		}else{
			strings = dir_Path_Format.split("/");
			if(dir_Path_Format.startsWith("/")){
				strings[0]="/"+strings[0];
			}
		}
		ArrayList<String> str_List = new ArrayList<>();
		Collections.addAll(str_List,strings);
		ArrayList<String> re_List = new ArrayList<>();
		re_List.add("");
		str_List.removeAll(re_List);//去掉空字符串
		return str_List;
	}

	/*在给出的上层目录列表dir_parent中，找到所有符合file_Format的目录或者文件，以is_Dir标记参数说明file_Format
	*是目录格式还是文件格式，返回一个排序后的目录列表或者是文件列表。没有找到则返回null，与基层函数保持一致。
    */
	ArrayList<File> get_Files(ArrayList<File> dir_parent, String file_Format, boolean is_Dir){
		if(dir_parent==null){
			return null;
		}
		int list_Size = dir_parent.size();
		ArrayList<File> file_List = new ArrayList<>();

		ArrayList<File> tmp_File_List = new ArrayList<>();
		for(int i=0;i<list_Size;i++){
			//这里有可能会加到null值，可以在这里消除.
			tmp_File_List = sort_And_List(dir_parent.get(i),file_Format,is_Dir);
			if(tmp_File_List!=null){
				file_List.addAll(tmp_File_List);
			}
		}
		if(file_List.size()==0){
			return null;
		}
		return file_List;
	}

	//从目录dir下找出符合file_Format格式和日期规格（>_LastModified）的目录或者是文件（由is_Dir参数控制），并进行升序排序
	//如果没有找到文件则返回null。
	ArrayList<File> sort_And_List(File dir, final String file_Format, boolean is_Dir){
		File[] files = null;

		if(is_Dir==true){
			files = dir.listFiles(new FileFilter() {
				@Override
				public boolean accept(File pathname) {
					return pathname.getName().matches(file_Format)&&pathname.isDirectory();
				}
			});
		}else{
			files = dir.listFiles(new FileFilter() {
				@Override
				public boolean accept(File pathname) {
					return pathname.getName().matches(file_Format)&&pathname.isFile();
				}
			});
		}

		if(files==null||files.length==0){
			return null;
		}

		Arrays.sort(files, new Comparator<File>() {
			@Override
			public int compare(File o1, File o2) {
				return o1.lastModified()>o2.lastModified()?1:(o1.lastModified()==o2.lastModified()?0:-1);
			}
		});

		ArrayList<File> file_List = new ArrayList<>();
		Collections.addAll(file_List,files);
		return file_List;
	}

//	public void print_Objects(Object[] objects){
//		for (Object ob:objects
//			 ) {
//			System.out.println(ob);
//		}
//	}

	public static void main(String[] args) {

//		FileUtil test = new FileUtil("[0-9]{6}.{4}");
//
//		test.print_Objects(test.get_Files("e:\\test\\test\\.{3}","[0-9]{6}.{4}"));
	//	test.print_Objects(test.get_Files("/root/java/.{2}/",".{6}"));
		//不能对null进行排序
//		File[] files = null;
//		Arrays.sort(files, new Comparator<File>() {
//			@Override
//			public int compare(File o1, File o2) {
//				return o1.lastModified()>o2.lastModified()?1:(o1.lastModified()==o2.lastModified()?0:-1);
//			}
//		});
		//java.lang.NullPointerException
//		ArrayList<String> str_List = null;
//		str_List.add("ok?");
//		System.out.println(str_List);
	}


}
