package source;

import myutil.*;

import java.io.*;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by Lei on 2016/10/18.
 */
public class TestUtil {
	public static void printFiles(File[] files) {
		for (File file : files
				) {
			System.out.println(file + " ");
		}
		System.out.println();
	}

	int x;

	private int privatemethod() {
		int y = x + 3;
		return y;
	}

	FileUtil fileUtil;

	public void Method()  {
		int i = 0;
		while(true){
			i++;
			System.out.println(i);
			try {
				Thread.sleep(100);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			if(i==3){
				try {
					throw new MyException();
				} catch (MyException e) {
				e.printStackTrace();
			}
		}
		}
	}
	public void Method1() throws MyException {
		int i = 0;
		while(true){
			i++;
			System.out.println(i);
			try {
				Thread.sleep(100);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			if(i==3){
				throw new MyException();
			}
		}
	}
	;
	class MyException extends Exception{}
	public static void main(String[] args)  {

	//测试自定义异常：
		TestUtil testUtil = new TestUtil();
		testUtil.Method();
//			try {
//			testUtil.Method1();
// 			} catch (MyException e) {
//			e.printStackTrace();
//		}
		System.out.println("behind catch exception");
		//测量在一个文件中边写边读
//			Thread read_Thread = new Thread() {
//			@Override
//			public void run() {
//				File file = new File("E:\\test\\test\\testfile_null.txt");
//			try {
//				BufferedReader reader = new BufferedReader(new FileReader(file));
//				String record;
//				while (true) {
//
//					record = reader.readLine();
//					if (record != null) {
//						System.out.println(record);
//					} else {
//						System.out.println("sleep");
//						sleep(1000);
//					}
//				}
//			} catch (FileNotFoundException e) {
//				e.printStackTrace();
//			} catch (IOException e) {
//				e.printStackTrace();
//
//			} catch (InterruptedException e) {
//				e.printStackTrace();
//			}
//		}
//		};
//
//		final Thread writer_Thread = new Thread() {
//			@Override
//			public void run() {
//				File file = new File("E:\\test\\test\\testfile_null.txt");
//				try {
//					BufferedWriter writer = new BufferedWriter(new FileWriter(file));
//					SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
//					while (true) {
//						String Date = dateFormat.format(System.currentTimeMillis());
//						writer.write(Date);
//						writer.newLine();
//						writer.flush();
//							sleep(1000);
//					}
//				} catch (FileNotFoundException e) {
//					e.printStackTrace();
//				} catch (IOException e) {
//					e.printStackTrace();
//				} catch (InterruptedException e) {
//					e.printStackTrace();
//				}
//			}
//		};
//		writer_Thread.start();
//		read_Thread.start();

		//测试文件如果没有关闭内容能写进去吗?只有BufferedWriter才有flush（）的问题。其他的类不存在缓存也就不用刷新。
//		File file = new File("e:\\test\\filetest.txt");
//			try {
//				if(!file.exists()){
//				file.createNewFile();
//				}
//				RandomAccessFile randomWrite = new RandomAccessFile(file,"rw");
//				String str = "342";
//				randomWrite.seek(randomWrite.length());
//				randomWrite.write(str.getBytes());
//				randomWrite.write(" ".getBytes());
//				randomWrite.write(str.getBytes());
//				randomWrite.close();
//				BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(file,true));
//				bufferedWriter.write(str);
//				bufferedWriter.write(" ");
//				bufferedWriter.write(str);
//				bufferedWriter.flush();
//				//bufferedWriter.close();
//			} catch (IOException e) {
//				e.printStackTrace();
//			}


//		FileUtil fileUtil = new FileUtil();
//		fileUtil.publicmethod();
		//System.out.println("is ok");

		//得出文件的绝对路径文件名
//		File file = new File("e:\\test\\test\\file1.txt");
//		System.out.println(file.getAbsolutePath());//得出文件的绝对路径文件名
//		String str = file.getAbsolutePath();
//		File file1 = new File(str);
//		System.out.println(file1.exists());

		//关于类型的默认值，还有默认值针对的是类的字段，在一个方法中使用是没有默认值这样的说法的，使用之前需要初始化
		//在类的静态mian方法下可以调用私有方法
//		int inta;
//		int intb = inta + 0;//没有被初始化，错误
//		TestUtil testUtil = new TestUtil();
//		System.out.println(testUtil.x);
//		System.out.println(testUtil.fileUtil);
//		System.out.println("is testUtil.privatemethod(): "+testUtil.privatemethod());
		//System.out.println(testUtil.Method());//cant Print(void);

		//类型相同的变量可以相互赋值，就算他们的长度不同
//		byte[] bytes = new byte[50];
//		System.out.println(bytes.length);
//		int[] ints = {1,2,3};
//		int[] ints1 = {2,3,4,5};
//		System.out.println("ints= "+ints+" :");
//		for (int i:ints
//			 ) {
//			System.out.print(i+" ");
//		}
//		System.out.println();
//		System.out.println("ints1= "+ints1+" :");
//		for (int i:ints1
//				) {
//			System.out.print(i+" ");
//		}
//		System.out.println();
//		ints = ints1;
//		System.out.println("ints= "+ints+" :");
//		for (int i:ints
//				) {
//			System.out.print(i+" ");
//		}
//		System.out.println();
	}
}
