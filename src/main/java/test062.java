import java.io.*;

public class test062 {
    public static void main(String[] args) {
        FileInputStream fis = null;
        InputStreamReader isr = null;
        BufferedReader bufr = null;
        String inputPath = "E:\\Spark_DataCleansing_DataWarehouse\\test\\处理数据\\handle_jobs6.csv";

        FileWriter fw = null;
        BufferedWriter bufw = null;
        String outPath = "E:\\Spark_DataCleansing_DataWarehouse\\test\\提取数据\\extract_jobs6.csv";
        int fileCount = 0;  // 处理的文件数量
        int dataCount = 0;	// 处理的数据数量
        try {
            fw = new FileWriter(outPath);
            bufw = new BufferedWriter(fw);

            fis = new FileInputStream(inputPath);
            // InputStreamReader 字符流
            isr = new InputStreamReader(fis);
            // 将字符流转变为缓冲字符流
            bufr = new BufferedReader(isr);

            String line = bufr.readLine();
            String str = "";
            while(line != null) {

//                String[] strs = line.split("###");
//                String word = "";
//                for(int i=0; i<strs.length; i++) {
//                    if(strs[i].equals("")) {
//                        continue;
//                    }
//                    if(i == 0) {
//                        word += strs[i];
//                    }else {
//                        word += "###"+strs[i];
//                    }
//                }

                String[] newStrs = line.split("###");
                for(int i=0; i<newStrs.length; i++) {
                    if(i==0 || i==1 || i==3 || i==7 || i==9) {
                        str += newStrs[i]+"/";
                    }
                }

                str = str.substring(0, str.length()-1);

                bufw.write(str);
                bufw.newLine();
                dataCount++;
                str = "";
                line = bufr.readLine();
            }
            if(bufr!=null){
                try {
                    bufr.close();
                } catch (IOException e) {
                }
            }
            if(isr!=null){
                try {
                    isr.close();
                } catch (IOException e) {
                }
            }

            if(fis!=null){
                try {
                    fis.close();
                } catch (IOException e) {
                }
            }
            fileCount++;
            if(fileCount % 50 == 0) {
                System.out.println();
            }else {
                System.out.print(".");
            }

            bufw.flush();
            System.out.println("共处理:"+fileCount+"个文件,和"+dataCount+"条数据");
        } catch (Exception e) {
            e.printStackTrace();
        }finally{
            if(bufw!=null){
                try {
                    bufw.close();
                } catch (IOException e) {
                }
            }
            if(fw!=null){
                try {
                    fw.close();
                } catch (IOException e) {
                }
            }

        }
    }
}
