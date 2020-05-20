import com.csvreader.CsvReader;

import java.io.*;
import java.nio.charset.Charset;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class test02 {
    public static void main(String[] args) throws IOException {
        String inputPath = "E:\\Spark_DataCleansing_DataWarehouse\\test\\源数据\\jobs2.csv";
        String outPath = "E:\\Spark_DataCleansing_DataWarehouse\\test\\处理数据\\handle_jobs2.csv";
        FileWriter fw = null;
        BufferedWriter bufw = null;
        fw = new FileWriter(outPath);
        bufw = new BufferedWriter(fw);    // 构造字符缓冲输出流对象
        FileInputStream fis = null;
        InputStreamReader isr = null;
        BufferedReader bufr = null;     // BufferedReader从字符输入流中读取文本，缓冲各个字符，从而实现字符、数组和行的高效读取。
        fis = new FileInputStream(inputPath);   // 构造文件输入流对象
        // InputStreamReader 字符流
        isr = new InputStreamReader(fis);   // 构造读取输入流对象
        // 将字符流转变为缓冲字符流
        bufr = new BufferedReader(isr);    // 构造缓冲字符流对象
        InputStream in = new FileInputStream(inputPath);
        CsvReader cr = new CsvReader(in, Charset.forName("utf-8"));

        System.out.println(cr.getHeader(12));
        cr.readRecord();
        String rawRecord = cr.getRawRecord();

        while(cr.readRecord()) {
            int columnCount = cr.getColumnCount();
            for (int i = 0; i < columnCount; i++) {
                String str = cr.get(i);
                Pattern p = Pattern.compile("\\s+|\t+|\n|\r");
                Matcher m = p.matcher(str);
                String s = m.replaceAll("~~");
                System.out.println(s);
                if(s.startsWith("company_financing_stage")) {
                    cr.readRecord();
                }
                else {
                    bufw.write(s+"###");
                }
            }
            bufw.write("\n");
        }
        bufw.flush();
        cr.close();
    }
}
