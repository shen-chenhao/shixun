import com.csvreader.CsvReader;

import java.io.*;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


public class test04 {
    public static void main(String[] args) throws IOException {
        String inputPath = "E:\\Spark_DataCleansing_DataWarehouse\\test\\源数据\\jobs4.csv";
        String outPath = "E:\\Spark_DataCleansing_DataWarehouse\\test\\处理数据\\handle_jobs4.csv";
        FileWriter fw = null;
        BufferedWriter bufw = null;
        fw = new FileWriter(outPath);
        bufw = new BufferedWriter(fw);
        FileInputStream fis = null;
        InputStreamReader isr = null;
        BufferedReader bufr = null;
        fis = new FileInputStream(inputPath);
        // InputStreamReader 字符流
        isr = new InputStreamReader(fis);
        // 将字符流转变为缓冲字符流
        bufr = new BufferedReader(isr);
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
                    bufw.write(s+"%%%");
                }
            }
            bufw.write("\n");
        }
        bufw.flush();
        cr.close();
    }
}
