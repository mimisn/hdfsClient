import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Progressable;

import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;

/***
 * hdfs 程序访问示例
 *
 *
 */
public class Main {
    static int x = 0;
    public static void main(String[] args) {
        try {
            //从项目classpath中加载core-defaule.xml，hdfs-default.xml，core-site.xml，hdfs-site.xml
            Configuration conf = new Configuration();
            conf.set("dfs.replication","2");  //设置文件副本数为2
            conf.set("dfs.blocksize","64m");  //设置文件分段大小为64m
            conf.set("hadoop.home.dir", "C:\\Users\\mimisn\\Downloads\\hadoop-3.2.0");

            //构造一个访问HDFS文件系统的客户端对象，参数一  HDFS 的URI地址 参数二 客户端要指定的参数 参数三 访问HDFS 的USER
            FileSystem fs = FileSystem.get(new URI("hdfs://192.168.1.222:9000/"),conf,"root");

            //fs.copyFromLocalFile(new Path("D:\\TunSafe-1.5-rc2.exe"),new Path("/"));  //上传文件

            //带进度条的上穿文件
//            InputStream in = new BufferedInputStream(new FileInputStream(new File("D:\\ImmunityDebugger_1_85_setup.exe")));
//            FSDataOutputStream fsDataOutputStream = fs.create(new Path("/test_upload1.zip"), new Progressable() {
//                public void progress() {
//                    x++;
//                    System.out.print(x+"\n");
//                }
//            });
//            IOUtils.copyBytes(in, fsDataOutputStream, 4096);

//            FSDataOutputStream fsDataOutputStream = fs.create(new Path("/chen.text"));          //创建文件并在文件中写入内容
//            fsDataOutputStream.write("hello work!\n".getBytes());                                             //向文件写入内容
//            fsDataOutputStream.write("hello work2!".getBytes());


            //FSDataInputStream open = fs.open(new Path("/chen.text"));            //读取文件
            //IOUtils.copyBytes(open,System.out,1024);                              //显示文件中内容

            //重命名
//            Path oldPath=new Path("/chen.text");
//            Path newPath = new Path("/chen.t");
//            boolean is_rename = fs.rename(oldPath,newPath);
//            if (is_rename){
//                System.out.println("chengong");
//            }else {
//                System.out.println("shibai");
//            }

            //下载文件 需要本地hadoop环境 配置conf.set("hadoop.home.dir", "C:\\Users\\mimisn\\Downloads\\hadoop-3.2.0")和HADOOP_HOME;
            fs.copyToLocalFile(new Path("/chen.text"),new Path("D:\\"));


            fs.close();



        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
