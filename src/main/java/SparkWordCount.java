import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.*;
import scala.Tuple2;

import java.util.Arrays;

/**
 * Spark实现wordcount例子
 */
public class SparkWordCount {

    /**
     * 参数1：输入文件地址，文件夹，自动读取下面所有的文件
     * 参数2：输入文件地址，如果没有则不输出
     * @param args
     */
    public static void main(String[] args){
        if(args.length>1){
            wordCount(args[0],args[1]);
        }else{
            //测试
            wordCount("E:\\新建文件夹","E\\output\\a.txt");
        }

    }

    public static void wordCount(String input,String output){

        if(input==null){
            System.err.println("no input path");
            System.exit(1);
        }
        //1. 初始化SprakConf，并且获得SparkContext对象
        SparkConf sparkConf = new SparkConf();
        //设置app名字，和运行模式，运行模式分为local,standalone,yarn
        //本机运行的话，直接使用local模式
        sparkConf.setAppName("SparkWordCount").setMaster("local[*]");

        //2. 获取一个Spark应用程序上下文
        JavaSparkContext context = new JavaSparkContext  (sparkConf);

        //3. 读取文件，抽象成为分布式弹性数据集:{"line1","line2",..."lienN"}
        JavaRDD<String> lines = context.textFile(input);

        //4. 将每一行，切换成一个个单词,抽象成的也是单词的数据集：{"word1","word3",..."wordN"}
        JavaRDD<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
            public Iterable<String> call(String s) throws Exception {
                return Arrays.asList(s.split(" "));
            }
        });

        //5. 将单词的数据集计数：{"word1"：1,"word1"：1,"word2"：1..."wordN"：1}
        //将单词一个个变成key:value的形式
        JavaPairRDD<String,Integer> everyWorld = words.mapToPair(new PairFunction<String, String, Integer>() {
            public Tuple2<String, Integer> call(String s) throws Exception {
                return new Tuple2<String, Integer>(s,1);
            }
        });

        //6. 每个单词都变成了word:1的形式，这时候做reduce操作，根据Key来reduce
        JavaPairRDD<String,Integer> count = everyWorld.reduceByKey(new Function2<Integer, Integer, Integer>() {
            public Integer call(Integer integer, Integer integer2) throws Exception {
                return integer+integer2;
            }
        });

        count.foreach(new VoidFunction<Tuple2<String, Integer>>() {
            public void call(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
                System.out.println(stringIntegerTuple2._1+":"+stringIntegerTuple2._2);
            }
        });

        //7. 输出
        if(output!=null){
            count.saveAsTextFile(output);
        }

        context.close();

    }
}
