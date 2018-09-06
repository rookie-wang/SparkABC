import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;

import java.util.ArrayList;
import java.util.List;

/**
 * SparkJava实现TopN示例
 */
public class SparkTopN {
    public static void main(String[] args){
        topN("E:\\input\\topn",null);
    }

    public static void topN(final String input, String output){
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
        JavaRDD<String> lines = context.textFile(input,3);

        //4. 分割出每个数字
        JavaRDD<Integer> numbers = lines.flatMap(new FlatMapFunction<String, Integer>() {
            public Iterable<Integer> call(String s) throws Exception {
                List<Integer> list = new ArrayList<Integer>();
                String[] arr  = s.split(" ");
                for(int i=0;i<arr.length;i++){
                    if(arr[i]!=null && !"".equals(arr[i])){
                        list.add(Integer.valueOf(arr[i]));
                    }
                }
                return list;
            }
        });
        //5. 去重
        numbers = numbers.distinct();
        numbers = numbers.sortBy(new Function<Integer, Object>() {
            public Object call(Integer integer) throws Exception {
                return integer;
            }
        },true,3);

        List<Integer> list = numbers.collect();

        numbers.foreach(new VoidFunction<Integer>() {
            public void call(Integer integer) throws Exception {
                System.out.println(integer);
            }
        });

        for(Integer integer : list){
            System.out.println(integer);
        }

    }
}
