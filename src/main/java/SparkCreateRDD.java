import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * Spark创建RDD的几种方式
 */
public class SparkCreateRDD {

    public static void main(String[] args){
        SparkConf conf = new SparkConf().setAppName("CreateRDD").setMaster("local[*]");
        JavaSparkContext context = new JavaSparkContext(conf);



    }
}
