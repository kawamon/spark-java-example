package solution;

/**
 * Created by kawasaki on 2016/06/26.
 */
/* SimpleApp.java */
import org.apache.spark.api.java.*;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function;
import java.util.*;

public class CountJPGs {
    public static void main(String[] args) {
		if (args.length < 1) {
			System.err.println("Usage: ....");
			System.exit(1);
		}
        SparkConf conf = new SparkConf().setAppName("CountJPGs Java Application");
        JavaSparkContext sc = new JavaSparkContext(conf);

        long jpgcount = sc.textFile(args[0]).filter(line -> line.contains(".jpg")).count();
		System.out.println( "Number of JPG requests: " + jpgcount);
	}
}

