package minsait.ttaa.datio;

import minsait.ttaa.datio.engine.Transformer;
import org.apache.spark.sql.SparkSession;

import static minsait.ttaa.datio.common.Common.SPARK_MODE;

import java.io.IOException;
import java.io.Serializable;

public class Runner implements Serializable {
	
    private static final long serialVersionUID = 1L;
    
    /** Inicia session con apache spark **/
	static SparkSession spark = SparkSession
            .builder()
            .master(SPARK_MODE)
            .getOrCreate();

    public static void main(String[] args) throws IOException {
    	//System.setProperty("hadoop.home.dir","C:\\Spark\\hadoop-2.7.3\\bin" );
        @SuppressWarnings("unused")
		Transformer engine = new Transformer(spark);
    }
}
