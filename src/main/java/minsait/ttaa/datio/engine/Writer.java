package minsait.ttaa.datio.engine;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import minsait.ttaa.datio.common.ConfigParameters;
import static minsait.ttaa.datio.common.naming.PlayerInput.nationality;
import static org.apache.spark.sql.SaveMode.Overwrite;

import java.io.IOException;
import java.io.Serializable;

abstract class Writer implements Serializable{

    private static final long serialVersionUID = 1L;

	static void write(Dataset<Row> df) throws IOException {
    	ConfigParameters conf = new ConfigParameters();
        df
                .coalesce(1)
                .write()
                .partitionBy(nationality.getName())
                .mode(Overwrite)
                .parquet(conf.rutaOut);
    }

}
