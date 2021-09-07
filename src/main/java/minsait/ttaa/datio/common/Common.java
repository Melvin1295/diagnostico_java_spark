package minsait.ttaa.datio.common;

import java.io.Serializable;

public final class Common implements Serializable {

    private static final long serialVersionUID = 1L;
    
	public static final String SPARK_MODE = "local[*]";
    public static final String HEADER = "header";
    public static final String INFER_SCHEMA = "inferSchema";
    public static final String INPUT_PATH = "config.rutaIn";
    public static final String OUTPUT_PATH = "config.rutaOut";

    public static final String VALOR_A = "A";
    public static final String VALOR_B = "B";
    public static final String VALOR_C = "C";
    public static final String VALOR_D = "D";
}
