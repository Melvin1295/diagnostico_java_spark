package minsait.ttaa.datio.engine;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;
import org.jetbrains.annotations.NotNull;

import minsait.ttaa.datio.common.ConfigParameters;

import static minsait.ttaa.datio.common.Common.*;
import static minsait.ttaa.datio.common.naming.PlayerInput.*;
import static minsait.ttaa.datio.common.naming.PlayerOutput.*;
import static org.apache.spark.sql.functions.*;

import java.io.IOException;
import java.io.Serializable;

public class Transformer extends Writer implements Serializable {
	
    private static final long serialVersionUID = 1L;
    
	private SparkSession spark;

    public Transformer(@NotNull SparkSession spark) throws IOException {
        this.spark = spark;
        Dataset<Row> df = readInput();

        df.printSchema();

        df = cleanData(df);
        df = addAgeRange(df);
        df = addRankByNationalityPosition(df);
        df = addPotentialVsOverall(df);
        df = columnSelection(df);
        df = addFiltros(df);
        // for show 100 records after your transformations and show the Dataset schema
        df.show(100000, false);
        df.printSchema();

        // Uncomment when you want write your final output
        write(df);
    }

    /**
     * Selecciona campos de tabla cargada y/o indicada en csv
     * @param df
     * @return
     */
    private Dataset<Row> columnSelection(Dataset<Row> df) {
        return df.select(
                shortName.column(),
                longName.column(),
                age.column(),
                heightCm.column(),
                weightKg.column(),
                nationality.column(),
                clubName.column(),
                overall.column(),
                potential.column(),
                teamPosition.column(),
                ageRange.column(),
                rankByNationalityPosition.column(),
                potentialVsOverall.column()
        );
    }

    
    /**
     * @return a Dataset con información de archivo csv 
     * @throws IOException 
     */
    private Dataset<Row> readInput() throws IOException {
    	ConfigParameters conf = new ConfigParameters();
        Dataset<Row> df = spark.read()
                .option(HEADER, true)
                .option(INFER_SCHEMA, true)
                .csv(conf.getRutaIn());
        return df;
    }
    
    /**
     * Limpiar data inicial
     * @param df Es la fila de datos del csv
     * @return La dataset modificado según regla
     */
    private Dataset<Row> cleanData(Dataset<Row> df) {
        df = df.filter(
                teamPosition.column().isNotNull().and(
                        shortName.column().isNotNull()
                ).and(
                        overall.column().isNotNull()
                ).and( 
                		age.column().isNotNull()
                ).and( 
                		teamPosition.column().isNotNull()
                ).and (
                		potential.column().isNotNull()
                )
        );

        return df;
    }
    
    /**
     * Filtraremos de acuerdo a las columnas age_range y rank_by_nationality_position con las siguientes condiciones:
     * Si rank_by_nationality_position es menor a 3
     * Si age_range es B o C y potential_vs_overall es superior a 1.15
     * Si age_range es A y potential_vs_overall es superior a 1.25
     * Si age_range es D y rank_by_nationality_position es menor a 5
     * @param df Es la fila de datos del csv
     * @return La dataset modificado según regla
     */
    private Dataset<Row> addFiltros(Dataset<Row> df) {
        df = df.filter(
                rankByNationalityPosition.column().$less(3).or(
                        (ageRange.column().$eq$eq$eq(VALOR_B).or(ageRange.column().$eq$eq$eq(VALOR_C))).and(potentialVsOverall.column().$greater(1.15))
                ).or(
                		(ageRange.column().$eq$eq$eq(VALOR_A)).and(potentialVsOverall.column().$greater(1.25))
                ).or( 
                		(ageRange.column().$eq$eq$eq(VALOR_D)).and(rankByNationalityPosition.column().$less(5))
                )
        );

        return df;
    }
    
    /**
     * Agregar una columna age_range qué responderá a la siguiente regla
     * A si el jugador es menor de 23 años
     * B si el jugador es menor de 27 años
     * C si el jugador es menor de 32 años
     * D si el jugador tiene 32 años o más
     * @param df Es la fila de datos del csv
     * @return La dataset modificado según regla
     */
    private Dataset<Row> addAgeRange(Dataset<Row> df) {     
       Column rule = when(age.column().$less(23), VALOR_A)
                .when(age.column().$less(27), VALOR_B)
                .when(age.column().$less(32), VALOR_C)
                .otherwise(VALOR_D);
        df = df.withColumn(ageRange.getName(), rule);
        return df;
    }
    
    /**
     * Agregaremos una columna rank_by_nationality_position con la siguiente regla: Para cada país (nationality) 
     * y posición(team_position) debemos ordenar a los jugadores por la columna overall de forma descendente y 
     * colocarles un número generado por la función row_number
     * @param df Es la fila de datos del csv
     * @return La dataset modificado según regla
     */
    private Dataset<Row> addRankByNationalityPosition(Dataset<Row> df) {
        WindowSpec w = Window
                .partitionBy(nationality.column(), teamPosition.column())
                .orderBy(overall.column().desc());
        Column rowNumber = row_number().over(w);
        df = df.withColumn(rankByNationalityPosition.getName(), rowNumber);
        return df;
    }
    
    /**
     * Agregaremos una columna potential_vs_overall cuyo valor estará definido por la siguiente regl
     * @param df Es la fila de datos del csv
     * @return La dataset modificado según regla
     */
    private Dataset<Row> addPotentialVsOverall(Dataset<Row> df) {      
        Column potenti = round((potential.column().divide(overall.column())), 2);
        df = df.withColumn(potentialVsOverall.getName(), potenti);
        return df;
    }
}