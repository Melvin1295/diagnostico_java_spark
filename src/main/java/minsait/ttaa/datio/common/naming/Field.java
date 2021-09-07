package minsait.ttaa.datio.common.naming;

import org.apache.spark.sql.Column;

import static org.apache.spark.sql.functions.col;

import java.io.Serializable;

public class Field implements Serializable {

    private static final long serialVersionUID = 1L;

	Field(String name) {
        this.name = name;
    }

    private String name = "field";

    public String getName() {
        return name;
    }

    public Column column() {
        return col(name);
    }
}
