package minsait.ttaa.datio.common.naming;

import java.io.Serializable;

public final class PlayerOutput implements Serializable {

    private static final long serialVersionUID = 1L;
    
	public static Field ageRange = new Field("age_range");
    public static Field rankByNationalityPosition = new Field("rank_by_nationality_position");
    public static Field potentialVsOverall = new Field("potential_vs_overall");

}
