package minsait.ttaa.datio.common.naming;

import java.io.Serializable;

public final class PlayerInput implements Serializable {

    private static final long serialVersionUID = 1L;
    
	public static Field shortName = new Field("short_name");
    public static Field longName = new Field("long_name");
    public static Field age = new Field("age");
    public static Field heightCm = new Field("height_cm");
    public static Field weightKg = new Field("weight_kg");
    public static Field nationality = new Field("nationality");
    public static Field clubName = new Field("club_name");
    public static Field overall = new Field("overall");
    public static Field potential = new Field("potential");
    public static Field teamPosition = new Field("team_position");
    
}
