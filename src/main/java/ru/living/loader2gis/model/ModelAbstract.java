package ru.living.loader2gis.model;

import java.util.HashMap;

abstract public class ModelAbstract {

    static public String getKeyPrefix() {return "";}

    abstract public String getKey();

    abstract public HashMap<String, String> toHashMap();

    abstract public ModelAbstract fromHashMap(HashMap<String, String> data);
}
