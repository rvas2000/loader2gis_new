package ru.living.loader2gis.model;

import redis.clients.jedis.Jedis;

import java.util.HashMap;
import java.util.List;

public class Infrastructure extends ModelAbstract {
    public Integer id;
    public String external_id;
    public Integer living_category_id;
    public String name;
    public Double latitude;
    public Double longitude;
    public Integer city_id;

    public static String getKeyPrefix()
    {
        return "infrastructure";
    }

    public String getKey()
    {
        return Infrastructure.getKeyPrefix() + ":" + this.external_id;
    }

    public Infrastructure()
    {}


    public Infrastructure (HashMap<String, String> data)
    {
        this.fromHashMap(data);
    }

    public Infrastructure fromHashMap(HashMap<String, String> data)
    {
        String value;
        if ((value = data.get("id")) != null ) this.id = Integer.parseInt(value);
        if ((value = data.get("external_id")) != null ) this.external_id = value;
        if ((value = data.get("living_category_id")) != null ) this.living_category_id = Integer.parseInt(value);
        if ((value = data.get("name")) != null ) this.name = value;
        if ((value = data.get("latitude")) != null ) this.latitude = Double.parseDouble(value);
        if ((value = data.get("longitude")) != null ) this.longitude = Double.parseDouble(value);
        if ((value = data.get("city_id")) != null ) this.city_id = Integer.parseInt(value);

        return this;
    }

    public HashMap<String, String> toHashMap()
    {
        HashMap<String, String> result = new HashMap<String, String>();
        if (this.id != null) result.put("id", this.id.toString());
        if (this.external_id != null) result.put("external_id", this.external_id);
        if (this.living_category_id != null) result.put("living_category_id", this.living_category_id.toString());
        if (this.name != null) result.put("name", this.name);
        if (this.latitude != null) result.put("latitude", this.latitude.toString());
        if (this.longitude != null) result.put("longitude", this.longitude.toString());
        if (this.city_id != null) result.put("city_id", this.city_id.toString());
        return result;
    }

    public static Infrastructure getFromBuff(Jedis jedis, String key)
    {
        HashMap<String, String> data = new HashMap<String, String>();
        List<String> fields = jedis.hmget(key, "id", "external_id", "living_category_id", "city_id", "name", "latitude", "longitude");
        data.put("id", fields.get(0));
        data.put("external_id", fields.get(1));
        data.put("living_category_id", fields.get(2));
        data.put("city_id", fields.get(3));
        data.put("name", fields.get(4));
        data.put("latitude", fields.get(5));
        data.put("longitude", fields.get(6));

        Infrastructure obj = new Infrastructure(data);
        return obj;
    }

}
