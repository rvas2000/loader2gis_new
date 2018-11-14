package ru.living.loader2gis.model;

import redis.clients.jedis.Jedis;

import java.util.HashMap;
import java.util.List;

public class Complex extends ModelAbstract implements Comparable<Complex> {
    public Integer complex_id;
    public Integer city_id;
    public Double latitude;
    public Double longitude;

    public static String getKeyPrefix()
    {
        return "complex";
    }

    public String getKey()
    {
        return getKeyPrefix() + ":" + this.city_id.toString() + ":" + this.complex_id.toString();
    }

    public HashMap<String, String> toHashMap()
    {
        HashMap<String,String> result = new HashMap<String, String>();
        if (this.complex_id != null) result.put("complex_id", this.complex_id.toString());
        if (this.city_id != null) result.put("city_id", this.city_id.toString());
        if (this.latitude != null) result.put("latitude", this.latitude.toString());
        if (this.longitude != null) result.put("longitude", this.longitude.toString());
        return result;
    }

    public Complex()
    {}

    public Complex(HashMap<String, String> data)
    {
        this.fromHashMap(data);
    }

    public Complex fromHashMap(HashMap<String, String> data)
    {
        String value;
        if ((value = data.get("complex_id")) != null ) this.complex_id = Integer.parseInt(value);
        if ((value = data.get("city_id")) != null ) this.city_id = Integer.parseInt(value);
        if ((value = data.get("latitude")) != null ) this.latitude = Double.parseDouble(value);
        if ((value = data.get("longitude")) != null ) this.longitude = Double.parseDouble(value);

        return this;
    }


    public static Complex getFromBuff(Jedis jedis, String key)
    {
        HashMap<String, String> data = new HashMap<String, String>();
        List<String> fields = jedis.hmget(key, "complex_id", "city_id", "latitude", "longitude");
        data.put("complex_id", fields.get(0));
        data.put("city_id", fields.get(1));
        data.put("latitude", fields.get(2));
        data.put("longitude", fields.get(3));

        Complex obj = new Complex(data);
        return obj;
    }

    public int compareTo(Complex obj)
    {
        if (this.complex_id < obj.complex_id) {
            return -1;
        } else {
            if (this.complex_id > obj.complex_id) {
                return 1;
            } else {
                return 0;
            }
        }
    }
}
