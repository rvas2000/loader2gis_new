package ru.living.loader2gis.model;

import redis.clients.jedis.Jedis;

import java.util.HashMap;
import java.util.List;
import java.util.Comparator;


public class InfrastructureComplex extends ModelAbstract {
    public Integer id;
    public Integer complex_id;
    public Integer infrastructure_id;
    public Integer living_category_id;
    public Long distance;
    public Long distance_walking;
    public Long duration_walking;
    public Long distance_transit;
    public Long duration_transit;
    public Long distance_driving;
    public Long duration_driving;
    public Boolean checked = false;

    public static String getKeyPrefix()
    {
        return "infrastructure_complex";
    }

    public String getKey()
    {
        return InfrastructureComplex.getKeyPrefix() + ":" + this.complex_id.toString() + "_" + this.infrastructure_id;
    }

    public InfrastructureComplex()
    {}


    public InfrastructureComplex(HashMap<String, String> data)
    {
        this.fromHashMap(data);
    }

    public InfrastructureComplex fromHashMap(HashMap<String, String> data)
    {
        String value;
        if ((value = data.get("id")) != null ) this.id = Integer.parseInt(value);
        if ((value = data.get("complex_id")) != null ) this.complex_id = Integer.parseInt(value);
        if ((value = data.get("infrastructure_id")) != null ) this.infrastructure_id = Integer.parseInt(value);
        if ((value = data.get("living_category_id")) != null ) this.living_category_id = Integer.parseInt(value);
        if ((value = data.get("distance")) != null ) this.distance = Long.parseLong(value);
        if ((value = data.get("distance_walking")) != null ) this.distance_walking = Long.parseLong(value);
        if ((value = data.get("duration_walking")) != null ) this.duration_walking = Long.parseLong(value);
        if ((value = data.get("distance_transit")) != null ) this.distance_transit = Long.parseLong(value);
        if ((value = data.get("duration_transit")) != null ) this.duration_transit = Long.parseLong(value);
        if ((value = data.get("distance_driving")) != null ) this.distance_driving = Long.parseLong(value);
        if ((value = data.get("duration_driving")) != null ) this.duration_driving = Long.parseLong(value);
        if ((value = data.get("checked")) != null ) this.checked = Boolean.parseBoolean(value);

        return this;
    }

    public HashMap<String, String> toHashMap()
    {
        HashMap<String, String> result = new HashMap<String, String>();
        if (this.id != null) result.put("id", this.id.toString());
        if (this.complex_id != null) result.put("complex_id", this.complex_id.toString());
        if (this.infrastructure_id != null) result.put("infrastructure_id", this.infrastructure_id.toString());
        if (this.living_category_id != null) result.put("living_category_id", this.living_category_id.toString());
        if (this.distance != null) result.put("distance", this.distance.toString());
        if (this.distance_walking != null) result.put("distance_walking", this.distance_walking.toString());
        if (this.duration_walking != null) result.put("duration_walking", this.duration_walking.toString());
        if (this.distance_transit != null) result.put("distance_transit", this.distance_transit.toString());
        if (this.duration_transit != null) result.put("duration_transit", this.duration_transit.toString());
        if (this.distance_driving != null) result.put("distance_driving", this.distance_driving.toString());
        if (this.duration_driving != null) result.put("duration_driving", this.duration_driving.toString());
        if (this.checked != null) result.put("checked", this.checked.toString());
        return result;
    }


    public static InfrastructureComplex getFromBuff(Jedis jedis,  String key)
    {
        HashMap<String, String> data = new HashMap<String, String>();
        List<String> fields = jedis.hmget(key, "id", "complex_id", "infrastructure_id", "living_category_id", "distance", "distance_walking", "duration_walking", "distance_transit", "duration_transit", "distance_driving", "duration_driving", "checked");
        data.put("id", fields.get(0));
        data.put("complex_id", fields.get(1));
        data.put("infrastructure_id", fields.get(2));
        data.put("living_category_id", fields.get(3));
        data.put("distance", fields.get(4));
        data.put("distance_walking", fields.get(5));
        data.put("duration_walking", fields.get(6));
        data.put("distance_transit", fields.get(7));
        data.put("duration_transit", fields.get(8));
        data.put("distance_driving", fields.get(9));
        data.put("duration_driving", fields.get(10));
        data.put("checked", fields.get(11));

        InfrastructureComplex obj = new InfrastructureComplex(data);
        return obj;
    }

    public static class DistanceComparator  implements Comparator<InfrastructureComplex>
    {
        public int compare(InfrastructureComplex obj1, InfrastructureComplex obj2)
        {
            if (obj1.living_category_id == obj2.living_category_id) {
                if (obj1.distance < obj2.distance) {
                    return -1;
                } else {
                    if (obj1.distance > obj2.distance) {
                        return 1;
                    } else {
                        return 0;
                    }
                }
            } else {
                return 1;
            }
        }

    }


}
