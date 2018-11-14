package ru.living.loader2gis.runnable;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import ru.living.loader2gis.Config;
import ru.living.loader2gis.Service;
import ru.living.loader2gis.model.Complex;
import ru.living.loader2gis.model.Infrastructure;
import ru.living.loader2gis.model.InfrastructureComplex;

import java.util.ArrayList;
import java.util.List;
import java.util.Collections;
import java.util.HashMap;

public class CreateInfrastructureComplexRunnable implements Runnable {

    protected int[] semaphore;
    protected JedisPool jedisPool;
    protected Integer number;
    protected Complex complex;
    protected ArrayList<Infrastructure> infrastructure;

    public CreateInfrastructureComplexRunnable(Integer number, int[] semaphore, JedisPool jedisPool, Complex commplex, ArrayList<Infrastructure> infrastructure)
    {
        this.number = number;
        this.semaphore = semaphore;
        this.jedisPool = jedisPool;
        this.complex = commplex;
        this.infrastructure = infrastructure;
    }

    public void run()
    {
        HashMap<Integer, ArrayList<InfrastructureComplex> > objectsCategory = new HashMap<Integer, ArrayList<InfrastructureComplex>>();
        ArrayList<InfrastructureComplex> objects;
        InfrastructureComplex object;
        Jedis jedis = this.jedisPool.getResource();

        try {

            if (this.infrastructure.size() > 0) {
                for (Infrastructure infrastructure: this.infrastructure) {
                    if (infrastructure.city_id == complex.city_id ) {

                        objects = objectsCategory.get(infrastructure.living_category_id);
                        if ( objects == null) {
                            objects = new ArrayList<InfrastructureComplex>();
                            objectsCategory.put(infrastructure.living_category_id, objects);
                        }

                        object = new InfrastructureComplex();
                        object.complex_id = complex.complex_id;
                        object.infrastructure_id = infrastructure.id;
                        object.living_category_id = infrastructure.living_category_id;
                        object.distance = this.distance(complex.latitude, complex.longitude, infrastructure.latitude, infrastructure.longitude);

                        objects.add(object);
                    }
                }

                for (Integer livingCategoryId: objectsCategory.keySet()) {
                    objects = objectsCategory.get(livingCategoryId);
                    Collections.sort(objects, new InfrastructureComplex.DistanceComparator());
                    List<InfrastructureComplex> nearestObjects = objects.subList(0, 3);

                    for (InfrastructureComplex nearestObject: nearestObjects) {
                        nearestObject.checked = true;
                        jedis.hmset(nearestObject.getKey(), nearestObject.toHashMap());
                    }
                }
            }

        } catch (Exception e) {

        } finally {
            if (jedis.isConnected()) jedis.close();
        }



        this.semaphore[this.number] = 2;
    }


    protected Long distance(Double lat1, Double lon1, Double lat2, Double lon2)
    {
        if (lat1 == null || lat2 == null || lon1 == null || lon2 == null) { return null;}

        lat1 = (Math.PI * lat1) / 180;
        lon1 = (Math.PI * lon1) / 180;

        lat2 = (Math.PI * lat2) / 180;
        lon2 = (Math.PI * lon2) / 180;


        double a = Math.sin((lat2 - lat1) / 2);
        double b = Math.sin((lon2 - lon1) / 2);

        double r = Math.round(12742000 * Math.asin(Math.sqrt( a * a + Math.cos(lat1) * Math.cos(lat2) * b * b)));
        return Math.round(r);
    }
}
