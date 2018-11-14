package ru.living.loader2gis.runnable;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import ru.living.loader2gis.api.Api;
import ru.living.loader2gis.api.model.SearchRequest;
import ru.living.loader2gis.api.model.SearchResponse;
import ru.living.loader2gis.model.Infrastructure;
import ru.living.loader2gis.runnable.model.SearchInfrastructureParameters;

import java.util.ArrayList;

public class SearchInfrastructureRunnable implements Runnable {

    protected int number;
    int[] semaphore;
    protected JedisPool jedisPool;
    protected SearchInfrastructureParameters parameters;


    public SearchInfrastructureRunnable(int number, int[] semapore, JedisPool jedisPool, SearchInfrastructureParameters parameters)
    {
        this.number = number;
        this.semaphore = semapore;
        this.jedisPool = jedisPool;
        this.parameters = parameters;
    }

    public String toString()
    {
        return String.format("living_category_id = %d; city_id = %d; rubric_id = %d", this.parameters.livingCategoryId, this.parameters.cityId, this.parameters.rubricId);
    }

    public void run()
    {
//        try {
//            Thread.sleep(3000 * (1 + number));
//            this.semaphore[this.number] = 2;
//        } catch (Exception e) {
//
//        }

        this.action();
    }

    protected void action()
    {
        //        System.out.println(String.format("Запущен процесс %s", this.toString()));
        Jedis jedis = null;
        SearchRequest params = null;
        Api api = null;
        Integer livingCategoryId = null;
        Integer cityId = null;
        try {
            jedis = this.jedisPool.getResource();
            params = new SearchRequest();
            api = new Api();

            params.radius = this.parameters.radius;
            params.rubric_id = this.parameters.rubricId;
            params.setPoint(this.parameters.latitude, this.parameters.longitude);
            livingCategoryId = this.parameters.livingCategoryId;
            cityId = this.parameters.cityId;

            params.page = 0;
            int total = 0;
            int loaded = 0;

            do {
                params.page++;
                SearchResponse.Result result = api.searchObjects(params);

                if (params.page == 1) {
                    total = result.total;
                }

//                System.out.println(String.format("Записывается станица %d", params.page));
                this.saveToBuff(jedis, cityId, livingCategoryId, result.items);

                loaded += result.items.length;
            } while(loaded < total);
            this.semaphore[this.number] = 2;
        } catch (Exception e) {
            System.out.println(e.getMessage());
            this.semaphore[this.number] = 3;
        } finally {
            if (jedis.isConnected()) jedis.close();
            System.out.println(String.format("Завершен процесс %s", this.toString()));
        }

    }

    protected void saveToBuff(Jedis jedis, int cityId, int livingCategoryId, SearchResponse.ResultItem[] items)
    {
        for (SearchResponse.ResultItem item: items) {
            Infrastructure obj = new Infrastructure();
            String externalId = item.id.split("_")[0];

            obj.external_id = externalId;
            obj.name = item.name;
            obj.longitude = item.point.lon;
            obj.latitude = item.point.lat;
            obj.living_category_id = livingCategoryId;
            obj.city_id = cityId;

            jedis.hmset(obj.getKey(), obj.toHashMap());

        }

    }
}
