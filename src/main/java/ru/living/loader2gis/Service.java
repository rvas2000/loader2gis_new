package ru.living.loader2gis;

import java.sql.*;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.postgresql.ds.PGConnectionPoolDataSource;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import ru.living.loader2gis.model.*;
import ru.living.loader2gis.runnable.CreateInfrastructureComplexRunnable;
import ru.living.loader2gis.runnable.SearchInfrastructureRunnable;
import ru.living.loader2gis.runnable.model.SearchInfrastructureParameters;

import java.util.*;
import java.util.Date;

public class Service {

    protected final int MAX_THREADS = 30;



    protected JedisPool jedisPool = null;

    protected PGConnectionPoolDataSource dbConnPool = null;

    protected JedisPool getJedisPool() throws Exception
    {
        if (this.jedisPool == null) {

            String host = Config.getInstance().redis.server;
            int port = Config.getInstance().redis.port;

            this.jedisPool = new JedisPool(host, port);
        }
        return this.jedisPool;
    }


    protected PGConnectionPoolDataSource getDbConnPool() throws Exception
    {
        if (this.dbConnPool == null) {

            Config conf = Config.getInstance();

            this.dbConnPool = new PGConnectionPoolDataSource();
            this.dbConnPool.setUser(conf.db.login);
            this.dbConnPool.setPassword(conf.db.password);
            this.dbConnPool.setDatabaseName(conf.db.db_name);
            this.dbConnPool.setPortNumber(conf.db.port);
            this.dbConnPool.setServerName(conf.db.server);
        }
        return this.dbConnPool;
    }

    public void execute() throws Exception
    {
//        this.clearBuff(Infrastructure.getKeyPrefix());
//        this.uploadInfrastructureIntoBuff();
//        this.searchInfrastructure();
//        this.saveInfrastructureToDb();
//
//        this.clearBuff(InfrastructureComplex.getKeyPrefix());
//        this.uploadInfrastructureComplexesIntoBuff();
//        this.createInfrastructureComplex();
        this.saveInfrastructureComplexToDb();

    }


    protected void createInfrastructureComplex() throws Exception
    {
        ArrayList<Infrastructure> infrastructure = this.getInfrastructureFromBuff();

        Queue<Complex> complexes = new PriorityQueue<Complex>();
        Complex complex;
        int number;

        Connection conn = this.getDbConnPool().getPooledConnection().getConnection();
        Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery("SELECT a1.id AS complex_id, a2.kladr AS city_id, a1.latitude, a1.longitude FROM complex a1 INNER JOIN city a2 ON a1.city_id = a2.id WHERE a1.is_active AND a1.longitude IS NOT NULL AND a1.latitude IS NOT NULL");
        while (rs.next()) {
            Complex obj = new Complex();
            obj.complex_id = rs.getInt("complex_id");
            obj.city_id = rs.getInt("city_id");
            obj.latitude = rs.getDouble("latitude");
            obj.longitude = rs.getDouble("longitude");
            complexes.add(obj);
        }

        rs.close();
        conn.close();

        int[] semaphore = new int[MAX_THREADS];
        for (number = 0; number < MAX_THREADS; number++) {
            semaphore[number] = 0;
        }

        boolean repeat;
        while ( (complex = complexes.poll()) != null) {

            repeat = true;
            while (repeat) {
                for (number = 0; number < MAX_THREADS; number++) {
                    if (semaphore[number] != 1) {
                        semaphore[number] = 1;
                        repeat = false;
                        new Thread(new CreateInfrastructureComplexRunnable(number, semaphore, this.getJedisPool(), complex, infrastructure)).start();
                        break;
                    }
                }
                if (repeat) {
                    Thread.sleep(1000);
                }
            }
        }

        this.testSemaphore(semaphore);

    }

    protected void testSemaphore(int[] semaphore) throws Exception
    {
        int cnt = semaphore.length;
        boolean repeat;
        int number;
        do {
            repeat = false;
            for (number = 0; number < cnt; number++) {
                if (semaphore[number] == 1) {
                    repeat = true;
                    break;
                }
            }

            if (repeat) Thread.sleep(1000);
        } while (repeat);
    }


    protected void saveInfrastructureComplexToDb() throws Exception
    {
        Integer ins = 0;
        Integer upd = 0;
        Integer del = 0;

        String cmdIns = "INSERT INTO public.infrastructure_complex (complex_id, infrastructure_id, living_category_id, distance, distance_walking, duration_walking, distance_transit, duration_transit, distance_driving, duration_driving, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
        String cmdUpd = "UPDATE public.infrastructure_complex SET complex_id = ?, infrastructure_id = ?, living_category_id = ?, distance = ?, distance_walking = ?, duration_walking = ?, distance_transit = ?, duration_transit = ?, distance_driving = ?, duration_driving = ?, updated_at = ? WHERE id = ?";
        String cmdDel = "DELETE FROM infrastructure_complex WHERE id = ?";

        Connection conn = this.getDbConnPool().getPooledConnection().getConnection();
        Jedis jedis = this.getJedisPool().getResource();

        PreparedStatement stmtIns = conn.prepareStatement(cmdIns, Statement.RETURN_GENERATED_KEYS);
        PreparedStatement stmtUpd = conn.prepareStatement(cmdUpd);
        PreparedStatement stmtDel = conn.prepareStatement(cmdDel);


        long createdAt = Math.round(new java.util.Date().getTime() / 1000);
        long updatedAt = createdAt;

        ResultSet ids;

        Set<String> keys = jedis.keys(InfrastructureComplex.getKeyPrefix() + ":*");
        InfrastructureComplex obj;
        for (String key: keys) {

            obj = InfrastructureComplex.getFromBuff(jedis, key);

            if (obj.checked == null || obj.checked == false) {
                // Надо удалить
                stmtDel.clearParameters();
                if (obj.id != null) {
                    stmtDel.setInt(1, obj.id);
                    stmtDel.executeUpdate();
                }
                jedis.del(obj.getKey());

                del++;
            } else {
                if (obj.id == null) {
                    // /Надо добавить
                    stmtIns.setObject(1, obj.complex_id);
                    stmtIns.setObject(2, obj.infrastructure_id);
                    stmtIns.setObject(3, obj.living_category_id);
                    stmtIns.setObject(4, obj.distance);
                    stmtIns.setObject(5, obj.distance_walking);
                    stmtIns.setObject(6, obj.duration_walking);
                    stmtIns.setObject(7, obj.distance_transit);
                    stmtIns.setObject(8, obj.duration_transit);
                    stmtIns.setObject(9, obj.distance_driving);
                    stmtIns.setObject(10, obj.duration_driving);
                    stmtIns.setObject(11, createdAt);
                    stmtIns.setObject(12, updatedAt);
                    stmtIns.executeUpdate();

                    ids = stmtIns.getGeneratedKeys();
                    ids.next();
                    obj.id = ids.getInt("id");
                    ids.close();

                    jedis.hmset(obj.getKey(), obj.toHashMap());

                    ins++;
                } else {
                    // Надо обновить
                    stmtUpd.clearParameters();
                    stmtUpd.setObject(1, obj.complex_id);
                    stmtUpd.setObject(2, obj.infrastructure_id);
                    stmtUpd.setObject(3, obj.living_category_id);
                    stmtUpd.setObject(4, obj.distance);
                    stmtUpd.setObject(5, obj.distance_walking);
                    stmtUpd.setObject(6, obj.duration_walking);
                    stmtUpd.setObject(7, obj.distance_transit);
                    stmtUpd.setObject(8, obj.duration_transit);
                    stmtUpd.setObject(9, obj.distance_driving);
                    stmtUpd.setObject(10, obj.duration_driving);
                    stmtUpd.setObject(11, updatedAt);
                    stmtUpd.setObject(12, obj.id);
                    stmtUpd.executeUpdate();

                    upd++;
                }
            }
        }
        System.out.println(String.format("ins = %d\nupd = %d\ndel = %d", ins, upd, del));
    }


    protected void saveInfrastructureToDb() throws Exception
    {
        Integer ins = 0;
        Integer upd = 0;
        Integer del = 0;

        String cmdIns = "INSERT INTO infrastructure (external_id, living_category_id, name, latitude, longitude, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ? )";
        String cmdUpd = "UPDATE infrastructure SET external_id = ?, living_category_id = ?, name = ?, latitude = ?, longitude = ?, updated_at = ? WHERE id = ?";
        String cmdDel = "DELETE FROM infrastructure WHERE id = ?";
        String cmdICDel = "DELETE FROM infrastructure_complex WHERE infrastructure_id = ?";

        Connection conn = this.getDbConnPool().getPooledConnection().getConnection();
        Jedis jedis = this.getJedisPool().getResource();

        PreparedStatement stmtIns = conn.prepareStatement(cmdIns, Statement.RETURN_GENERATED_KEYS);
        PreparedStatement stmtUpd = conn.prepareStatement(cmdUpd);
        PreparedStatement stmtDel = conn.prepareStatement(cmdDel);
        PreparedStatement stmtICDel = conn.prepareStatement(cmdICDel);


        long createdAt = Math.round(new java.util.Date().getTime() / 1000);
        long updatedAt = createdAt;

        ResultSet ids;
        Set<String> keys = jedis.keys(Infrastructure.getKeyPrefix() + ":*");
        Infrastructure obj;

        for (String key: keys) {

            obj = Infrastructure.getFromBuff(jedis, key);

            if (obj.city_id == null) {
                // Надо удалить
                stmtDel.clearParameters();
                stmtICDel.clearParameters();

                if (obj.id != null) {
                    stmtICDel.setInt(1, obj.id);
                    stmtICDel.executeUpdate();
                }
                stmtDel.setInt(1, obj.id);
                stmtDel.executeUpdate();

                jedis.del(obj.getKey());

                del++;
            } else {
                if (obj.id == null) {
                    // /Надо добавить
                    stmtIns.clearParameters();
                    stmtIns.setObject(1, obj.external_id);
                    stmtIns.setObject(2, obj.living_category_id);
                    stmtIns.setObject(3, obj.name);
                    stmtIns.setObject(4, obj.latitude);
                    stmtIns.setObject(5, obj.longitude);
                    stmtIns.setObject(6, createdAt);
                    stmtIns.setObject(7, updatedAt);
                    stmtIns.executeUpdate();

                    ids = stmtIns.getGeneratedKeys();
                    ids.next();
                    obj.id = ids.getInt("id");
                    ids.close();

                    jedis.hmset(obj.getKey(), obj.toHashMap());

                    ins++;
                } else {
                    // Надо обновить
                    stmtUpd.clearParameters();
                    stmtUpd.setObject(1, obj.external_id);
                    stmtUpd.setObject(2, obj.living_category_id);
                    stmtUpd.setObject(3, obj.name);
                    stmtUpd.setObject(4, obj.latitude);
                    stmtUpd.setObject(5, obj.longitude);
                    stmtUpd.setObject(6, updatedAt);
                    stmtUpd.setObject(7, obj.id);
                    stmtUpd.executeUpdate();

                    upd++;
                }
            }
        }
        System.out.println(String.format("ins = %d\nupd = %d\ndel = %d", ins, upd, del));
    }

    protected ArrayList<Infrastructure> getInfrastructureFromBuff() throws Exception
    {
        Jedis jedis = this.getJedisPool().getResource();
        Set<String> keys = jedis.keys(Infrastructure.getKeyPrefix() + ":*");
        Infrastructure obj;
        ArrayList<Infrastructure> result = new ArrayList<Infrastructure>();
        for (String key: keys) {
            obj = Infrastructure.getFromBuff(jedis, key);
            result.add(obj);
        }
        return result;
    }

    protected ArrayList<InfrastructureComplex> getInfrastructureComplexFromBuff() throws Exception
    {
        Jedis jedis = this.getJedisPool().getResource();
        Set<String> keys = jedis.keys(InfrastructureComplex.getKeyPrefix() + ":*");
        InfrastructureComplex obj;
        ArrayList<InfrastructureComplex> result = new ArrayList<InfrastructureComplex>();
        for (String key: keys) {
            obj = InfrastructureComplex.getFromBuff(jedis, key);
            result.add(obj);
        }
        return result;
    }


    protected ArrayList<Complex> getComplexFromBuff() throws Exception
    {
        return this.getComplexFromBuffWithPattern("*");
    }

    protected ArrayList<Complex> getComplexFromBuff(Integer cityId) throws Exception
    {
        return this.getComplexFromBuffWithPattern(cityId.toString() + ":*");
    }

    protected ArrayList<Complex> getComplexFromBuffWithPattern(String pattern) throws Exception
    {
        Jedis jedis = this.getJedisPool().getResource();
        Set<String> keys = jedis.keys(Complex.getKeyPrefix() + ":" + pattern);
        Complex obj;
        List<String> fields;
        HashMap<String, String> data;
        ArrayList<Complex> result = new ArrayList<Complex>();
        for (String key: keys) {
            data = new HashMap<String, String>();
            fields = jedis.hmget(key, "complex_id", "city_id", "latitude", "longitude");
            data.put("complex_id", fields.get(0));
            data.put("city_id", fields.get(1));
            data.put("latitude", fields.get(2));
            data.put("longitude", fields.get(3));

            obj = new Complex(data);
            result.add(obj);
        }
        return result;
    }





    protected void searchInfrastructure() throws Exception
    {
        Config conf = Config.getInstance();
        Queue<SearchInfrastructureParameters> parameters = new PriorityQueue<SearchInfrastructureParameters>();
        SearchInfrastructureParameters p;

        int number = 0;
        for (Config.Category category: conf.categories) {
            for (int rubricId: category.rubrics) {
                for (Config.City city: conf.cities) {
                    p = new SearchInfrastructureParameters();
                    p.cityId = city.city_id;
                    p.livingCategoryId = category.living_category_id;
                    p.rubricId = rubricId;
                    p.longitude = city.longitude;
                    p.latitude = city.latitude;
                    p.radius = city.radius;

                    parameters.add(p);
                    number++;
                }
            }
        }

        int[] semaphore = new int[MAX_THREADS];
        for (number = 0; number < MAX_THREADS; number++) {
            semaphore[number] = 0;
        }

        System.out.println(String.format("Надо загрузить\t%d", parameters.size()));

        boolean repeat;
        while ( (p = parameters.poll()) != null) {

            System.out.println(String.format("Осталось загрузить\t%d", parameters.size()));

            repeat = true;
            while (repeat) {
                for (number = 0; number < MAX_THREADS; number++) {
                    if (semaphore[number] != 1) {
                        semaphore[number] = 1;
                        repeat = false;
                        new Thread(new SearchInfrastructureRunnable(number, semaphore, this.getJedisPool(), p)).start();
                        break;
                    }
                }
                if (repeat) {
                    Thread.sleep(1000);
                }
            }
        }

        this.testSemaphore(semaphore);

    }


    public static void varDump(Object var)
    {
        Gson gson = new GsonBuilder().create();
        System.out.println(gson.toJson(var));
    }

    protected void clearBuff(String keyPrefix) throws Exception
    {
        Jedis jedis = this.getJedisPool().getResource();
        Set<String> allKeys = jedis.keys(keyPrefix + ":*");
        for(String key: allKeys) {
            jedis.del(key);
        }

    }


    protected void uploadInfrastructureIntoBuff() throws Exception
    {
        Jedis jedis = this.getJedisPool().getResource();

        Connection conn = this.getDbConnPool().getPooledConnection().getConnection();

        Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery("SELECT * FROM Infrastructure");

        while (rs.next()) {
            Infrastructure obj = new Infrastructure();
            obj.id = rs.getInt("id");
            obj.external_id = rs.getString("external_id");
            obj.name = rs.getString("name");
            obj.living_category_id = rs.getInt("living_category_id");
            obj.latitude = rs.getDouble("latitude");
            obj.longitude = rs.getDouble("longitude");

            jedis.hmset(obj.getKey(), obj.toHashMap());

        }

        rs.close();
        conn.close();
        jedis.close();
    }

    protected void uploadInfrastructureComplexesIntoBuff() throws Exception
    {
        Jedis jedis = this.getJedisPool().getResource();

        Connection conn = this.getDbConnPool().getPooledConnection().getConnection();

        Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery("SELECT * FROM infrastructure_complex");

        while (rs.next()) {
            InfrastructureComplex obj = new InfrastructureComplex();
            obj.id = rs.getInt("id");
            obj.complex_id = rs.getInt("complex_id");
            obj.infrastructure_id = rs.getInt("infrastructure_id");
            obj.living_category_id = rs.getInt("living_category_id");
            obj.distance = rs.getLong("distance");
            obj.distance_walking = rs.getLong("distance_walking");
            obj.duration_walking = rs.getLong("duration_walking");
            obj.distance_transit = rs.getLong("distance_transit");
            obj.duration_transit = rs.getLong("duration_transit");
            obj.distance_driving = rs.getLong("distance_driving");
            obj.duration_driving = rs.getLong("duration_driving");

            jedis.hmset(obj.getKey(), obj.toHashMap());
        }

        rs.close();
        conn.close();
        jedis.close();
    }
}
