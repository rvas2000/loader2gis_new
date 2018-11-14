package ru.living.loader2gis.api;

import com.google.gson.stream.JsonReader;
import org.apache.http.Header;
import org.apache.http.message.BasicHeader;
import ru.living.loader2gis.api.model.SearchResponse;
import org.apache.http.client.config.CookieSpecs;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;

import java.io.InputStreamReader;
import java.net.URI;
import java.util.ArrayList;

import ru.living.loader2gis.api.model.*;
import com.google.gson.GsonBuilder;
import com.google.gson.Gson;

public class Api {

    protected Gson gson = null;

    protected String host = "catalog.api.2gis.ru";

    protected Gson getGson()
    {
        if (this.gson == null) {
            this.gson = new GsonBuilder().create();
        }
        return this.gson;
    }


    protected CloseableHttpClient getHttpClient()
    {
            ArrayList<Header> headers = new ArrayList<Header>();
            headers.add(new BasicHeader("User-Agent", "lolo"));

            return HttpClientBuilder
                    .create()
                    .setDefaultRequestConfig(RequestConfig.custom().setCookieSpec(CookieSpecs.STANDARD).build())
                    .setDefaultHeaders(headers)
                    .build();
    }


    public SearchResponse.Result searchObjects(SearchRequest params) throws Exception
    {
        String path = "/2.0/catalog/branch/search";

        URI uri = new URIBuilder()
                .setScheme("http")
                .setHost(this.host)
                .setPath(path)
                .setParameters(params.getParamsList())
                .build();

        HttpUriRequest request = new HttpGet(uri);

        CloseableHttpClient client;
        CloseableHttpResponse response;
        int statusCode;
        SearchResponse result;
        SearchResponse.Result ret = null;
        int attempts = 0;
        boolean repeat;

        do {
            repeat = false;
            try {
                client = this.getHttpClient();
                response = client.execute(request);
                statusCode = response.getStatusLine().getStatusCode();

                if (statusCode == 200 ) {
                    result = this.getGson().fromJson(new JsonReader(new InputStreamReader(response.getEntity().getContent())), SearchResponse.class);
                    if (result.meta.error == null) {
                        ret = result.result;
                    } else {
                        if (result.meta.error.type.equalsIgnoreCase("ItemNotFound")) {
                            ret = new SearchResponse().result;
                        } else {
                            throw new Exception(result.meta.error.message);
                        }
                    }
                } else {
                    throw new Exception(String.format("Ошибка с кодом %d", statusCode));
                }

            } catch (Exception e) {
                attempts++;
                if (attempts == 3){
                    throw e;
                } else {
                    repeat = true;
                }
            }

        } while (repeat);

        return ret;
    }

}
