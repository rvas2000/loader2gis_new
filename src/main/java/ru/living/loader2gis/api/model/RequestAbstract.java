package ru.living.loader2gis.api.model;

import org.apache.http.NameValuePair;

import java.util.ArrayList;
import java.util.List;

abstract public class RequestAbstract {

    public String key = "rudvch1667";


    protected class NameValue implements NameValuePair
    {
        private String name;
        private String value;

        public NameValue(String name, String value)
        {
            this.name = name;
            this.value = value;

        }

        public String getName()
        {
            return this.name;
        }

        public String getValue()
        {
            return this.value;
        }
    }



    public List<NameValuePair> getParamsList()
    {
        ArrayList<NameValuePair> result = new ArrayList<NameValuePair>();
        if (this.key != null) result.add(new SearchRequest.NameValue("key", this.key));
        return result;
    };

}
