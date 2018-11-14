package ru.living.loader2gis.api.model;

import org.apache.http.NameValuePair;
import java.util.ArrayList;
import java.util.List;

public class SearchRequest extends RequestAbstract{
    public String format = "json";
    public Integer page_size = 50;
    public String point;
    public Integer radius;
    public String fields = "items.point";
    public Integer rubric_id;
    public Integer page = 1;


    public SearchRequest setPoint(Double latitude, Double longitude)
    {
        this.point = longitude.toString() + "," + latitude.toString();
        return this;
    }


    public List<NameValuePair> getParamsList()
    {
        ArrayList<NameValuePair> result = new ArrayList<NameValuePair>();
        if (this.key != null) result.add(new SearchRequest.NameValue("key", this.key));
        if (this.format != null) result.add(new SearchRequest.NameValue("format", this.format));
        if (this.page_size != null) result.add(new SearchRequest.NameValue("page_size", this.page_size.toString()));
        if (this.point != null) result.add(new SearchRequest.NameValue("point", this.point));
        if (this.radius != null) result.add(new SearchRequest.NameValue("radius", this.radius.toString()));
        if (this.fields != null) result.add(new SearchRequest.NameValue("fields", this.fields));
        if (this.rubric_id != null) result.add(new SearchRequest.NameValue("rubric_id", this.rubric_id.toString()));
        if (this.page != null) result.add(new SearchRequest.NameValue("page", this.page.toString()));
        return result;
    }

}
