package ru.living.loader2gis.api.model;

public class SearchResponse {

    public Meta meta;

    public Result result;

    public SearchResponse()
    {
        this.result = new Result();
        this.result.total = 0;
        this.result.items = new ResultItem[0];

    }

    public class Meta
    {
        public String api_version;
        public String issue_date;
        public Integer code;
        public Error error;
    }

    public class Error
    {
        public String type;
        public String message;
    }

    public class Result
    {
        public Integer total;
        public ResultItem[] items;
    }

    public class Point
    {
        public Double lat;
        public Double lon;
    }

    public class ResultItem
    {
        public String name;
        public Point point;
        public String id;
        public String address_comment;
        public String address_name;
        public String type;
    }
}
