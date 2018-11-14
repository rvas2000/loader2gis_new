package ru.living.loader2gis.runnable.model;

public class SearchInfrastructureParameters implements Comparable<SearchInfrastructureParameters>
{
    public Integer cityId;
    public Integer livingCategoryId;
    public Integer rubricId;
    public Double latitude;
    public Double longitude;
    public Integer radius;

    public int compareTo(SearchInfrastructureParameters obj)
    {
       if (this.livingCategoryId < obj.livingCategoryId) {
           return -1;
       } else {
           if (this.livingCategoryId > obj.livingCategoryId) {
               return 1;
           } else {
               return 0;
           }
       }
    }
}
