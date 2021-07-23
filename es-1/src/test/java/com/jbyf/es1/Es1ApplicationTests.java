package com.jbyf.es1;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.jbyf.es1.bean.Goods;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.CreateIndexResponse;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.terms.ParsedStringTerms;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.metrics.avg.ParsedAvg;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightField;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@SpringBootTest
class Es1ApplicationTests {
    @Autowired
    RestHighLevelClient client ;

    @Test
    void test11(){
        System.out.println("sss");
    }

    @Test
    void contextLoads() {
        //创建映射
        try {
            CreateIndexRequest indexRequest = new CreateIndexRequest("atguigu5");
            indexRequest.mapping("{\"properties\":{\"id\":{\"type\":\"long\"} , \"name\":{\"type\":\"text\", \"analyzer\":\"ik_max_word\"}}}",XContentType.JSON);
            CreateIndexResponse response = client.indices().create(indexRequest, RequestOptions.DEFAULT);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    //查询某一个文档
    @Test
    public void testGetDoc() throws IOException{
        GetRequest getRequest= new GetRequest("atguigu","goods","4");

        GetResponse response = client.get(getRequest, RequestOptions.DEFAULT);
        Map<String, Object> sourceAsMap = response.getSourceAsMap();
        ObjectMapper objectMapper =new ObjectMapper();
        Goods goods = objectMapper.convertValue(sourceAsMap, Goods.class);
        System.out.println("goods = " + goods);
    }


    //查询所有文档
    @Test
    public void testQuery() throws IOException {
        SearchRequest searchRequest = new SearchRequest();
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        MatchAllQueryBuilder queryBuilder = new MatchAllQueryBuilder();


        searchRequest.indices("atguigu");
        searchRequest.types("goods");


        sourceBuilder.query(queryBuilder);
        searchRequest.source(sourceBuilder);

        SearchResponse response = client.search(searchRequest, RequestOptions.DEFAULT);

        //从结果集中获取hit
        SearchHit[] hits = response.getHits().getHits();
        //遍历hit
        for (SearchHit hit : hits) {
            String sourceAsString = hit.getSourceAsString();
                System.out.println("sourceAsString = " + sourceAsString);
            }
        }




    //高亮查询
    @Test
    public void testQueryHighLight() throws IOException {
        //需要查询的数据
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.query(QueryBuilders.matchQuery("title","小米手机"));

        //要高亮显示的字段
        HighlightBuilder highlightBuilder = new HighlightBuilder();
        highlightBuilder.field("title");
        highlightBuilder.preTags("<em>");
        highlightBuilder.postTags("</em>");
        sourceBuilder.highlighter(highlightBuilder);

        System.out.println(sourceBuilder);

        SearchRequest searchRequest = new SearchRequest("atguigu");
        searchRequest.source(sourceBuilder);
        SearchResponse response = client.search(searchRequest, RequestOptions.DEFAULT);

        SearchHit[] hits = response.getHits().getHits();
        for (SearchHit hit : hits) {
            System.out.println("_---_--_start_---_---_----");

            //获取高亮字段
            HighlightField highlightField = hit.getHighlightFields().get("title");
            System.out.println("highlightField.getFragments() = " +
                    Arrays.toString(highlightField.getFragments()));

            System.out.println("_---_--_-----_end_---_----");


        }

    }

    //聚合查询
    @Test
    public void testAgg() throws IOException {
            SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
            //聚合查询
            //terms() 的形参: 聚合后的组名
            sourceBuilder.aggregation(AggregationBuilders.terms("brands")
                                                        .field("attr.brand.keyword"));


            SearchRequest searchRequest = new SearchRequest("atguigu");
            searchRequest.source(sourceBuilder);

            SearchResponse response = client.search(searchRequest, RequestOptions.DEFAULT);
            //解析结果集

            //获取查询结果集
    //        response.getHits();

            //获取聚合的分组结果
            Map<String, Aggregation> map = response.getAggregations().asMap();
            ParsedStringTerms parsedStringTerms = (ParsedStringTerms) map.get("brands");

        List<? extends Terms.Bucket> buckets = parsedStringTerms.getBuckets();
        buckets.forEach(bucket->{
            System.out.println("------start---------");

            System.out.println("文档数量 = " + bucket.getDocCount());
            System.out.println("bucket.getKeyAsString() = " + bucket.getKeyAsString());

            System.out.println("-------end----------");
        });

    }

    //桶内度量
    @Test
    public void testAggAndMetric() throws IOException{
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();

        AggregationBuilder metricAgg = AggregationBuilders
                                        .avg("avg_price")//聚合后的名字
                                        .field("price");//子聚合的分组字段

        AggregationBuilder agg = AggregationBuilders.terms("brands")
                                                    .field("attr.brand.keyword")//分组字段名
                                                    .size(500)//表示分组时最大的记录条数
                                                    .subAggregation(metricAgg)//使用子聚合对前面的结果继续分组
        ;

        sourceBuilder.aggregation(agg);

        SearchRequest searchRequest = new SearchRequest("atguigu");
        searchRequest.source(sourceBuilder);
        SearchResponse response = client.search(searchRequest, RequestOptions.DEFAULT);

        //获取分组结果集
        // ParsedStringTerms 类代表根据词条分组之后的结果
        // Aggregation       类代表最后的分组的结果
        ParsedStringTerms parsedStringTerms = (ParsedStringTerms) response.getAggregations().asMap().get("brands");
        //获取桶
        List<? extends Terms.Bucket> buckets = parsedStringTerms.getBuckets();
        buckets.forEach(bucket->{
            System.out.println("------------------------------start-------------------------");

            System.out.println("词条: " + bucket.getKeyAsString());
            System.out.println("桶内所有文档数量: " + bucket.getDocCount());
            //获取桶内的度量结果
            ParsedAvg avg_price = (ParsedAvg) bucket.getAggregations().asMap().get("avg_price");
            System.out.println("本次操作的类型 : avg_price.getType() = " + avg_price.getType());
            System.out.println("本次操作的结果值 : avg_price.getValue() = " + avg_price.getValue());


            System.out.println("-------------------------------end--------------------------");
        });
    }

    //判断文档是否存再
    @Test
    public void testIsExists() throws IOException {
        GetRequest getRequest = new GetRequest("atguigu","goods","2");
        boolean exists = client.exists(getRequest, RequestOptions.DEFAULT);
        System.out.println(exists);
    }

    //删除文档
    @Test
    public void testDelete() throws IOException {
        DeleteRequest deleteRequest = new DeleteRequest("atguigu","goods","2");

        DeleteResponse response = client.delete(deleteRequest, RequestOptions.DEFAULT);
        System.out.println(response.status());
    }

    //更新文档
    @Test
    public void testUpdte() throws IOException{
        UpdateRequest updateRequest = new UpdateRequest("atguigu","goods","4");
        Map map = new HashMap();
        map.put("price","5999");
        map.put("stock","1000");
        updateRequest.doc(map,XContentType.JSON);
        UpdateResponse response = client.update(updateRequest, RequestOptions.DEFAULT);
        System.out.println(response.status());
    }




}





















