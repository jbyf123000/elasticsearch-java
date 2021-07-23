package com.jbyf.es1;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.gson.Gson;
import com.jbyf.es1.bean.Goods;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.*;
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
import org.elasticsearch.search.sort.SortOrder;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.json.GsonTester;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@SpringBootTest
public class Es2Test {
    @Autowired
    RestHighLevelClient client;

    //同步插入文档
    @Test
    public void testCreateDoc() throws IOException{
        IndexRequest indexRequest = new IndexRequest();
        indexRequest.index("guigu");
        indexRequest.type("house");

        //把需要新增的文档用map封装
        Map map = new HashMap();
        map.put("id","1002");
        map.put("address","圣来圣2223");
        Map userMap = new HashMap();
        userMap.put("id","9520");
        userMap.put("username","NM$L");
        map.put("user",userMap);

        indexRequest.source(map, XContentType.JSON);

        IndexResponse response = client.index(indexRequest, RequestOptions.DEFAULT);
        DocWriteResponse.Result result = response.getResult();
        System.out.println(result);


    }

    //异步插入文档
    @Test
    public void testAsyncInsert(){
        IndexRequest indexRequest =  new IndexRequest("atguigu","goods","932");
        Map goods = new HashMap();
        Map goods_attr = new HashMap();
        goods.put("title","一加手机");
        goods.put("stock",200);
        goods.put("price",4999);
        goods.put("images","https://img14.360buyimg.com/n0/jfs/t1/114704/10/2962/173036/5ea4f4f4E943ddac6/75693e49b29e29d9.jpg");
        goods.put("attr",goods_attr);
        goods_attr.put("brand","一加");
        goods_attr.put("category","手机");

        indexRequest.source(goods , XContentType.JSON);
        ActionListener<IndexResponse> listener = new ActionListener<IndexResponse>() {
            //新增数据成功的回调
            @Override
            public void onResponse(IndexResponse indexResponse) {
                System.out.println("文档新增成功："+ indexResponse.toString());
            }

            //新增失败的回调
            @Override
            public void onFailure(Exception e) {
                System.out.println("文档新增失败："+ e.getMessage());
            }
        };


        client.indexAsync(indexRequest,RequestOptions.DEFAULT,listener);
        System.out.println("...");
        while (true){

        }
    }


    //判断文档是否存在
    public boolean isExists(String id) throws IOException {
        GetRequest getRequest = new GetRequest("atguigu","goods",id);
//        GetRequest getRequest = new GetRequest("guli","house",id);
        boolean exists = client.exists(getRequest, RequestOptions.DEFAULT);
        return exists;
    }

    //删除文档
    @Test
    public void deleteDoc() throws IOException{
        String id = "4";
        System.out.println("isExists(id) = " + isExists(id));

        DeleteRequest deleteRequest = new DeleteRequest("atguigu","goods",id);
        DeleteResponse delete = client.delete(deleteRequest, RequestOptions.DEFAULT);
        System.out.println("delete.status() = " + delete.status());

        System.out.println("isExists(id) = " + isExists(id));
    }

    //更新文档
    @Test
    public void testUpdate() throws IOException{
        UpdateRequest updateRequest = new UpdateRequest("atguigu","goods","932");
        Map map = new HashMap();
        map.put("price",4599);
        map.put("stock",2000);

        updateRequest.doc(map,XContentType.JSON);

        UpdateResponse response = client.update(updateRequest, RequestOptions.DEFAULT);
        System.out.println("response.status() = " + response.status());
    }

    //查询
    //根据id查询一条记录
    @Test
    public void testGetMatch() throws IOException{
        GetRequest getRequest = new GetRequest();
        getRequest.index("atguigu");
        getRequest.type("goods");
        getRequest.id("9");

        GetResponse response = client.get(getRequest, RequestOptions.DEFAULT);
        Map<String, Object> sourceAsMap = response.getSourceAsMap();
        ObjectMapper mapper = new ObjectMapper();
        Goods goods = mapper.convertValue(sourceAsMap, Goods.class);
        System.out.println("goods = " + goods);

    }

    //查询所有
    @Test
    public void testMatchAll() throws IOException {
        SearchRequest searchRequest = new SearchRequest();
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        QueryBuilder query = new MatchAllQueryBuilder();

        searchRequest.indices("atguigu");
        searchRequest.types("goods");

        searchSourceBuilder.query(query);
        searchRequest.source(searchSourceBuilder);

        SearchResponse response = client.search(searchRequest, RequestOptions.DEFAULT);

        SearchHit[] hits = response.getHits().getHits();
        for (SearchHit hit : hits) {
            System.out.println("hit.getSourceAsString() = " + hit.getSourceAsString());
        }

    }

    //条件查询
    @Test
    public void testByConditions() throws IOException{
        SearchRequest searchRequest = new SearchRequest();
        QueryBuilder queryBuilder = new MatchQueryBuilder("title","小米");


        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();

        sourceBuilder.query(queryBuilder);
        searchRequest.source(sourceBuilder);

        SearchResponse response = client.search(searchRequest, RequestOptions.DEFAULT);
        SearchHit[] hits = response.getHits().getHits();

        for (SearchHit hit : hits) {
            String sourceAsString = hit.getSourceAsString();
            System.out.println("sourceAsString = " + sourceAsString);
        }
    }

    //分页查询
    @Test
    public void testPageQuery() throws IOException {
        SearchRequest searchRequest = new SearchRequest("atguigu");
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();

        sourceBuilder.query(QueryBuilders.matchQuery("title","小米手机"));
        sourceBuilder.from(0);
        sourceBuilder.size(2);

        sourceBuilder.sort("price", SortOrder.DESC);
        //要保留在结果中的字段列表
        String includes[] = new String[]{
                "title","price","attr.brand"
        };
        //需要排除出结果的字段:
        String[] excludes = Strings.EMPTY_ARRAY;

        sourceBuilder.fetchSource(includes,excludes);


        searchRequest.source(sourceBuilder);

        SearchResponse response = client.search(searchRequest, RequestOptions.DEFAULT);
        SearchHit[] hits = response.getHits().getHits();

        for (SearchHit hit : hits) {
            System.out.println("hit.getSourceAsString() = " + hit.getSourceAsString());
        }
    }

    //高亮查询
    @Test
    public void testHighLight() throws IOException{
        SearchRequest searchRequest = new SearchRequest("atguigu");
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.query(QueryBuilders.matchQuery("title","小米手机"));

        HighlightBuilder highlightBuilder = new HighlightBuilder();
        highlightBuilder.field("title");//需要高亮显示的字段
        highlightBuilder.preTags("<em>");//高亮前缀
        highlightBuilder.postTags("</em>");//高亮前缀


        sourceBuilder.highlighter(highlightBuilder);
        searchRequest.source(sourceBuilder);

        SearchResponse search = client.search(searchRequest, RequestOptions.DEFAULT);
        SearchHit[] hits = search.getHits().getHits();

        for (SearchHit hit : hits) {
            System.out.println("--------------------=start=-----------------------");
            Goods goods = new Gson().fromJson(hit.getSourceAsString(), Goods.class);

            HighlightField highlightField =hit.getHighlightFields().get("title");
            System.out.println(Arrays.toString(highlightField.getFragments()));
            goods.setHighLightTitle(Arrays.toString(highlightField.getFragments()));

            System.out.println("获取到的goods对象："+ goods);


            System.out.println("---------------------=end=------------------------");
        }
    }

    //聚合查询
    //聚合条件的封装,聚合结果的解析
    @Test
    public void aggsTest() throws IOException{
        SearchRequest searchRequest = new SearchRequest("atguigu");
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();

        sourceBuilder.aggregation(AggregationBuilders.terms("brands")
                                                     .field("attr.brand.keyword"));
        searchRequest.source(sourceBuilder);

        SearchResponse response = client.search(searchRequest, RequestOptions.DEFAULT);

        //解析结果集
        Map<String, Aggregation> map = response.getAggregations().asMap();
        ParsedStringTerms brands = (ParsedStringTerms) map.get("brands");
        List<? extends Terms.Bucket> buckets = brands.getBuckets();
        buckets.forEach(bucket->{
            System.out.println("------start---------");

            System.out.println("文档数量 = " + bucket.getDocCount());
            System.out.println("bucket.getKeyAsString() = " + bucket.getKeyAsString());

            System.out.println("-------end----------");
        });
    }

    //桶内度量
    /*
    # 聚合查询
        GET atguigu/_search
        {
          "size":0,
          "aggs": {
            "brands": {
              "terms": {
                "field": "attr.brand.keyword"
              },
              "aggs": {
                "avg_price": {
                  "avg": {
                    "field": "price"
                  }
                }
              }
            }
          }
        }
     */
    @Test
    public void subAgg() throws IOException{
        SearchRequest searchRequest = new SearchRequest("atguigu");
        SearchSourceBuilder source = new SearchSourceBuilder();

        AggregationBuilder subAggr = AggregationBuilders.avg("avg_price")
                                                        .field("price");
        AggregationBuilder aggs = AggregationBuilders.terms("brands")
                                                    .field("attr.brand.keyword")
                                                    .subAggregation(subAggr);
        source.aggregation(aggs);
        searchRequest.source(source);

        SearchResponse response = client.search(searchRequest, RequestOptions.DEFAULT);
        Map<String, Aggregation> map = response.getAggregations().asMap();

        ParsedStringTerms parsedStringTerms = (ParsedStringTerms) map.get("brands");

        List<? extends Terms.Bucket> buckets = parsedStringTerms.getBuckets();

        buckets.forEach(bucket->{
            System.out.println("------------------------------start-------------------------");

            System.out.println("词条 = " + bucket.getKeyAsString());
            System.out.println("本次查询桶内所有文档的数量 = " + bucket.getDocCount());

            //获取桶内度量结果
            ParsedAvg avg = (ParsedAvg)bucket.getAggregations().asMap().get("avg_price");
            System.out.println("avg.getType() = " + avg.getType());
            System.out.println("avg.getValue() = " + avg.getValue());

            System.out.println("-------------------------------end--------------------------");
        });


    }

}
