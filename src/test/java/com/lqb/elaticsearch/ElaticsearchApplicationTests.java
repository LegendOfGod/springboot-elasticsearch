package com.lqb.elaticsearch;

import com.alibaba.fastjson.JSONObject;
import com.lqb.elaticsearch.dao.BookRepository;
import com.lqb.elaticsearch.entity.Book;
import com.lqb.elaticsearch.entity.Emp;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.*;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightField;
import org.elasticsearch.search.sort.SortOrder;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.domain.Sort;

import javax.annotation.Resource;
import java.io.IOException;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.Date;
import java.util.Map;

@SpringBootTest
class ElaticsearchApplicationTests {

    @Autowired
    private BookRepository bookRepository;

    @Resource
    private RestHighLevelClient restHighLevelClient;

    @Test
    void testSaveOrUpdate() {
        Book book = new Book();
        book.setId("1");
        book.setName("天龙八部");
        book.setAuthor("金庸");
        book.setContent("贪嗔痴恨");
        book.setCreateDate(new Date());

        Book book1 = new Book();
        book1.setId("2");
        book1.setName("斗破苍穹");
        book1.setAuthor("天蚕土豆");
        book1.setContent("佛怒火莲");
        book1.setCreateDate(new Date());

        Book book2 = new Book();
        book2.setId("3");
        book2.setName("effective java");
        book2.setAuthor("java大神");
        book2.setContent("java进阶");
        book2.setCreateDate(new Date());

        Book book3 = new Book();
        book3.setId("0");
        book3.setName("倚天剑和屠龙刀");
        book3.setAuthor("金庸");
        book3.setContent("张无忌和赵敏");
        book3.setCreateDate(new Date());
        bookRepository.save(book);
        bookRepository.save(book1);
        bookRepository.save(book2);
        bookRepository.save(book3);
    }

    @Test
    void testDelete() {
        Book book = new Book();
        book.setId("1");
        bookRepository.delete(book);
    }

    @Test
    void testFindAll() {
        Iterable<Book> all = bookRepository.findAll();
        all.forEach(book -> System.out.println(book));
    }

    @Test
    void testFindAllSort() {
        Iterable<Book> all = bookRepository.findAll(Sort.by(Sort.Order.desc("id")));
        all.forEach(book -> System.out.println(book));
    }

    @Test
    public void testSearchPage() throws IOException {
        SearchRequest searchRequest = new SearchRequest();
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        BoolQueryBuilder boolQueryBuilder1 = QueryBuilders.boolQuery();
        boolQueryBuilder1.should(QueryBuilders.termQuery("author", "天蚕土豆")).should(QueryBuilders.termQuery("name", "天龙八部"));
        BoolQueryBuilder boolQueryBuilder2 = QueryBuilders.boolQuery();
        boolQueryBuilder2.should(boolQueryBuilder1).should(QueryBuilders.idsQuery().addIds("0", "1", "2", "3"));
        sourceBuilder.from(0).size(4).sort("author", SortOrder.ASC).query(boolQueryBuilder2);
        searchRequest.indices("liqiubo").types("book").source(sourceBuilder);
        SearchResponse search = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
        SearchHit[] hits = search.getHits().getHits();
        for (SearchHit hit : hits) {
            System.out.println(hit.getSourceAsString());
        }
    }

    @Test
    public void testTermSearch() throws IOException {
        SearchRequest searchRequest = new SearchRequest();
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        TermQueryBuilder termQueryBuilder = QueryBuilders.termQuery("name", "小黑");
        searchSourceBuilder.from(0).size(2).fetchSource(new String[]{"name"}, new String[0]).query(termQueryBuilder);
        searchRequest.indices("ems").source(searchSourceBuilder);
        SearchResponse search = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
        SearchHit[] hits = search.getHits().getHits();
        for (SearchHit hit : hits) {
            System.out.println(hit);
        }
    }

    @Test
    public void testRangeSearch() throws IOException {
        SearchRequest searchRequest = new SearchRequest();
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        RangeQueryBuilder rangeQueryBuilder = QueryBuilders.rangeQuery("age").gte(43).lte(59);
        searchSourceBuilder.from(0).size(3).sort("age", SortOrder.ASC)
                .fetchSource(new String[]{"name"}, new String[0]).query(rangeQueryBuilder);
        searchRequest.indices("ems").source(searchSourceBuilder);
        SearchResponse searchResponse = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
        SearchHit[] hits = searchResponse.getHits().getHits();
        for (SearchHit hit : hits) {
            System.out.println(hit);
        }
    }

    @Test
    public void testPrefixSearch() throws IOException {
        SearchRequest searchRequest = new SearchRequest();
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        PrefixQueryBuilder prefixQueryBuilder = QueryBuilders.prefixQuery("content", "spring");
        searchSourceBuilder.from(0).size(20).query(prefixQueryBuilder).fetchSource(new String[]{"content"}, new String[0]);
        SearchRequest ems = searchRequest.indices("ems").source(searchSourceBuilder);
        SearchResponse searchResponse = restHighLevelClient.search(ems, RequestOptions.DEFAULT);
        SearchHit[] hits = searchResponse.getHits().getHits();
        for (SearchHit hit : hits) {
            System.out.println(hit.getSourceAsString());
            System.out.println(hit.getId());
        }
    }

    @Test
    public void testWildCardSearch() throws IOException {
        SearchRequest searchRequest = new SearchRequest();
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        WildcardQueryBuilder wildcardQueryBuilder = QueryBuilders.wildcardQuery("content", "re*ful");
        searchSourceBuilder.query(wildcardQueryBuilder).fetchSource(new String[]{"content"}, new String[0]);
        searchRequest = searchRequest.indices("ems").source(searchSourceBuilder);
        SearchResponse searchResponse = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
        SearchHit[] hits = searchResponse.getHits().getHits();
        for (SearchHit hit : hits) {
            System.out.println(hit.getSourceAsString());
            System.out.println(hit.getId());
            System.out.println(hit.getScore());
        }

    }

    @Test
    public void testIdsSearch() throws IOException {
        SearchRequest searchRequest = new SearchRequest();
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        IdsQueryBuilder idsQueryBuilder = QueryBuilders.idsQuery().addIds("vfjhRXkBR71wJ9VCg9f5", "wfjhRXkBR71wJ9VCg9f5");
        searchSourceBuilder.query(idsQueryBuilder).fetchSource(new String[]{"content"}, new String[0]);
        searchRequest = searchRequest.indices("ems").source(searchSourceBuilder);
        SearchResponse searchResponse = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
        SearchHit[] hits = searchResponse.getHits().getHits();
        for (SearchHit hit : hits) {
            System.out.println(hit.getSourceAsString());
            System.out.println(hit.getId());
            System.out.println(hit.getScore());
        }
    }

    @Test
    public void testFuzzySearch() throws IOException {
        SearchRequest searchRequest = new SearchRequest();
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        FuzzyQueryBuilder fuzzyQueryBuilder = QueryBuilders.fuzzyQuery("content", "spring");
        searchSourceBuilder.query(fuzzyQueryBuilder).fetchSource(new String[]{"content"}, new String[0]);
        searchRequest = searchRequest.indices("ems").source(searchSourceBuilder);
        SearchResponse searchResponse = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
        SearchHit[] hits = searchResponse.getHits().getHits();
        for (SearchHit hit : hits) {
            System.out.println(hit.getSourceAsString());
            System.out.println(hit.getId());
            System.out.println(hit.getScore());
        }
    }

    @Test
    public void testStringQuerySearch() throws IOException {
        SearchRequest searchRequest = new SearchRequest();
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        StringBuilder sb = new StringBuilder();
        sb.append("content:re*");
        QueryStringQueryBuilder queryStringQueryBuilder = QueryBuilders.queryStringQuery(sb.toString());
        searchSourceBuilder.query(queryStringQueryBuilder).fetchSource(new String[]{"name", "age", "address", "content"}, new String[0]);
        searchRequest = searchRequest.indices("ems").source(searchSourceBuilder);
        SearchResponse searchResponse = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
        SearchHit[] hits = searchResponse.getHits().getHits();
        for (SearchHit hit : hits) {
            System.out.println(hit.getSourceAsString());
            System.out.println(hit.getId());
            System.out.println(hit.getScore());
        }
    }

    @Test
    public void testMatchSearch() throws IOException {
        SearchRequest searchRequest = new SearchRequest();
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        //MatchAllQueryBuilder matchAllQueryBuilder = QueryBuilders.matchAllQuery();
        MultiMatchQueryBuilder multiMatchQueryBuilder = QueryBuilders.multiMatchQuery("redis", "name", "content");
        searchSourceBuilder.query(multiMatchQueryBuilder).fetchSource(new String[]{"name", "age", "address", "content"}, new String[0]);
        searchRequest.indices("ems").source(searchSourceBuilder);
        SearchResponse searchResponse = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
        SearchHit[] hits = searchResponse.getHits().getHits();
        System.out.println(hits.length);
        for (SearchHit hit : hits) {
            System.out.println(hit.getSourceAsString());
            System.out.println(hit.getId());
            System.out.println(hit.getScore());
        }
    }

    /**
     * 时间范围查询
     *
     * @throws IOException
     */
    @Test
    public void testQueryStringSearch() throws IOException {
        SearchRequest searchRequest = new SearchRequest();
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        //String query = "address:(北京 OR 上海)";
        LocalDate startDate = LocalDate.of(2012, 12, 12);
        LocalDate endDate = LocalDate.of(2012, 12, 13);
        String query = "bir:[" + startDate.atStartOfDay(ZoneId.systemDefault()).toInstant().toEpochMilli() + " TO " +
                endDate.atStartOfDay(ZoneId.systemDefault()).toInstant().toEpochMilli() + "]";
        QueryStringQueryBuilder queryStringQueryBuilder = QueryBuilders.queryStringQuery(query);
        searchSourceBuilder.query(queryStringQueryBuilder).fetchSource(new String[]{"name", "age", "address", "content", "bir"}, new String[0]);
        searchRequest.indices("ems").source(searchSourceBuilder);
        SearchResponse searchResponse = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
        SearchHit[] hits = searchResponse.getHits().getHits();
        System.out.println(hits.length);
        for (SearchHit hit : hits) {
            System.out.println(hit.getSourceAsString());
            System.out.println(hit.getId());
            System.out.println(hit.getScore());
        }
    }


    @Test
    public void testSimpleQueryStringSearch() throws IOException {
        SearchRequest searchRequest = new SearchRequest();
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        String query = "address:(-北京 +上海)";
        SimpleQueryStringBuilder simpleQueryStringBuilder = QueryBuilders.simpleQueryStringQuery(query);
        searchSourceBuilder.query(simpleQueryStringBuilder).fetchSource(new String[]{"name", "age", "address", "content", "bir"}, new String[0]);
        searchRequest.indices("ems").source(searchSourceBuilder);
        SearchResponse searchResponse = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
        SearchHit[] hits = searchResponse.getHits().getHits();
        System.out.println(hits.length);
        for (SearchHit hit : hits) {
            System.out.println(hit.getSourceAsString());
            System.out.println(hit.getId());
            System.out.println(hit.getScore());
        }
    }

    @Test
    public void testNotSearch() throws IOException {
        SearchRequest searchRequest = new SearchRequest();
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        String query = "age:(!23)";
        QueryStringQueryBuilder queryStringQueryBuilder = QueryBuilders.queryStringQuery(query);
        searchSourceBuilder.query(queryStringQueryBuilder).fetchSource(new String[]{"name", "age", "address", "content", "bir"}, new String[0]);
        searchRequest.indices("ems").source(searchSourceBuilder);
        SearchResponse searchResponse = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
        SearchHit[] hits = searchResponse.getHits().getHits();
        System.out.println(hits.length);
        for (SearchHit hit : hits) {
            System.out.println(hit.getSourceAsString());
            System.out.println(hit.getId());
            System.out.println(hit.getScore());
        }
    }

    @Test
    public void testSearchHig() throws IOException {
        SearchRequest searchRequest = new SearchRequest();
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        HighlightBuilder highlightBuilder = new HighlightBuilder();
        highlightBuilder.field("address").requireFieldMatch(false).preTags("<span style='color:red;'>").postTags("</span>");
        sourceBuilder.from(0).size(2).sort("age", SortOrder.DESC).highlighter(highlightBuilder).query(QueryBuilders.termQuery("address", "北京"));
        searchRequest.indices("ems").types("emp").source(sourceBuilder);
        SearchResponse search = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
        SearchHit[] hits = search.getHits().getHits();
        for (SearchHit hit : hits) {
            System.out.println(hit.getSourceAsString());
            Map<String, HighlightField> highlightFields = hit.getHighlightFields();
            highlightFields.forEach((k, v) -> System.out.println("key: " + k + " value: " + v.fragments()[0]));
        }
    }

    @Test
    public void testCreateIndex() throws IOException {
        CreateIndexRequest test = new CreateIndexRequest("test");
        String json = "{\"properties\":{\"name\":{\"type\":\"text\",\"analyzer\":\"ik_max_word\"},\"age\":{\"type\":\"integer\"},\"sex\":{\"type\":\"keyword\"},\"content\":{\"type\":\"text\",\"analyzer\":\"ik_max_word\"}}}";
        test.mapping(json, XContentType.JSON);
        System.out.println(json);
        restHighLevelClient.indices().create(test, RequestOptions.DEFAULT);
    }

    @Test
    public void testDeleteIndex() throws IOException {
        DeleteIndexRequest test = new DeleteIndexRequest("test");
        AcknowledgedResponse acknowledgedResponse = restHighLevelClient.indices().delete(test, RequestOptions.DEFAULT);
        System.out.println(acknowledgedResponse.isAcknowledged());
    }

    @Test
    public void testCreateDocument() throws IOException {
        Emp emp = new Emp();
        emp.setAge(27);
        emp.setName("李XX");
        emp.setContent("这是添加文档测试内容");
        emp.setSex("男");
        String jsonString = JSONObject.toJSONString(emp);
        IndexRequest indexRequest = new IndexRequest();
        indexRequest.id("1");
        indexRequest.type("_doc");
        indexRequest.index("test");
        indexRequest.source(jsonString, XContentType.JSON);
        IndexResponse indexResponse = restHighLevelClient.index(indexRequest, RequestOptions.DEFAULT);
        System.out.println(indexResponse.status());
    }

    @Test
    public void testUpdateDocument() throws IOException {
        Emp emp = new Emp();
        emp.setAge(27);
        emp.setName("李XX修改测试");
        emp.setContent("这是添加文档测试内容");
        emp.setSex("男");
        String jsonString = JSONObject.toJSONString(emp);
        UpdateRequest updateRequest = new UpdateRequest("test", "_doc", "1");
        updateRequest.doc(jsonString, XContentType.JSON);
        UpdateResponse updateResponse = restHighLevelClient.update(updateRequest, RequestOptions.DEFAULT);
        System.out.println(updateResponse.status());
    }


    @Test
    public void testGetRequest() throws IOException {
        GetRequest getRequest = new GetRequest("test", "_doc", "1");
        getRequest.fetchSourceContext(new FetchSourceContext(true, new String[]{"name", "sex"}, new String[0]));
        GetResponse getResponse = restHighLevelClient.get(getRequest, RequestOptions.DEFAULT);
        System.out.println(getResponse.getSourceAsString());
        System.out.println(getResponse.getSource());
        System.out.println(getResponse.getSourceAsMap());
        System.out.println(getResponse.getId());
        System.out.println(getResponse.isExists());
    }

    @Test
    public void testBulkRequest() throws IOException {
        BulkRequest bulkRequest = new BulkRequest();
        for (int i = 1; i <= 3; i++) {
            Emp emp = new Emp();
            emp.setAge(i);
            emp.setName("李XX" + i);
            emp.setContent("这是添加文档测试内容" + i);
            emp.setSex("男" + i);
            String jsonString = JSONObject.toJSONString(emp);
            IndexRequest indexRequest = new IndexRequest("test", "_doc", String.valueOf(i));
            indexRequest.source(jsonString, XContentType.JSON);
            bulkRequest.add(indexRequest);
        }
        BulkResponse bulkResponse = restHighLevelClient.bulk(bulkRequest, RequestOptions.DEFAULT);
        RestStatus status = bulkResponse.status();
        System.out.println(status.getStatus());
    }
}
