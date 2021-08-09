package cn.com.taiji.elasticsearchclient;

import cn.com.taiji.elasticsearchclient.domain.Corporation;
import com.alibaba.fastjson.JSON;
import lombok.extern.slf4j.Slf4j;
import org.apache.lucene.search.TotalHits;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.*;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.CreateIndexResponse;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.*;
import org.elasticsearch.search.Scroll;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;
import org.elasticsearch.search.sort.SortOrder;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.SpringBootTest;

import java.io.IOException;
import java.util.ArrayList;
import java.util.concurrent.TimeUnit;

/**
 * ElasticSearch 客户端API
 */
@Slf4j
@SpringBootTest
class ElasticsearchclientApplicationTests {

    @Autowired
    @Qualifier("restHighLevelClient")
    private RestHighLevelClient client;

    // 测试client连接是否正常
    @Test
    void contextLoads() {
        System.out.println(client);
    }

    // 判断索引是否存在
    @Test
    void testExistIndex() throws IOException {
        GetIndexRequest request = new GetIndexRequest("rkzhk.label");
        boolean exists = client.indices().exists(request, RequestOptions.DEFAULT);

        System.out.println(exists);
    }

    // 创建索引
    @Test
    void testCreateIndex() throws IOException {
        // 1.创建索引的请求
        CreateIndexRequest request = new CreateIndexRequest("test_index1");

        // 2.执行创建的请求 indicesclient,请求后获得响应
        CreateIndexResponse createIndexResponse = client.indices().create(request, RequestOptions.DEFAULT);
        System.out.println(createIndexResponse);
    }

    // 删除索引
    @Test
    void testDeleteIndex() throws IOException {
        DeleteIndexRequest request = new DeleteIndexRequest("rkzhk.label");
        AcknowledgedResponse delete = client.indices().delete(request, RequestOptions.DEFAULT);

        System.out.println(delete.isAcknowledged());
    }

    // 判断文档是否存在
    @Test
    void testExistDocument() throws IOException {
        GetRequest getRequest = new GetRequest("test_index1", "1");

        // 不获取返回的source的上下文
        getRequest.fetchSourceContext(new FetchSourceContext(false));
        getRequest.storedFields("_none_");

        boolean exists = client.exists(getRequest, RequestOptions.DEFAULT);

        System.out.println(exists);
    }

    // 添加文档
    @Test
    void testAddDocument() throws IOException {
        // 1.创建对象
        Corporation corporation = new Corporation("a","1000");

        // 2.创建请求
        IndexRequest request = new IndexRequest("test_index1");
        request.id("1");
        request.timeout(TimeValue.timeValueSeconds(1));

        // 3.将数据放入请求 json
        request.source(JSON.toJSONString(corporation), XContentType.JSON);

        // 4.客户端发送请求,获取响应结果
        IndexResponse indexResponse = client.index(request, RequestOptions.DEFAULT);

        System.out.println(indexResponse.toString());
        System.out.println(indexResponse.status());
    }

    // 查询文档信息
    @Test
    void testGetDocument() throws IOException {
        GetRequest getRequest = new GetRequest("test_index1", "1");
        GetResponse getResponse = client.get(getRequest, RequestOptions.DEFAULT);

        System.out.println(getResponse.getSourceAsString());
        System.out.println(getResponse);
    }

    // 更新文档信息
    @Test
    void testUpdateDocument() throws IOException {
        UpdateRequest updateRequest = new UpdateRequest("test_index1", "1");
        updateRequest.timeout("1s");

        Corporation corporation = new Corporation("b","1001");

        updateRequest.doc(JSON.toJSONString(corporation), XContentType.JSON);
        UpdateResponse updateResponse = client.update(updateRequest, RequestOptions.DEFAULT);

        System.out.println(updateResponse.status());
    }

    // 删除文档信息
    @Test
    void testDeleteDocument() throws IOException {
        DeleteRequest request = new DeleteRequest("test_index1", "1");
        request.timeout("1s");
        DeleteResponse deleteResponse = client.delete(request, RequestOptions.DEFAULT);

        System.out.println(deleteResponse.status());
    }

    // 批量插入文档
    @Test
    void testBulkAddDocument() throws IOException {
        BulkRequest bulkRequest = new BulkRequest();
        bulkRequest.timeout("10s");

        ArrayList<Corporation> userList = new ArrayList<>();
        userList.add(new Corporation("aaa", "15"));
        userList.add(new Corporation("bbb", "88"));
        userList.add(new Corporation("ccc", "39"));
        userList.add(new Corporation("ddd", "62"));

        for (int i = 0; i < userList.size(); i++) {
            bulkRequest.add(
                    new IndexRequest("test_index1")
                            .id("" + (i + 6))
                            .source(JSON.toJSONString(userList.get(i)), XContentType.JSON)
            );
        }

        BulkResponse bulkResponse = client.bulk(bulkRequest, RequestOptions.DEFAULT);

        System.out.println(bulkResponse.hasFailures());
    }

    // 搜索查询，排序，分页
    @Test
    void testSearch1() throws IOException {

        SearchRequest searchRequest = new SearchRequest("rkzhk.label");
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();

        MatchPhraseQueryBuilder matchPhraseQueryBuilder = QueryBuilders.matchPhraseQuery("name", "有限公司");
        sourceBuilder.query(matchPhraseQueryBuilder);
        sourceBuilder.sort("zczj", SortOrder.DESC);
        sourceBuilder.from(0);
        sourceBuilder.size(10);
        sourceBuilder.fetchSource(new String[]{"name", "fddbrxm","zczj","kyrq"}, new String[]{});
        sourceBuilder.timeout(new TimeValue(60, TimeUnit.SECONDS));

        searchRequest.source(sourceBuilder);
        SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);

        // 搜索结果
        SearchHits hits = searchResponse.getHits();
        SearchHit[] searchHits = hits.getHits();
        // 匹配到的总记录数
        TotalHits totalHits = hits.getTotalHits();

        log.info("查询总数: " + totalHits);
        System.out.println("查询结果:");
        for (SearchHit searchHit : searchHits) {
            System.out.println(searchHit.getSourceAsMap());
        }

    }

    // 滚动搜索查询
    @Test
    void testSearch2() throws IOException {

        final Scroll scroll = new Scroll(TimeValue.timeValueMinutes(1L));

        // 创建搜索请求对象
        SearchRequest searchRequest = new SearchRequest("test_index1");
        searchRequest.scroll(scroll);

        // 构建搜索源对象
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();

        // 搜索方式，使用QueryBuilders工具来实现

        // (1)全量查询
        // MatchAllQueryBuilder matchAllQueryBuilder = QueryBuilders.matchAllQuery();

        // (2)匹配查询，根据给定的字段的值的分词查询分词中包含给定的值的文档
        // MatchQueryBuilder matchQueryBuilder = QueryBuilders.matchQuery("name", "雅迎轩");

        // (3)精确查询，根据给定的字段的值的分词查询分词中包含给定的值的文档，匹配的字段分词所在位置必须和待查询的值的分词位置一致
        MatchPhraseQueryBuilder matchPhraseQueryBuilder = QueryBuilders.matchPhraseQuery("name", "aaa");

        sourceBuilder.query(matchPhraseQueryBuilder);
        // 设置源字段过虑,第一个参数结果集包括哪些字段，第二个参数表示结果集不包括哪些字段
        sourceBuilder.fetchSource(new String[]{"name", "age"}, new String[]{});
        sourceBuilder.timeout(new TimeValue(60, TimeUnit.SECONDS));

        searchRequest.source(sourceBuilder);
        // 执行搜索,向ES发起http请求
        SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
        String scrollId = searchResponse.getScrollId();

        // 搜索结果
        SearchHits hits = searchResponse.getHits();
        SearchHit[] searchHits = hits.getHits();
        // 匹配到的总记录数
        TotalHits totalHits = hits.getTotalHits();

        log.info("查询总数: " + totalHits);
        System.out.println("查询结果:");
        for (SearchHit searchHit : searchHits) {
            System.out.println(searchHit.getSourceAsMap());
        }

        while (searchHits != null && searchHits.length > 0) {
            SearchScrollRequest scrollRequest = new SearchScrollRequest(scrollId);
            scrollRequest.scroll(scroll);
            searchResponse = client.scroll(scrollRequest, RequestOptions.DEFAULT);
            scrollId = searchResponse.getScrollId();
            searchHits = searchResponse.getHits().getHits();
        }

        ClearScrollRequest clearScrollRequest = new ClearScrollRequest();
        clearScrollRequest.addScrollId(scrollId);
        ClearScrollResponse clearScrollResponse = client.clearScroll(clearScrollRequest, RequestOptions.DEFAULT);
        boolean succeeded = clearScrollResponse.isSucceeded();

        log.info("Clear the scroll context: " + succeeded);

    }

}