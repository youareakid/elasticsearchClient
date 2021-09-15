package cn.com.taiji.elasticsearchclient.service.Impl;

import cn.com.taiji.elasticsearchclient.service.ImportDataFromMysqlService;
import com.alibaba.fastjson.JSON;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.CreateIndexResponse;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.List;
import java.util.Map;

@Service
@Slf4j
public class ImportDataFromMysqlServiceImpl implements ImportDataFromMysqlService {

    @Autowired
    @Qualifier("restHighLevelClient")
    private RestHighLevelClient client;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    // 添加或者更新文档到 ES
    public String addOrUpdateDocument(String dbName, String tbName) throws IOException {
        String index = dbName + "." + tbName;

        if (!existIndex(index)) {
            createIndex(index);
        }

        String sql = "select * from " + index;
        // 从MySQL中读取数据
        List<Map<String, Object>> mapList = jdbcTemplate.queryForList(sql);

        for (int i = 0; i < mapList.size(); i++) {
            String id = mapList.get(i).get("id").toString();
            // String id = RandomStringUtils.randomAlphanumeric(24);

            if (!existDocument(index, id)) {
                int addResult = addDocument(index, id, mapList.get(i));

                if (addResult != 201) {
                    return "add failure";
                }
            } else {
                int updateResult = updateDocument(index, id, mapList.get(i));

                if (updateResult != 200) {
                    return "update failure";
                }
            }
        }

        return "success";

    }

    // 判断索引是否存在
    public boolean existIndex(String index) throws IOException {
        GetIndexRequest request = new GetIndexRequest(index);

        return client.indices().exists(request, RequestOptions.DEFAULT);

    }

    // 创建索引
    public boolean createIndex(String index) throws IOException {
        // 创建索引的请求
        CreateIndexRequest request = new CreateIndexRequest(index);
        // 执行创建的请求,请求后获得响应
        CreateIndexResponse createIndexResponse = client.indices().create(request, RequestOptions.DEFAULT);

        return createIndexResponse.isAcknowledged();

    }

    // 判断文档是否存在
    public boolean existDocument(String index, String id) throws IOException {
        GetRequest getRequest = new GetRequest(index, id);
        // 不获取返回的source的上下文
        getRequest.fetchSourceContext(new FetchSourceContext(false));
        getRequest.storedFields("_none_");

        return client.exists(getRequest, RequestOptions.DEFAULT);
    }

    // 添加文档
    public int addDocument(String index, String id, Map<String, Object> map) throws IOException {
        IndexRequest request = new IndexRequest(index);
        request.id(id);
        request.timeout(TimeValue.timeValueSeconds(1));
        request.source(JSON.toJSONString(map), XContentType.JSON);

        IndexResponse indexResponse = client.index(request, RequestOptions.DEFAULT);

        return indexResponse.status().getStatus();

    }

    // 更新文档
    public int updateDocument(String index, String id, Map<String, Object> map) throws IOException {
        UpdateRequest updateRequest = new UpdateRequest(index, id);
        updateRequest.timeout("1s");
        updateRequest.doc(JSON.toJSONString(map), XContentType.JSON);

        UpdateResponse updateResponse = client.update(updateRequest, RequestOptions.DEFAULT);

        return updateResponse.status().getStatus();

    }

}
