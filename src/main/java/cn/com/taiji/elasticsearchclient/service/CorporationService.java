package cn.com.taiji.elasticsearchclient.service;

import cn.com.taiji.elasticsearchclient.domain.Corporation;
import lombok.extern.slf4j.Slf4j;
import org.apache.lucene.search.TotalHits;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.MatchPhraseQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import org.elasticsearch.search.sort.SortOrder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

@Service
@Slf4j
public class CorporationService {

    @Autowired
    @Qualifier("restHighLevelClient")
    private RestHighLevelClient client;

    public List<Corporation> listCorporation(String name) throws IOException {
        SearchRequest searchRequest = new SearchRequest("rkzhk.label");
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();

        MatchPhraseQueryBuilder matchPhraseQueryBuilder = QueryBuilders.matchPhraseQuery("name", name);
        sourceBuilder.query(matchPhraseQueryBuilder);
        sourceBuilder.timeout(new TimeValue(60, TimeUnit.SECONDS));
        // sourceBuilder.fetchSource(new String[]{"id", "name", "fddbrxm", "zczj", "kyrq"}, new String[]{});
        sourceBuilder.from(0);
        sourceBuilder.size(10);
        sourceBuilder.sort("zczj", SortOrder.DESC);

        searchRequest.source(sourceBuilder);
        SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);

        // 搜索结果
        SearchHits hits = searchResponse.getHits();
        SearchHit[] searchHits = hits.getHits();
        // 匹配到的总记录数
        TotalHits totalHits = hits.getTotalHits();
        List<Corporation> corporationList = new ArrayList<>();
        for (SearchHit searchHit : searchHits) {
            Corporation corporation = new Corporation(
                    String.valueOf(searchHit.getSourceAsMap().get("name")),
                    String.valueOf(searchHit.getSourceAsMap().get("zczj"))
            );
            corporationList.add(corporation);
        }

        return corporationList;
    }

}