package cn.com.taiji.elasticsearchclient.service.Impl;

import cn.com.taiji.elasticsearchclient.domain.PopulationMatchAllLabelByBirthdate;
import cn.com.taiji.elasticsearchclient.domain.PopulationMatchAllLabelByGender;
import cn.com.taiji.elasticsearchclient.domain.PopulationMatchOneLabel;
import cn.com.taiji.elasticsearchclient.dto.PopulationDTO;
import cn.com.taiji.elasticsearchclient.service.PopulationService;
import lombok.extern.slf4j.Slf4j;
import org.apache.lucene.search.TotalHits;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

@Service
@Slf4j
public class PopulationServiceImpl implements PopulationService {

    @Autowired
    @Qualifier("restHighLevelClient")
    private RestHighLevelClient client;

    public PopulationDTO populationStatsByLabelId(ArrayList<String> labelIdList, ArrayList<String> genderLabelIdList, ArrayList<String> birthdateLabelIdList) throws IOException {
        // 总人口数量
        Long totalPopulation = queryTotalPopulation();

        // 匹配符合所有标签id的人口的数量
        Long matchAllLabel = queryPopulationMatchAllLabel(labelIdList);

        // 挨个匹配符合集合中单个标签id的人口的数量
        List<PopulationMatchOneLabel> populationMatchOneLabelList = queryPopulationMatchOneLabel(labelIdList);

        // 按性别匹配符合所有标签id的人口的数量
        List<PopulationMatchAllLabelByGender> populationMatchAllLabelByGenderList = new ArrayList<>();
        for (String genderLabelId : genderLabelIdList) {
            Long matchAllLabelByGender = queryPopulationMatchAllLabelByGender(labelIdList, genderLabelId);
            PopulationMatchAllLabelByGender populationMatchAllLabelByGender = new PopulationMatchAllLabelByGender(genderLabelId, matchAllLabelByGender);
            populationMatchAllLabelByGenderList.add(populationMatchAllLabelByGender);
        }

        // 按出生日期匹配符合所有标签id的人口的数量
        List<PopulationMatchAllLabelByBirthdate> populationMatchAllLabelByBirthdateList = new ArrayList<>();
        for (String birthdateLabelId : birthdateLabelIdList) {
            Long matchAllLabelByBirthdate = queryPopulationMatchAllLabelByBirthdate(labelIdList, birthdateLabelId);
            PopulationMatchAllLabelByBirthdate populationMatchAllLabelByBirthdate = new PopulationMatchAllLabelByBirthdate(birthdateLabelId, matchAllLabelByBirthdate);
            populationMatchAllLabelByBirthdateList.add(populationMatchAllLabelByBirthdate);
        }

        return PopulationDTO.builder()
                .totalPopulation(totalPopulation)
                .populationMatchOneLabelList(populationMatchOneLabelList)
                .matchAllLabel(matchAllLabel)
                .populationMatchAllLabelByGenderList(populationMatchAllLabelByGenderList)
                .populationMatchAllLabelByBirthdateList(populationMatchAllLabelByBirthdateList)
                .build();

    }

    // 查询总人口数量
    public Long queryTotalPopulation() throws IOException {
        SearchRequest searchRequest = new SearchRequest("rkzhk.peplabinfo");
        // SearchRequest searchRequest = new SearchRequest("rkzhk.label");
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder().trackTotalHits(true);

        MatchAllQueryBuilder matchAllQueryBuilder = QueryBuilders.matchAllQuery();

        sourceBuilder.query(matchAllQueryBuilder);
        sourceBuilder.timeout(new TimeValue(60, TimeUnit.SECONDS));

        searchRequest.source(sourceBuilder);
        SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
        TotalHits totalHits = searchResponse.getHits().getTotalHits();

        return totalHits.value;
    }

    // 查询匹配符合所有标签id的人口的数量
    public Long queryPopulationMatchAllLabel(ArrayList<String> labelIdList) throws IOException {
        SearchRequest searchRequest = new SearchRequest("rkzhk.peplabinfo");
        // SearchRequest searchRequest = new SearchRequest("rkzhk.label");
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder().trackTotalHits(true);

        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
        for (String labelId : labelIdList) {
            String str = "label." + labelId;
            boolQueryBuilder.must(QueryBuilders.existsQuery(str));
        }

        sourceBuilder.query(boolQueryBuilder);
        sourceBuilder.timeout(new TimeValue(60, TimeUnit.SECONDS));

        searchRequest.source(sourceBuilder);
        SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);

        // 搜索结果
        SearchHits hits = searchResponse.getHits();
        // SearchHit[] searchHits = hits.getHits();
        // 匹配到的总记录数
        TotalHits totalHits = hits.getTotalHits();

        return totalHits.value;

    }

    // 查询匹配符合集合中单个标签id的人口的数量
    public List<PopulationMatchOneLabel> queryPopulationMatchOneLabel(ArrayList<String> labelIdList) throws IOException {
        SearchRequest searchRequest = new SearchRequest("rkzhk.peplabinfo");
        // SearchRequest searchRequest = new SearchRequest("rkzhk.label");
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder().trackTotalHits(true);

        List<PopulationMatchOneLabel> populationMatchOneLabelList = new ArrayList<>();
        for (String labelId : labelIdList) {
            String str = "label." + labelId;
            BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
            boolQueryBuilder.must(QueryBuilders.existsQuery(str));

            sourceBuilder.query(boolQueryBuilder);
            sourceBuilder.timeout(new TimeValue(60, TimeUnit.SECONDS));

            searchRequest.source(sourceBuilder);
            SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);

            // 搜索结果
            SearchHits hits = searchResponse.getHits();
            // SearchHit[] searchHits = hits.getHits();
            // 匹配到的总记录数
            TotalHits totalHits = hits.getTotalHits();
            Long matchOneLabel = totalHits.value;

            PopulationMatchOneLabel populationMatchOneLabel = new PopulationMatchOneLabel(labelId, matchOneLabel);
            populationMatchOneLabelList.add(populationMatchOneLabel);
        }

        return populationMatchOneLabelList;
    }

    // 按性别查询匹配符合所有标签id的人口的数量
    public Long queryPopulationMatchAllLabelByGender(ArrayList<String> labelIdList, String genderLabelId) throws IOException {
        SearchRequest searchRequest = new SearchRequest("rkzhk.peplabinfo");
        // SearchRequest searchRequest = new SearchRequest("rkzhk.label");
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder().trackTotalHits(true);

        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();

        for (String labelId : labelIdList) {
            String str = "label." + labelId;
            boolQueryBuilder.must(QueryBuilders.existsQuery(str));
        }
        boolQueryBuilder.must(QueryBuilders.existsQuery("label." + genderLabelId));

        sourceBuilder.query(boolQueryBuilder);
        sourceBuilder.timeout(new TimeValue(60, TimeUnit.SECONDS));

        searchRequest.source(sourceBuilder);
        SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);

        // 搜索结果
        SearchHits hits = searchResponse.getHits();
        // SearchHit[] searchHits = hits.getHits();
        // 匹配到的总记录数
        TotalHits totalHits = hits.getTotalHits();

        return totalHits.value;

    }

    // 按出生日期查询匹配符合所有标签id的人口的数量
    public Long queryPopulationMatchAllLabelByBirthdate(ArrayList<String> labelIdList, String birthdateLabelId) throws IOException {
        SearchRequest searchRequest = new SearchRequest("rkzhk.peplabinfo");
        // SearchRequest searchRequest = new SearchRequest("rkzhk.label");
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder().trackTotalHits(true);

        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();

        for (String labelId : labelIdList) {
            String str = "label." + labelId;
            boolQueryBuilder.must(QueryBuilders.existsQuery(str));
        }
        boolQueryBuilder.must(QueryBuilders.existsQuery("label." + birthdateLabelId));

        sourceBuilder.query(boolQueryBuilder);
        sourceBuilder.timeout(new TimeValue(60, TimeUnit.SECONDS));

        searchRequest.source(sourceBuilder);
        SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);

        // 搜索结果
        SearchHits hits = searchResponse.getHits();
        // SearchHit[] searchHits = hits.getHits();
        // 匹配到的总记录数
        TotalHits totalHits = hits.getTotalHits();

        return totalHits.value;

    }

}
