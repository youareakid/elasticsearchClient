package cn.com.taiji.elasticsearchclient.service.Impl;

import cn.com.taiji.elasticsearchclient.domain.Corporation;
import cn.com.taiji.elasticsearchclient.dto.CorporationDTO;
import cn.com.taiji.elasticsearchclient.service.CorporationService;
import lombok.extern.slf4j.Slf4j;
import org.apache.lucene.search.TotalHits;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.BoolQueryBuilder;
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
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

@Service
@Slf4j
public class CorporationServiceImpl implements CorporationService {

    @Autowired
    @Qualifier("restHighLevelClient")
    private RestHighLevelClient client;

    public CorporationDTO listCorporationByName(String name, Integer pageNum, Integer pageSize) throws IOException {

        SearchRequest searchRequest = new SearchRequest("rkzhk.label");
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder().trackTotalHits(true);

        MatchPhraseQueryBuilder matchPhraseQueryBuilder = QueryBuilders.matchPhraseQuery("name", name);
        sourceBuilder.query(matchPhraseQueryBuilder);
        sourceBuilder.timeout(new TimeValue(60, TimeUnit.SECONDS));
        sourceBuilder.from(pageNum * pageSize);
        sourceBuilder.size(pageSize);
        sourceBuilder.sort("zczj", SortOrder.DESC);

        searchRequest.source(sourceBuilder);
        SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);

        // ????????????
        SearchHits hits = searchResponse.getHits();
        SearchHit[] searchHits = hits.getHits();
        // ????????????????????????
        TotalHits totalHits = hits.getTotalHits();
        Long totalNum = totalHits.value;
        // ?????????????????????
        Integer totalPage = (int) (Math.ceil(totalNum * 1.0 / pageSize));

        List<Corporation> corporationList = new ArrayList<>();
        for (SearchHit searchHit : searchHits) {
            BigDecimal zczj = new BigDecimal(String.valueOf(searchHit.getSourceAsMap().get("zczj")));

            Corporation corporation = new Corporation(
                    String.valueOf(searchHit.getId()),
                    String.valueOf(searchHit.getSourceAsMap().get("name")),
                    String.valueOf(searchHit.getSourceAsMap().get("tyshxydm")),
                    String.valueOf(searchHit.getSourceAsMap().get("fddbrxm")),
                    String.valueOf(searchHit.getSourceAsMap().get("zcdz")),
                    zczj.toString()
            );

            // null ???????????????
            if (corporation.getId() == "null") {
                corporation.setId("");
            }
            if (corporation.getName() == "null") {
                corporation.setName("");
            }
            if (corporation.getTyshxydm() == "null") {
                corporation.setTyshxydm("");
            }
            if (corporation.getFddbrxm() == "null") {
                corporation.setFddbrxm("");
            }
            if (corporation.getZcdz() == "null") {
                corporation.setZcdz("");
            }
            if (corporation.getZczj() == "null") {
                corporation.setZczj("");
            }

            corporationList.add(corporation);
        }

        return CorporationDTO.builder()
                .corporationList(corporationList).totalNum(totalNum).totalPage(totalPage)
                .build();

    }

    public CorporationDTO listCorporationByLabelId(ArrayList<String> labelIdList, Integer pageNum, Integer pageSize) throws IOException {

        SearchRequest searchRequest = new SearchRequest("rkzhk.label");
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder().trackTotalHits(true);

        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();

        for (String labelId : labelIdList) {
            String str = "label." + labelId;
            boolQueryBuilder.must(QueryBuilders.existsQuery(str));
        }

        sourceBuilder.query(boolQueryBuilder);
        sourceBuilder.timeout(new TimeValue(60, TimeUnit.SECONDS));
        sourceBuilder.from(pageNum * pageSize);
        sourceBuilder.size(pageSize);
        sourceBuilder.sort("zczj", SortOrder.DESC);

        searchRequest.source(sourceBuilder);
        SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);

        // ????????????
        SearchHits hits = searchResponse.getHits();
        SearchHit[] searchHits = hits.getHits();
        // ????????????????????????
        TotalHits totalHits = hits.getTotalHits();
        Long totalNum = totalHits.value;
        // ?????????????????????
        Integer totalPage = (int) (Math.ceil(totalNum * 1.0 / pageSize));

        List<Corporation> corporationList = new ArrayList<>();
        for (SearchHit searchHit : searchHits) {
            BigDecimal zczj = new BigDecimal(String.valueOf(searchHit.getSourceAsMap().get("zczj")));

            Corporation corporation = new Corporation(
                    String.valueOf(searchHit.getId()),
                    String.valueOf(searchHit.getSourceAsMap().get("name")),
                    String.valueOf(searchHit.getSourceAsMap().get("tyshxydm")),
                    String.valueOf(searchHit.getSourceAsMap().get("fddbrxm")),
                    String.valueOf(searchHit.getSourceAsMap().get("zcdz")),
                    zczj.toString()
            );

            // null ???????????????
            if (corporation.getId() == "null") {
                corporation.setId("");
            }
            if (corporation.getName() == "null") {
                corporation.setName("");
            }
            if (corporation.getTyshxydm() == "null") {
                corporation.setTyshxydm("");
            }
            if (corporation.getFddbrxm() == "null") {
                corporation.setFddbrxm("");
            }
            if (corporation.getZcdz() == "null") {
                corporation.setZcdz("");
            }
            if (corporation.getZczj() == "null") {
                corporation.setZczj("");
            }

            corporationList.add(corporation);
        }

        return CorporationDTO.builder()
                .corporationList(corporationList).totalNum(totalNum).totalPage(totalPage)
                .build();

    }

    public CorporationDTO listCorporationByDataMasking(String name, Integer level, Integer pageNum, Integer pageSize) throws IOException {

        if (level != 1 && level != 2 && level != 3 && level != 4) {
            log.info("???????????????????????????????????????");
            return null;
        }

        SearchRequest searchRequest = new SearchRequest("rkzhk.label");
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder().trackTotalHits(true);

        MatchPhraseQueryBuilder matchPhraseQueryBuilder = QueryBuilders.matchPhraseQuery("name", name);
        sourceBuilder.query(matchPhraseQueryBuilder);
        sourceBuilder.timeout(new TimeValue(60, TimeUnit.SECONDS));
        sourceBuilder.from(pageNum * pageSize);
        sourceBuilder.size(pageSize);
        sourceBuilder.sort("zczj", SortOrder.DESC);

        searchRequest.source(sourceBuilder);
        SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);

        // ????????????
        SearchHits hits = searchResponse.getHits();
        SearchHit[] searchHits = hits.getHits();
        // ????????????????????????
        TotalHits totalHits = hits.getTotalHits();
        Long totalNum = totalHits.value;
        // ?????????????????????
        Integer totalPage = (int) (Math.ceil(totalNum * 1.0 / pageSize));

        List<Corporation> corporationList = new ArrayList<>();
        for (SearchHit searchHit : searchHits) {
            BigDecimal zczj = new BigDecimal(String.valueOf(searchHit.getSourceAsMap().get("zczj")));

            Corporation corporation = new Corporation(
                    String.valueOf(searchHit.getId()),
                    String.valueOf(searchHit.getSourceAsMap().get("name")),
                    String.valueOf(searchHit.getSourceAsMap().get("tyshxydm")),
                    String.valueOf(searchHit.getSourceAsMap().get("fddbrxm")),
                    String.valueOf(searchHit.getSourceAsMap().get("zcdz")),
                    zczj.toString()
            );

            // null ???????????????
            if (corporation.getId() == "null") {
                corporation.setId("");
            }
            if (corporation.getName() == "null") {
                corporation.setName("");
            }
            if (corporation.getTyshxydm() == "null") {
                corporation.setTyshxydm("");
            }
            if (corporation.getFddbrxm() == "null") {
                corporation.setFddbrxm("");
            }
            if (corporation.getZcdz() == "null") {
                corporation.setZcdz("");
            }
            if (corporation.getZczj() == "null") {
                corporation.setZczj("");
            }

            // ????????????
            if (level == 1) {
                if (corporation.getZcdz().length() >= 6) {
                    corporation.setZcdz(corporation.getZcdz().substring(0, 6) + "****");
                }
                if (corporation.getFddbrxm().length() >= 1) {
                    corporation.setFddbrxm(corporation.getFddbrxm().substring(0, 1) + "**");
                }
            } else if (level == 2) {
                if (corporation.getFddbrxm().length() >= 1) {
                    corporation.setFddbrxm(corporation.getFddbrxm().substring(0, 1) + "**");
                }
            }

            corporationList.add(corporation);
        }

        return CorporationDTO.builder()
                .corporationList(corporationList).totalNum(totalNum).totalPage(totalPage)
                .build();

    }

}
