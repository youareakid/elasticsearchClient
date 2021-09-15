package cn.com.taiji.elasticsearchclient.service;

import cn.com.taiji.elasticsearchclient.domain.PopulationMatchOneLabel;
import cn.com.taiji.elasticsearchclient.dto.PopulationDTO;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

@Slf4j
@SpringBootTest
public class PopulationServiceTest {

    @Autowired
    PopulationService populationService;

    @Test
    void testQueryTotalPopulation() throws IOException {
        Long totalPopulation = populationService.queryTotalPopulation();
        log.info("返回数据: ===>{}", totalPopulation);
    }

    @Test
    void testQueryPopulationMatchAllLabel() throws IOException {
        ArrayList<String> labelIdList = new ArrayList<>();
        labelIdList.add("3bdc0616-441f-49a9-8d7f-a1231b57083a");    // 城市建设税缴纳正常
        labelIdList.add("64d11983-5f7f-4dbb-a2b8-2b5f4a10967a");    // 社保缴纳正常
        labelIdList.add("e72f9892-c1db-42a3-bcc1-bd8c9f3d86ae");    // 增值税缴纳正常

        Long matchAllLabel = populationService.queryPopulationMatchAllLabel(labelIdList);
        log.info("返回数据: ===>{}", matchAllLabel);
    }

    @Test
    void testQueryPopulationMatchOneLabel() throws IOException {
        ArrayList<String> labelIdList = new ArrayList<>();
        labelIdList.add("3bdc0616-441f-49a9-8d7f-a1231b57083a");    // 城市建设税缴纳正常
        labelIdList.add("64d11983-5f7f-4dbb-a2b8-2b5f4a10967a");    // 社保缴纳正常
        labelIdList.add("e72f9892-c1db-42a3-bcc1-bd8c9f3d86ae");    // 增值税缴纳正常

        List<PopulationMatchOneLabel> populationMatchOneLabelList = populationService.queryPopulationMatchOneLabel(labelIdList);
        log.info("返回数据: ===>{}", populationMatchOneLabelList);
    }

    @Test
    void testQueryPopulationMatchAllLabelByGender() throws IOException {
        String genderLabelId = "d4d7bf50-7fac-49ce-95ca-fb7cd743e17c";  // 企业法人

        ArrayList<String> labelIdList = new ArrayList<>();
        labelIdList.add("3bdc0616-441f-49a9-8d7f-a1231b57083a");    // 城市建设税缴纳正常
        labelIdList.add("64d11983-5f7f-4dbb-a2b8-2b5f4a10967a");    // 社保缴纳正常
        labelIdList.add("e72f9892-c1db-42a3-bcc1-bd8c9f3d86ae");    // 增值税缴纳正常

        Long matchAllLabelByGender = populationService.queryPopulationMatchAllLabelByGender(labelIdList, genderLabelId);
        log.info("返回数据: ===>{}", matchAllLabelByGender);
    }

    @Test
    void testQueryPopulationMatchAllLabelByBirthdate() throws IOException {
        String birthdateLabelId = "4d4835e2-bf56-4469-a29f-c1f596897569";  // 小微企业

        ArrayList<String> labelIdList = new ArrayList<>();
        labelIdList.add("3bdc0616-441f-49a9-8d7f-a1231b57083a");    // 城市建设税缴纳正常
        labelIdList.add("64d11983-5f7f-4dbb-a2b8-2b5f4a10967a");    // 社保缴纳正常
        labelIdList.add("e72f9892-c1db-42a3-bcc1-bd8c9f3d86ae");    // 增值税缴纳正常

        Long matchAllLabelByBirthdate = populationService.queryPopulationMatchAllLabelByBirthdate(labelIdList, birthdateLabelId);
        log.info("返回数据: ===>{}", matchAllLabelByBirthdate);
    }

    @Test
    void testPopulationStatsByLabelId() throws IOException {
        ArrayList<String> labelIdList = new ArrayList<>();
        labelIdList.add("3bdc0616-441f-49a9-8d7f-a1231b57083a");    // 城市建设税缴纳正常
        labelIdList.add("64d11983-5f7f-4dbb-a2b8-2b5f4a10967a");    // 社保缴纳正常
        labelIdList.add("e72f9892-c1db-42a3-bcc1-bd8c9f3d86ae");    // 增值税缴纳正常

        ArrayList<String> birthdateLabelIdList = new ArrayList<>();
        birthdateLabelIdList.add("4d4835e2-bf56-4469-a29f-c1f596897569");   // 小微企业
        birthdateLabelIdList.add("32837d30-42e2-461b-855c-a2c3acc640b7");   // 私营独资企业

        ArrayList<String> genderLabelIdList = new ArrayList<>();
        genderLabelIdList.add("d4d7bf50-7fac-49ce-95ca-fb7cd743e17c");  // 企业法人
        genderLabelIdList.add("e639df01-2dce-4262-bfc8-5993d1cdcbd2");  // 企业非法人

        PopulationDTO populationDTO = populationService.populationStatsByLabelId(labelIdList, genderLabelIdList, birthdateLabelIdList);
        log.info("返回数据: ===>{}", populationDTO);
    }
}
