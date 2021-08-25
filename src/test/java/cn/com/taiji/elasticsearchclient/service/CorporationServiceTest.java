package cn.com.taiji.elasticsearchclient.service;

import cn.com.taiji.elasticsearchclient.domain.Corporation;
import cn.com.taiji.elasticsearchclient.dto.CorporationDTO;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

@Slf4j
@SpringBootTest
public class CorporationServiceTest {

    @Autowired
    CorporationService corporationService;

    @Test
    void testListCorporationByName() throws IOException {

        CorporationDTO corporationDTO = corporationService.listCorporationByName("name2", 0, 10);

        log.info("返回数据: ===>{}", corporationDTO);
    }

    @Test
    void testListCorporationByLabelId() throws IOException {

        ArrayList<String> labelIdList = new ArrayList<>();
        labelIdList.add("164b3c82-a534-4116-8884-464dd0cde4a3");
        labelIdList.add("3bdc0616-441f-49a9-8d7f-a1231b57083a");
        labelIdList.add("e72f9892-c1db-42a3-bcc1-bd8c9f3d86ae");

        CorporationDTO corporationDTO = corporationService.listCorporationByLabelId(labelIdList, 0, 10);

        log.info("返回数据: ===>{}", corporationDTO);
    }

    @Test
    void testListCorporationByDataMasking() throws IOException {

        CorporationDTO corporationDTO = corporationService.listCorporationByDataMasking("name2", 1, 0, 10);

        log.info("返回数据: ===>{}", corporationDTO);
    }

}
