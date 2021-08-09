package cn.com.taiji.elasticsearchclient.service;

import cn.com.taiji.elasticsearchclient.domain.Corporation;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.io.IOException;
import java.util.List;

@Slf4j
@SpringBootTest
public class CorporationServiceTest {

    @Autowired
    CorporationService corporationService;

    @Test
    void testListCorporation() throws IOException {

        List<Corporation> corporationList = corporationService.listCorporation("有限公司");

        log.info("返回数据: ===>{}", corporationList);
    }

}
