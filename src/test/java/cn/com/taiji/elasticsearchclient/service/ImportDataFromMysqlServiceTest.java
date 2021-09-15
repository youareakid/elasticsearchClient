package cn.com.taiji.elasticsearchclient.service;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.io.IOException;

@Slf4j
@SpringBootTest
public class ImportDataFromMysqlServiceTest {

    @Autowired
    ImportDataFromMysqlService importDataFromMysqlService;

    @Test
    void testAddOrUpdateDocument() throws IOException {
        String dbName = "rkzhk";
        String tbName = "jbxxb_tmp";
        String result = importDataFromMysqlService.addOrUpdateDocument(dbName, tbName);

        System.out.println("result: " + result);
    }

}