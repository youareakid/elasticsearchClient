package cn.com.taiji.elasticsearchclient.web;

import cn.com.taiji.elasticsearchclient.dto.ResultDTO;
import cn.com.taiji.elasticsearchclient.service.ImportDataFromMysqlService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

import java.io.IOException;

@Controller
@RequestMapping("/import-data")
public class ImportDataFromMysqlController {

    @Autowired
    ImportDataFromMysqlService importDataFromMysqlService;

    @GetMapping(value = "/from-mysql")
    @ResponseBody
    public ResultDTO<String> importDataFromMysql(@RequestParam String dbName, String tbName) throws IOException {

        String result = importDataFromMysqlService.addOrUpdateDocument(dbName, tbName);

        return new ResultDTO(result);

    }

}
