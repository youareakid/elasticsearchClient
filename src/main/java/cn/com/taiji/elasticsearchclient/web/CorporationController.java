package cn.com.taiji.elasticsearchclient.web;

import cn.com.taiji.elasticsearchclient.domain.Corporation;
import cn.com.taiji.elasticsearchclient.dto.ResultDTO;
import cn.com.taiji.elasticsearchclient.service.CorporationService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

import java.io.IOException;
import java.util.List;

@Controller
@RequestMapping("/corporation")
public class CorporationController {

    @Autowired
    CorporationService corporationService;

    @GetMapping(value = "/search-by-name")
    @ResponseBody
    public ResultDTO<List<Corporation>> searchByName(@RequestParam String name) throws IOException {

        List<Corporation> corporationList = corporationService.listCorporation(name);
        return new ResultDTO(corporationList);

    }

}
