package cn.com.taiji.elasticsearchclient.web;

import cn.com.taiji.elasticsearchclient.domain.Corporation;
import cn.com.taiji.elasticsearchclient.dto.ResultDTO;
import cn.com.taiji.elasticsearchclient.service.CorporationService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

@Controller
@RequestMapping("/corporation")
public class CorporationController {

    @Autowired
    CorporationService corporationService;

    @GetMapping(value = "/search-by-name")
    @ResponseBody
    public ResultDTO<List<Corporation>> searchByName(@RequestParam String name, Integer pageNum, Integer pageSize) throws IOException {

        List<Corporation> corporationList = corporationService.listCorporationByName(name, pageNum, pageSize);
        return new ResultDTO(corporationList);

    }

    @GetMapping(value = "/search-by-labelId")
    @ResponseBody
    public ResultDTO<List<Corporation>> searchByLabelId(@RequestParam ArrayList<String> labelIdList, Integer pageNum, Integer pageSize) throws IOException {

        List<Corporation> corporationList = corporationService.listCorporationByLabelId(labelIdList, pageNum, pageSize);
        return new ResultDTO(corporationList);

    }

}
