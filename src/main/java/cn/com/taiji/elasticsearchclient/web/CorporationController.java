package cn.com.taiji.elasticsearchclient.web;

import cn.com.taiji.elasticsearchclient.dto.CorporationDTO;
import cn.com.taiji.elasticsearchclient.dto.ResultDTO;
import cn.com.taiji.elasticsearchclient.service.Impl.CorporationServiceImpl;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.*;

import java.io.IOException;
import java.util.ArrayList;

@Controller
@RequestMapping("/corporation")
public class CorporationController {

    @Autowired
    CorporationServiceImpl corporationService;

    @GetMapping(value = "/search-by-name")
    @ResponseBody
    public ResultDTO<CorporationDTO> searchByName(@RequestParam String name, Integer pageNum, Integer pageSize) throws IOException {

        CorporationDTO corporationDTO = corporationService.listCorporationByName(name, pageNum, pageSize);
        return new ResultDTO(corporationDTO);

    }

    @GetMapping(value = "/search-by-labelId")
    @ResponseBody
    public ResultDTO<CorporationDTO> searchByLabelId(@RequestParam ArrayList<String> labelIdList, Integer pageNum, Integer pageSize) throws IOException {

        CorporationDTO corporationDTO = corporationService.listCorporationByLabelId(labelIdList, pageNum, pageSize);
        return new ResultDTO(corporationDTO);

    }

    @GetMapping(value = "/search-by-dataMasking")
    @ResponseBody
    public ResultDTO<CorporationDTO> searchByDataMasking(@RequestParam String name, Integer level, Integer pageNum, Integer pageSize) throws IOException {

        ResultDTO<CorporationDTO> resultDTO = new ResultDTO<>();

        if (level != 1 && level != 2 && level != 3 && level != 4) {
            resultDTO.setCode(400);
            resultDTO.setMessage("脱敏级别不在一级到四级之间");
            return resultDTO;
        }

        CorporationDTO corporationDTO = corporationService.listCorporationByDataMasking(name, level, pageNum, pageSize);
        resultDTO.setData(corporationDTO);
        return resultDTO;

    }

}
