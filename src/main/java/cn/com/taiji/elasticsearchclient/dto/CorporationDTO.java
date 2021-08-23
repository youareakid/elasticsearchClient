package cn.com.taiji.elasticsearchclient.dto;


import cn.com.taiji.elasticsearchclient.domain.Corporation;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.util.List;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class CorporationDTO implements Serializable {

    private List<Corporation> corporationList;

    private Long totalNum;

    private Integer totalPage;

}
