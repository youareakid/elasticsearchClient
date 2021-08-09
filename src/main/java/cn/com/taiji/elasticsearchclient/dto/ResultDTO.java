package cn.com.taiji.elasticsearchclient.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.http.HttpStatus;
import java.io.Serializable;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class ResultDTO<T> implements Serializable {

    private Integer code = HttpStatus.OK.value();
    private String message = "操作成功";
    private T data;

    public ResultDTO(T data) {
        this.data = data;
    }
}