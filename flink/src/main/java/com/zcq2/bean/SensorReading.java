package com.zcq2.bean;

/**
 * @author cqzhang
 * @create 2020-12-11 18:59
 */

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class SensorReading {
    private String id;
    private Long ts;
    private Double temp;
}
