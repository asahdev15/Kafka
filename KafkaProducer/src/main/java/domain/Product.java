package domain;

import lombok.*;

@Data
@EqualsAndHashCode
@NoArgsConstructor
@AllArgsConstructor
public class Product {

    private String category; // TV, MOBILE, AC
    private Integer id;

}