package com.lqb.elaticsearch.entity;

import com.fasterxml.jackson.annotation.JsonFormat;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.data.annotation.Id;
import org.springframework.data.elasticsearch.annotations.DateFormat;
import org.springframework.data.elasticsearch.annotations.Document;
import org.springframework.data.elasticsearch.annotations.Field;
import org.springframework.data.elasticsearch.annotations.FieldType;

import java.util.Date;

/**
 * @author lqb
 * @date 2021/4/28 19:50
 */
@Document(indexName = "liqiubo")
@Data
@AllArgsConstructor
@NoArgsConstructor
public class Book {

    @Id
    private String id;

    @Field(type = FieldType.Text, analyzer = "ik_max_word")
    private String name;

    @Field(type = FieldType.Date,format = DateFormat.custom,pattern = "yyyy-MM-dd HH:mm:ss")
    @JsonFormat(pattern = "yyyy-MM-dd HH:mm:ss",timezone = "GMT+8")
    private Date createDate;

    @Field(type = FieldType.Keyword)
    private String author;

    @Field(type = FieldType.Text, analyzer = "ik_max_word")
    private String content;
}
