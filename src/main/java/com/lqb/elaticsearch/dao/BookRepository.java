package com.lqb.elaticsearch.dao;

import com.lqb.elaticsearch.entity.Book;
import org.springframework.data.elasticsearch.repository.ElasticsearchRepository;
import org.springframework.stereotype.Repository;

/**
 * @author lqb
 * @date 2021/4/28 19:56
 */
@Repository
public interface BookRepository extends ElasticsearchRepository<Book,String> {
}
