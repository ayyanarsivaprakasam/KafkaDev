package com.learnkafka.jpa;

//import com.learnkafka.entity.LibraryEvent;
import org.springframework.data.repository.CrudRepository;

import com.learnkafka.entity.BookDetails;

public interface LibraryEventsRepository extends CrudRepository<BookDetails,Integer> {
}
