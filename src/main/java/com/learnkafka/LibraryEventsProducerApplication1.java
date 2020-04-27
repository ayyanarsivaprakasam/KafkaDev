package com.learnkafka;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
public class LibraryEventsProducerApplication1 {

	public static void main(String[] args) {
		SpringApplication.run(LibraryEventsProducerApplication1.class, args);
	}

}
