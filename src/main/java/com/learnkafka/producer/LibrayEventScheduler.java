package com.learnkafka.producer;

import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.learnkafka.domain.Book;
import com.learnkafka.domain.LibraryEvent;
import com.learnkafka.entity.BookDetails;
import com.learnkafka.jpa.StudentJdbcRepository;

@Configuration
@EnableScheduling
@Component
public class LibrayEventScheduler
{
	private static final Logger logger= LogManager.getLogger(LibrayEventScheduler.class);

	
	@Autowired
	StudentJdbcRepository studentJdbcRepository;
	
	 @Autowired
	 LibraryEventProducer libraryEventProducer;
	 
	  @Autowired
	  KafkaTemplate<Integer,String> kafkaTemplate;
	    
	    public int onSuccess=0;
	    public int onFailure=0;

	@Scheduled(cron="0 45 11 * * MON-FRI")
	//@Scheduled(cron = "1 * * ? * * ")
	public void scheduleTaskUsingCronExpression()
	{
	  
	    long now = System.currentTimeMillis() / 1000;
	    System.out.println(
	      "schedule tasks using cron jobs - " + now);
	    
	    
	    List<BookDetails> bookDetails=studentJdbcRepository.findAll();
	    
	    for(BookDetails book :bookDetails)
	    {
	    	/*logger.info("--------------------------------------------------------");
	    	logger.info("LibraryEventId :" +book.getLibraryEventId());
	    	logger.info("LibraryEventType :" +book.getLibraryEventType());
	    	logger.info("BookId :" +book.getBookAuthor());
	    	logger.info("BookName :" +book.getBookId());
	    	logger.info("BookAuthor" +book.getBookName());
	    	logger.info("--------------------------------------------------------");*/
	    	
	    	Book book1=new Book();
	    	book1.setBookId(book.getBookId());
	    	book1.setBookName(book.getBookName());
	    	book1.setBookAuthor(book.getBookAuthor());
	    	LibraryEvent libraryEvent=new LibraryEvent();
	    	libraryEvent.setLibraryEventId(book.getLibraryEventId());
	    	libraryEvent.setLibraryEventType(null);
	    	libraryEvent.setBook(book1);
	    	
	    	
	    	
	    	SendResult<Integer, String> sendResult;
			try {
				onSuccess++;
				sendResult = libraryEventProducer.sendLibraryEventSynchronous(libraryEvent);
				logger.info("Send Result is {}" +sendResult.toString());
		       	logger.info("sendResult.getProducerRecord():"+sendResult.getProducerRecord());

			} catch (JsonProcessingException | ExecutionException | InterruptedException | TimeoutException e) 
			{
				// TODO Auto-generated catch block
				e.printStackTrace();
				logger.info("onFailure---------->"+e);
				onFailure++;
				
			}
	    	
	    	
	    }
	    
      	logger.info("onSuccess--------->:"+onSuccess);
      	onSuccess=0;
      	logger.info("onFailure--------->:"+onFailure);

	}
}



