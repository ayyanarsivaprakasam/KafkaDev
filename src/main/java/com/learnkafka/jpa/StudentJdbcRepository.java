package com.learnkafka.jpa;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.BeanPropertyRowMapper;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Repository;

import com.learnkafka.entity.BookDetails;

@Repository
public class StudentJdbcRepository 
{

    @Autowired
    JdbcTemplate jdbcTemplate;

    class StudentRowMapper implements RowMapper < BookDetails > 
    {

        @Override

        public BookDetails mapRow(ResultSet rs, int rowNum) throws SQLException {

        	BookDetails bookDetails = new BookDetails();
        	
        	bookDetails.setLibraryEventId(rs.getInt("LIBRARY_EVENT_ID"));
        	bookDetails.setLibraryEventType(rs.getString("LIBRARY_EVENT_TYPE"));
        	bookDetails.setBookId(rs.getInt("BOOK_ID"));
        	bookDetails.setBookName(rs.getString("BOOK_NAME"));
        	bookDetails.setBookAuthor(rs.getString("BOOK_AUTHOR"));

        	
        	
           /* 
            * LIBRARY_EVENT_ID
			LIBRARY_EVENT_TYPE
			BOOK_AUTHOR
			BOOK_ID
			BOOK_NAME
            * 
            * student.setId(rs.getLong("id"));
            student.setName(rs.getString("name"));
            student.setPassportNumber(rs.getString("passport_number"));
            @Id  
    		@Column  
    	    private Integer libraryEventId;
    		@Column 
    	    private String LibraryEventType;    
    		@Column 
    	    private Integer bookId;    
    		@Column 
    	    private String bookName;  
    		@Column 
    	    private String bookAuthor;*/
        	
        	return bookDetails;
        }



    }

   
    public List < BookDetails > findAll() {

        return jdbcTemplate.query("select * from Book_Details", new StudentRowMapper());

    }






}