package com.learnkafka.entity;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

import javax.persistence.Column;
import javax.persistence.Id;

import org.springframework.jdbc.core.RowMapper;

class StudentRowMapper implements RowMapper <BookDetails> 
{

    @Override

    public BookDetails mapRow(ResultSet rs, int rowNum) throws SQLException
    {

    	BookDetails bookDetails = new BookDetails();
    	
    	bookDetails.setBookId(rs.getInt("libraryEventId"));

       /* student.setId(rs.getLong("id"));
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



