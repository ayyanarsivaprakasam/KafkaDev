package com.learnkafka.entity;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;
import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotNull;
@Entity  
@Table 
public class BookDetails 
{
	
	public BookDetails()
	{}

	
	public Integer getLibraryEventId() {
		return libraryEventId;
	}
	public void setLibraryEventId(Integer libraryEventId) {
		this.libraryEventId = libraryEventId;
	}
	public String getLibraryEventType() {
		return LibraryEventType;
	}
	public void setLibraryEventType(String libraryEventType) {
		LibraryEventType = libraryEventType;
	}
	public Integer getBookId() {
		return bookId;
	}
	public void setBookId(Integer bookId) {
		this.bookId = bookId;
	}
	public String getBookName() {
		return bookName;
	}
	public void setBookName(String bookName) {
		this.bookName = bookName;
	}
	public String getBookAuthor() {
		return bookAuthor;
	}
	public void setBookAuthor(String bookAuthor) {
		this.bookAuthor = bookAuthor;
	}


	
	public BookDetails(Integer libraryEventId, String libraryEventType, Integer bookId, String bookName,
			String bookAuthor) {
		super();
		this.libraryEventId = libraryEventId;
		LibraryEventType = libraryEventType;
		this.bookId = bookId;
		this.bookName = bookName;
		this.bookAuthor = bookAuthor;
	}

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
    private String bookAuthor;
    
    
}
