package com.learnkafka.domain;


import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;

public class LibraryEvent {
	

    private Integer libraryEventId;
    private LibraryEventType libraryEventType;
    @NotNull
    @Valid
    private Book book;
	
	public LibraryEvent() {}

    public Integer getLibraryEventId() {
		return libraryEventId;
	}
	public void setLibraryEventId(Integer libraryEventId) {
		this.libraryEventId = libraryEventId;
	}
	public LibraryEventType getLibraryEventType() {
		return libraryEventType;
	}
	public void setLibraryEventType(LibraryEventType libraryEventType) {
		this.libraryEventType = libraryEventType;
	}
	public Book getBook() {
		return book;
	}
	public void setBook(Book book) {
		this.book = book;
	}
	
    public LibraryEvent(Integer libraryEventId, LibraryEventType libraryEventType, Book book) {
		super();
		this.libraryEventId = libraryEventId;
		this.libraryEventType = libraryEventType;
		this.book = book;
	}
  

}
