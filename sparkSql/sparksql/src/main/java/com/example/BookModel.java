package com.example;

public class BookModel {
    private String author;
    private String country;
    private String language;
    private int pages;
    private int year;
    private String title;


    
    public BookModel(String author, String country, String language, int pages, int year, String title) {
        this.author = author;
        this.country = country;
        this.language = language;
        this.pages = pages;
        this.year = year;
        this.title = title;
    }
    public BookModel() {
    }
    public String getAuthor() {
        return author;
    }
    public void setAuthor(String author) {
        this.author = author;
    }
    public String getCountry() {
        return country;
    }
    public void setCountry(String country) {
        this.country = country;
    }
    public String getLanguage() {
        return language;
    }
    public void setLanguage(String language) {
        this.language = language;
    }
    public int getPages() {
        return pages;
    }
    public void setPages(int pages) {
        this.pages = pages;
    }
    public int getYear() {
        return year;
    }
    public void setYear(int year) {
        this.year = year;
    }
    public String getTitle() {
        return title;
    }
    public void setTitle(String title) {
        this.title = title;
    }

    
}
