package com.example.model;

public class Movie {
    private String title;
    private String mainGenre;



    
    public Movie(String title, String mainGenre) {
        this.title = title;
        this.mainGenre = mainGenre;
    }
    public String getTitle() {
        return title;
    }
    public void setTitle(String title) {
        this.title = title;
    }
    public String getMainGenre() {
        return mainGenre;
    }
    public void setMainGenre(String mainGenre) {
        this.mainGenre = mainGenre;
    }
    


    
}
