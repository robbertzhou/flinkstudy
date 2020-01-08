package com.zy.windowoperator;

import java.io.Serializable;

/**
 * 用户对电影的评分
 */
public class MovieRate implements Serializable {
//    userId, movieId, rating, timestamp
    private  int userId;
    private  int movieId;
    private  double rating;
    private  Long timestamp;
    public MovieRate(){

    }
    public  int getUserId() {
        return userId;
    }

    public void setUserId(int userId) {
        this.userId = userId;
    }

    public int getMovieId() {
        return movieId;
    }

    public void setMovieId(int movieId) {
        this.movieId = movieId;
    }

    public double getRating() {
        return rating;
    }

    public void setRating(double rating) {
        this.rating = rating;
    }

    public Long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Long timestamp) {
        this.timestamp = timestamp;
    }


}
