package com.example;

import java.io.Serializable;

public class GroupPlayersModel implements Serializable {
    String playerName;
    int matchCount;

  
    public GroupPlayersModel(String playerName, int size) {
        this.playerName = playerName;
        this.matchCount = size;
    }


    public String getPlayerName() {
        return playerName;
    }
    public void setPlayerName(String playerName) {
        this.playerName = playerName;
    }
    public int getMatchCount() {
        return matchCount;
    }
    public void setMatchCount(int matchCount) {
        this.matchCount = matchCount;
    }
    
}
