package com.example;

import java.io.Serializable;

public class PlayersModel implements Serializable {

    String roundID;
    String matchID;
    String teamInitials;
    String coachName;
    String lineUp;
    String shirtNumber;
    String playerName;
    String position;
    String event;

    public PlayersModel(String roundID, String matchID, String teamInitials, String coachName, String lineUp,
            String shirtNumber, String playerName, String position, String event) {
        this.roundID = roundID;
        this.matchID = matchID;
        this.teamInitials = teamInitials;
        this.coachName = coachName;
        this.lineUp = lineUp;
        this.shirtNumber = shirtNumber;
        this.playerName = playerName;
        this.position = position;
        this.event = event;
    }

    public String getRoundID() {
        return roundID;
    }

    public void setRoundID(String roundID) {
        this.roundID = roundID;
    }

    public String getMatchID() {
        return matchID;
    }

    public void setMatchID(String matchID) {
        this.matchID = matchID;
    }

    public String getTeamInitials() {
        return teamInitials;
    }

    public void setTeamInitials(String teamInitials) {
        this.teamInitials = teamInitials;
    }

    public String getCoachName() {
        return coachName;
    }

    public void setCoachName(String coachName) {
        this.coachName = coachName;
    }

    public String getLineUp() {
        return lineUp;
    }

    public void setLineUp(String lineUp) {
        this.lineUp = lineUp;
    }

    public String getShirtNumber() {
        return shirtNumber;
    }

    public void setShirtNumber(String shirtNumber) {
        this.shirtNumber = shirtNumber;
    }

    public String getPlayerName() {
        return playerName;
    }

    public void setPlayerName(String playerName) {
        this.playerName = playerName;
    }

    public String getPosition() {
        return position;
    }

    public void setPosition(String position) {
        this.position = position;
    }

    public String getEvent() {
        return event;
    }

    public void setEvent(String event) {
        this.event = event;
    }

}
