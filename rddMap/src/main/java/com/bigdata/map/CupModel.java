package com.bigdata.map;

import java.io.Serializable;

public class CupModel implements Serializable {
  
    String yil;
    String evSahibi;
    String birinci;
    String ikinci;
    String ucuncu;
    String dorduncu;
    int toplamGol;
    int toplamUlke;
    int toplamMac;
    String toplamKatilimci;

    public CupModel(String yil, String evSahibi, String birinci, String ikinci, String ucuncu, String dorduncu,
            int toplamGol, int toplamUlke, int toplamMac, String toplamKatilimci) {
        this.yil = yil;
        this.evSahibi = evSahibi;
        this.birinci = birinci;
        this.ikinci = ikinci;
        this.ucuncu = ucuncu;
        this.dorduncu = dorduncu;
        this.toplamGol = toplamGol;
        this.toplamUlke = toplamUlke;
        this.toplamMac = toplamMac;
        this.toplamKatilimci = toplamKatilimci;
    }

    public String getYil() {
        return yil;
    }

    public void setYil(String yil) {
        this.yil = yil;
    }

    public String getEvSahibi() {
        return evSahibi;
    }

    public void setEvSahibi(String evSahibi) {
        this.evSahibi = evSahibi;
    }

    public String getBirinci() {
        return birinci;
    }

    public void setBirinci(String birinci) {
        this.birinci = birinci;
    }

    public String getIkinci() {
        return ikinci;
    }

    public void setIkinci(String ikinci) {
        this.ikinci = ikinci;
    }

    public String getUcuncu() {
        return ucuncu;
    }

    public void setUcuncu(String ucuncu) {
        this.ucuncu = ucuncu;
    }

    public String getDorduncu() {
        return dorduncu;
    }

    public void setDorduncu(String dorduncu) {
        this.dorduncu = dorduncu;
    }

    public int getToplamGol() {
        return toplamGol;
    }

    public void setToplamGol(int toplamGol) {
        this.toplamGol = toplamGol;
    }

    public int getToplamUlke() {
        return toplamUlke;
    }

    public void setToplamUlke(int toplamUlke) {
        this.toplamUlke = toplamUlke;
    }

    public int getToplamMac() {
        return toplamMac;
    }

    public void setToplamMac(int toplamMac) {
        this.toplamMac = toplamMac;
    }

    public String getToplamKatilimci() {
        return toplamKatilimci;
    }

    public void setToplamKatilimci(String toplamKatilimci) {
        this.toplamKatilimci = toplamKatilimci;
    }




}
