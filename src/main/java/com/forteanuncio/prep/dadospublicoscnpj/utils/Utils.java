package com.forteanuncio.prep.dadospublicoscnpj.utils;

public class Utils<T>{

    public static String SET_METHOD = "set";
    public static String GET_METHOD = "get";

    public static String firstUpperNameField(String field){
        return field.substring(0,1).toUpperCase()+field.substring(1);
    }

}