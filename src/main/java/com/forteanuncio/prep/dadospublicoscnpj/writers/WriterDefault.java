package com.forteanuncio.prep.dadospublicoscnpj.writers;

public abstract class WriterDefault{

    public abstract void defaultWriter(String createTable, String insertCommand, String tabela, String[] fields, String parentFolder) throws Exception;
    public abstract String getTableName();
    
}