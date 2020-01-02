package com.forteanuncio.prep.dadospublicoscnpj.reader.impl;

import com.forteanuncio.prep.dadospublicoscnpj.reader.Reader;

import org.springframework.stereotype.Component;

@Component
public class ReaderSocio implements Reader {

    @Override
    public void reader() {
        System.out.println("do nothing");
    }

} 