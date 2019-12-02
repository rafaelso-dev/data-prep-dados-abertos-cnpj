package com.forteanuncio.loader.dadospublicoscnpj.service.reader;

import org.springframework.stereotype.Component;

@Component
public class ReaderSocio implements Reader {

    @Override
    public void reader() {
        System.out.println("do nothing");
    }

} 