package com.forteanuncio.loader.dadospublicoscnpj.service.loader;

import java.io.IOException;

public interface Loader{
    public void writerData() throws IOException, InterruptedException;
}