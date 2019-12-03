package com.forteanuncio.loader.dadospublicoscnpj.service.generator;

import java.lang.reflect.InvocationTargetException;
import java.util.List;

import com.forteanuncio.loader.dadospublicoscnpj.model.Empresa;

public interface GeneratorFileDelimited{
    public String addFile(List<String> lista, String genericHeader);
    public String generateHeaderByObject(Empresa emp) throws IllegalArgumentException, IllegalAccessException,
        NoSuchMethodException, SecurityException, InvocationTargetException;
    public String convertToCsvByObject(Empresa emp) throws NoSuchMethodException, SecurityException,
        IllegalAccessException, IllegalArgumentException, InvocationTargetException;
}