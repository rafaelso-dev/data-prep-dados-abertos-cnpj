package com.forteanuncio.prep.dadospublicoscnpj.runnables;

import java.lang.reflect.InvocationTargetException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;

import com.forteanuncio.prep.dadospublicoscnpj.converter.CsvConverter;
import com.forteanuncio.prep.dadospublicoscnpj.mapper.MapperEmpresa;
import com.forteanuncio.prep.dadospublicoscnpj.model.Empresa;
import com.forteanuncio.prep.dadospublicoscnpj.utils.CsvUtils;

public class MapperEmpresaExecutor implements Runnable {

    private String line;

    public MapperEmpresaExecutor(String line) {
        this.line = line;
    }

    CsvConverter<Empresa> converter = new CsvConverter<Empresa>() {};
    CsvUtils<Empresa> utils = new CsvUtils<Empresa>() {};

    @Override
    public void run() {
        
        
        Empresa empresa = null;
        try {
            empresa = (Empresa) converter.convertToObject(line);
            String genericHeader = utils.generateHeaderByObject(empresa);
            String lineFormated = converter.convertToCsv(empresa);
            if(MapperEmpresa.mapObjetosByHeader.get(genericHeader) == null){
                List<String> lista = new ArrayList<String>();
                lista.add(lineFormated);
                MapperEmpresa.adiciona(genericHeader, lista);
            }else {
                MapperEmpresa.adiciona(genericHeader, lineFormated);
            }
            
        } catch (NoSuchMethodException | SecurityException | IllegalAccessException | IllegalArgumentException | InvocationTargetException | InstantiationException | ParseException e) {
            e.printStackTrace();
        }
    }

}