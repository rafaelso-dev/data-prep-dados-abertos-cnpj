package com.forteanuncio.prep.dadospublicoscnpj.converters;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Date;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.forteanuncio.prep.dadospublicoscnpj.utils.Utils.GET_METHOD;
import static com.forteanuncio.prep.dadospublicoscnpj.utils.Utils.firstUpperNameField;

public class SSTableConverter<T> {

    private static final Logger logger = LoggerFactory.getLogger(CsvConverter.class);
    
    public List<Object> convertLineToListObjectTypes(Object model, String genericHeader) {
        List<Object> columnsOfTable = new CopyOnWriteArrayList<Object>();
        try{
            String[] headerColumns = genericHeader.split(",");
            for(int i=0; i < headerColumns.length; i++){
                Method method = model.getClass().getDeclaredMethod(GET_METHOD+firstUpperNameField(headerColumns[i]));
                if(method.getReturnType().getName().contains("LocalDateTime")){
                    LocalDateTime col = (LocalDateTime) method.invoke(model);
                    Date newCol = Date.from(col.atZone(ZoneId.systemDefault()).toInstant());
                    columnsOfTable.add(newCol);
                }else{
                    columnsOfTable.add(method.invoke(model));
                }
            }
        }catch(NoSuchMethodException | SecurityException | IllegalAccessException | IllegalArgumentException | InvocationTargetException  e){
            logger.error("Error on mapping of List<Object> for SSTable. Details: {}", e.getMessage());
        }
        return columnsOfTable;
    }

}