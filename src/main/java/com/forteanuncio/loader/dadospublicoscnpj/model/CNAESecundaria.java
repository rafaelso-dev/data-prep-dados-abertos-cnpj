package com.forteanuncio.loader.dadospublicoscnpj.model;

import java.io.Serializable;
import java.util.UUID;

import org.springframework.data.cassandra.core.cql.Ordering;
import org.springframework.data.cassandra.core.cql.PrimaryKeyType;
import org.springframework.data.cassandra.core.mapping.PrimaryKeyColumn;
import org.springframework.data.cassandra.core.mapping.Table;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Table
public class CNAESecundaria implements Serializable{

    private static final long serialVersionUID = 2361902976097655601L;

    @PrimaryKeyColumn(name = "id", ordinal = 0, type = PrimaryKeyType.CLUSTERED, ordering = Ordering.DESCENDING)
    private UUID id;
    
    @PrimaryKeyColumn(name="cnpj", ordinal = 1, type = PrimaryKeyType.PARTITIONED)
    private Long cnpj;

    @PrimaryKeyColumn(name="cnaeSecundaria", ordinal = 2, type = PrimaryKeyType.PARTITIONED)
    private Integer cnaeSecundaria;

}