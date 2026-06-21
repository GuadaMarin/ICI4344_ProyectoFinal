package com.googledrive.core.models;

import java.io.Serializable;

public class PeticionArchivo implements Serializable {
    private static final long serialVersionUID = 1L;
    
    public enum Operacion { SUBIR, DESCARGAR, EDITAR }
    
    private Operacion tipoOperacion;
    private String nombreArchivo;
    private long tamanoBytes;
    private String checksum; // Para validación de integridad
    private byte[] contenido; // Carga útil serializada junto a la petición (marshalling)
    
    // Agregado para el proyecto final (Relojes Lamport y Origen)
    private int timestampLamport;
    private String idNodoOrigen;

    public PeticionArchivo(Operacion tipoOperacion, String nombreArchivo, long tamanoBytes) {
        this.tipoOperacion = tipoOperacion;
        this.nombreArchivo = nombreArchivo;
        this.tamanoBytes = tamanoBytes;
    }

    public PeticionArchivo(Operacion tipoOperacion, String nombreArchivo, long tamanoBytes, int timestampLamport, String idNodoOrigen) {
        this.tipoOperacion = tipoOperacion;
        this.nombreArchivo = nombreArchivo;
        this.tamanoBytes = tamanoBytes;
        this.timestampLamport = timestampLamport;
        this.idNodoOrigen = idNodoOrigen;
    }

    public Operacion getTipoOperacion() { return tipoOperacion; }
    public String getNombreArchivo() { return nombreArchivo; }
    public long getTamanoBytes() { return tamanoBytes; }
    
    public String getChecksum() { return checksum; }
    public void setChecksum(String checksum) { this.checksum = checksum; }

    public byte[] getContenido() { return contenido; }
    public void setContenido(byte[] contenido) { this.contenido = contenido; }

    public int getTimestampLamport() { return timestampLamport; }
    public void setTimestampLamport(int timestampLamport) { this.timestampLamport = timestampLamport; }

    public String getIdNodoOrigen() { return idNodoOrigen; }
    public void setIdNodoOrigen(String idNodoOrigen) { this.idNodoOrigen = idNodoOrigen; }
}