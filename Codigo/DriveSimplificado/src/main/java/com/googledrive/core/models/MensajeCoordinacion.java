package com.googledrive.core.models;

import java.io.Serializable;

public class MensajeCoordinacion implements Serializable {
    private static final long serialVersionUID = 1L;

    public enum Tipo { SOLICITUD_ACCESO, PERMISO_CONCEDIDO, HEARTBEAT }

    private Tipo tipo;
    private int tiempoLamport;
    private String idNodoOrigen;
    private String nombreArchivo;

    public MensajeCoordinacion(Tipo tipo, int tiempoLamport, String idNodoOrigen, String nombreArchivo) {
        this.tipo = tipo;
        this.tiempoLamport = tiempoLamport;
        this.idNodoOrigen = idNodoOrigen;
        this.nombreArchivo = nombreArchivo;
    }

    public Tipo getTipo() { return tipo; }
    public int getTiempoLamport() { return tiempoLamport; }
    public String getIdNodoOrigen() { return idNodoOrigen; }
    public String getNombreArchivo() { return nombreArchivo; }
    
    @Override
    public String toString() {
        return "MensajeCoordinacion{" +
                "tipo=" + tipo +
                ", tiempoLamport=" + tiempoLamport +
                ", origen='" + idNodoOrigen + '\'' +
                ", archivo='" + nombreArchivo + '\'' +
                '}';
    }
}
