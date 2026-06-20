package com.googledrive.core.utils;

import com.googledrive.core.models.MensajeCoordinacion;
import com.googledrive.core.models.RegistroMembresia;

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;

/**
 * Canal único de envío de mensajes de coordinación entre nodos.
 * Centraliza la apertura del socket TCP y el marshalling del objeto
 * MensajeCoordinacion para que tanto la exclusión mutua como el servicio
 * de membresía (heartbeats) usen exactamente el mismo transporte.
 */
public final class CanalCoordinacion {

    private static final int TIMEOUT_CONEXION_MS = 1500;

    private CanalCoordinacion() {
    }

    /**
     * Envía un mensaje de coordinación a un nodo destino.
     *
     * @return true si el mensaje se entregó al socket; false si el nodo no
     *         respondió (posible caída u omisión), sin lanzar excepción.
     */
    public static boolean enviar(String nodoDestino, MensajeCoordinacion mensaje) {
        int puertoDestino = RegistroMembresia.obtenerPuerto(nodoDestino);
        if (puertoDestino == -1) {
            return false;
        }

        try (Socket socketRed = new Socket()) {
            socketRed.connect(new InetSocketAddress(RegistroMembresia.obtenerHost(), puertoDestino),
                    TIMEOUT_CONEXION_MS);
            try (ObjectOutputStream flujoSalida = new ObjectOutputStream(socketRed.getOutputStream())) {
                flujoSalida.writeObject(mensaje);
                flujoSalida.flush();
            }
            return true;
        } catch (IOException e) {
            return false;
        }
    }
}
