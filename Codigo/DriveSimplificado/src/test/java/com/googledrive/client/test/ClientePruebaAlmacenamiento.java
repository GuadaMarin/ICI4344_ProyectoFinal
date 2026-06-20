package com.googledrive.client.test;

import com.googledrive.core.models.PeticionArchivo;
import com.googledrive.core.utils.Utils;
import java.io.*;
import java.net.Socket;
import javax.net.ssl.SSLSocketFactory;

public class ClientePruebaAlmacenamiento {
    private static final String HOST = "127.0.0.1";
    private static final int PUERTO = 9000;

    public static void main(String[] args) {
        // Configuramos el trustStore para confiar en el certificado autofirmado del servidor local
        System.setProperty("javax.net.ssl.trustStore", "keystore.jks");
        System.setProperty("javax.net.ssl.trustStorePassword", "password");

        System.out.println("Iniciando prueba de red TLS (Cliente -> Nodo de Almacenamiento)...");
        ejecutarPruebaSubida();
    }

    private static void ejecutarPruebaSubida() {
        SSLSocketFactory ssf = (SSLSocketFactory) SSLSocketFactory.getDefault();
        
        // Se utiliza try-with-resources para garantizar el cierre del Socket seguro
        try (Socket socket = ssf.createSocket(HOST, PUERTO);
             // El ObjectOutputStream debe inicializarse ANTES que el ObjectInputStream para evitar bloqueos
             ObjectOutputStream oos = new ObjectOutputStream(socket.getOutputStream());
             ObjectInputStream ois = new ObjectInputStream(socket.getInputStream())) {

            // 1. Generar un archivo simulado en memoria
            String contenidoSimulado = "Este es un flujo de bytes de prueba para el sistema distribuido. Archivo generado correctamente.";
            byte[] bytesArchivo = contenidoSimulado.getBytes();
            String nombreArchivo = "prueba_conexion.txt";

            System.out.println("Conectado al servidor TLS. Transfiriendo archivo: " + nombreArchivo);

            // 2. Transmisión de Control (Marshalling avanzado con Checksum)
            String md5 = Utils.calcularChecksum(bytesArchivo);
            PeticionArchivo peticion = new PeticionArchivo(
                PeticionArchivo.Operacion.SUBIR, 
                nombreArchivo, 
                bytesArchivo.length,
                1,
                "cliente-prueba"
            );
            peticion.setChecksum(md5);
            
            oos.writeObject(peticion);
            oos.flush();

            // 3. Transmisión de Datos Binarios (payload con longitud prefijada)
            oos.writeInt(bytesArchivo.length);
            oos.write(bytesArchivo);
            oos.flush();

            // 4. Recepción de Confirmación
            String respuestaServidor = ois.readUTF();
            System.out.println("Respuesta del servidor: " + respuestaServidor);

        } catch (IOException e) {
            System.err.println("Fallo en la conexión segura de red: " + e.getMessage());
        }
    }
}