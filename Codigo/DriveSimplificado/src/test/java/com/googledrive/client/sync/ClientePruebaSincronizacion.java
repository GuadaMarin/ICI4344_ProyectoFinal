package com.googledrive.client.sync;

import com.googledrive.core.models.PeticionArchivo;
import com.googledrive.core.utils.Utils;

import javax.net.ssl.SSLSocketFactory;
import java.io.*;
import java.net.Socket;
import java.util.concurrent.atomic.AtomicInteger;

public class ClientePruebaSincronizacion {
    private static final String HOST = "127.0.0.1";
    private static final int PUERTO = 9000;
    private static final String ARCHIVO_SYNC = "documento_compartido.txt";

    // Reloj lógico del cliente: cada edición lleva su propia marca de Lamport.
    private static final AtomicInteger relojCliente = new AtomicInteger(0);

    public static void main(String[] args) {
        System.setProperty("javax.net.ssl.trustStore", "keystore.jks");
        System.setProperty("javax.net.ssl.trustStorePassword", "password");

        System.out.println("=== Iniciando Prueba de Sincronización Concurrente (TLS) ===");
        System.out.println("Se lanzarán 3 clientes simultáneos para editar el mismo archivo...\n");

        // creamos 3 hilos para probar que 3 usuarios distintos editen el archivo a la vez
        Thread cliente1 = new Thread(() -> editarDocumento("Usuario_1", "Hola, soy el Usuario 1 agregando texto.\n"));
        Thread cliente2 = new Thread(() -> editarDocumento("Usuario_2", "Este es el aporte del Usuario 2 al documento.\n"));
        Thread cliente3 = new Thread(() -> editarDocumento("Usuario_3", "Ahora el Usuario 3 escribe su parte.\n"));

        // le damos start a los hilos para que corran en paralelo y forzar la concurrencia
        cliente1.start();
        cliente2.start();
        cliente3.start();

        try {
            // esperamos que terminen los threads
            cliente1.join();
            cliente2.join();
            cliente3.join();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        System.out.println("\n=== Prueba de Sincronización Finalizada ===");
    }

    private static void editarDocumento(String nombreUsuario, String textoAgregar) {
        // le puse un delay random para que no lleguen exactamente al mismo milisegundo y se vea mas real
        // como si la gente se demorara distinto en escribir
        try {
            int delay = (int) (Math.random() * 1500);
            Thread.sleep(delay);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        SSLSocketFactory ssf = (SSLSocketFactory) SSLSocketFactory.getDefault();

        try (Socket socket = ssf.createSocket(HOST, PUERTO);
                ObjectOutputStream oos = new ObjectOutputStream(socket.getOutputStream());
                ObjectInputStream ois = new ObjectInputStream(socket.getInputStream())) {

            byte[] bytesAporte = textoAgregar.getBytes();
            String md5 = Utils.calcularChecksum(bytesAporte);

            System.out.println("[" + nombreUsuario + "] Conectado de forma segura. Enviando edición...");

            // mandamos la peticion con el enum EDITAR, el checksum y la marca de Lamport
            PeticionArchivo peticion = new PeticionArchivo(
                    PeticionArchivo.Operacion.EDITAR,
                    ARCHIVO_SYNC,
                    bytesAporte.length,
                    relojCliente.incrementAndGet(),
                    nombreUsuario);
            peticion.setChecksum(md5);
            oos.writeObject(peticion);
            oos.flush();

            // mandamos el texto en bytes (payload con longitud prefijada)
            oos.writeInt(bytesAporte.length);
            oos.write(bytesAporte);
            oos.flush();

            // esperamos la respuesta del server
            String confirmacion = ois.readUTF();
            System.out.println("[" + nombreUsuario + "] Servidor responde: " + confirmacion);

            // leemos el estado completo del documento devuelto por el servidor
            int longitud = ois.readInt();
            byte[] buffer = new byte[longitud];
            ois.readFully(buffer);

            String estadoFinal = new String(buffer);
            System.out.println("\n--- ESTADO DEL DOCUMENTO VISTO POR [" + nombreUsuario + "] ---\n" + estadoFinal
                    + "--------------------------------------------------------\n");

        } catch (IOException e) {
            System.err.println("[" + nombreUsuario + "] Fallo en la conexión: " + e.getMessage());
        }
    }
}
