package com.googledrive.client.sync;

import com.googledrive.core.models.PeticionArchivo;
import com.googledrive.core.utils.Utils;

import javax.net.ssl.SSLSocketFactory;
import java.io.*;
import java.net.Socket;

public class ClientePruebaSincronizacion {
    private static final String HOST = "127.0.0.1";
    private static final int PUERTO = 9000;
    private static final String ARCHIVO_SYNC = "documento_compartido.txt";

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
                OutputStream out = socket.getOutputStream();
                InputStream in = socket.getInputStream();
                ObjectOutputStream oos = new ObjectOutputStream(out);
                ObjectInputStream ois = new ObjectInputStream(in)) {

            byte[] bytesAporte = textoAgregar.getBytes();
            String md5 = Utils.calcularChecksum(bytesAporte);

            System.out.println("[" + nombreUsuario + "] Conectado de forma segura. Enviando edición...");

            // mandamos la peticion EDITAR con el contenido serializado junto a la petición
            PeticionArchivo peticion = new PeticionArchivo(
                    PeticionArchivo.Operacion.EDITAR,
                    ARCHIVO_SYNC,
                    bytesAporte.length);
            peticion.setChecksum(md5);
            peticion.setContenido(bytesAporte);
            oos.writeObject(peticion);
            oos.flush();

            // esperamos la confirmación del server
            String confirmacion = ois.readUTF();
            System.out.println("[" + nombreUsuario + "] Servidor responde: " + confirmacion);

        } catch (IOException e) {
            System.err.println("[" + nombreUsuario + "] Fallo en la conexión: " + e.getMessage());
            return;
        }

        // descargamos el documento (operación de lectura) para ver cómo quedó tras la edición
        String estadoFinal = descargarDocumento();
        System.out.println("\n--- ESTADO DEL DOCUMENTO VISTO POR [" + nombreUsuario + "] ---\n" + estadoFinal
                + "--------------------------------------------------------\n");
    }

    private static String descargarDocumento() {
        SSLSocketFactory ssf = (SSLSocketFactory) SSLSocketFactory.getDefault();
        try (Socket socket = ssf.createSocket(HOST, PUERTO);
                OutputStream out = socket.getOutputStream();
                InputStream in = socket.getInputStream();
                ObjectOutputStream oos = new ObjectOutputStream(out)) {

            oos.writeObject(new PeticionArchivo(PeticionArchivo.Operacion.DESCARGAR, ARCHIVO_SYNC, 0));
            oos.flush();

            ByteArrayOutputStream buffer = new ByteArrayOutputStream();
            byte[] buf = new byte[8192];
            int leidos;
            while ((leidos = in.read(buf)) != -1) {
                buffer.write(buf, 0, leidos);
            }
            return new String(buffer.toByteArray());
        } catch (IOException e) {
            return "(no se pudo descargar el documento: " + e.getMessage() + ")\n";
        }
    }
}
