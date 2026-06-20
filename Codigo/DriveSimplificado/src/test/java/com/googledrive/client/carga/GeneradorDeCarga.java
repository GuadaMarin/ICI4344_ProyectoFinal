package com.googledrive.client.carga;

import com.googledrive.core.models.MensajeCoordinacion;
import com.googledrive.core.models.PeticionArchivo;
import com.googledrive.core.utils.CanalCoordinacion;
import com.googledrive.core.utils.Utils;

import javax.net.ssl.SSLSocketFactory;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Generador de carga concurrente (Sección 3 de la rúbrica).
 *
 * Lanza N clientes (hilos) durante D segundos sostenidos, repartiendo las
 * peticiones entre los nodos disponibles y ejercitando las dos funciones
 * principales (subir y editar/sincronizar), con foco en el recurso protegido
 * por exclusión mutua (un mismo archivo editado por todos).
 *
 * Recolecta: throughput, latencia promedio, latencia p95 y tasa de error.
 * Los mensajes de coordinación quedan registrados en los logs de cada nodo.
 *
 * Uso:
 *   java -cp target/classes:target/test-classes com.googledrive.client.carga.GeneradorDeCarga \
 *        [numClientes] [duracionSeg] [nodoADerribar] [segHastaDerribo]
 *
 * Ejemplo con falla inducida del nodo2 a los 30 s:
 *   java -cp target/classes:target/test-classes com.googledrive.client.carga.GeneradorDeCarga 50 60 nodo2 30
 */
public class GeneradorDeCarga {

    private static final String HOST = "127.0.0.1";
    // Puertos de servicio a clientes (TLS) de cada nodo.
    private static final int[] PUERTOS_NODOS = {9000, 9001, 9002};
    private static final String ARCHIVO_COMPARTIDO = "documento_compartido.txt";

    // Métricas globales.
    private static final AtomicLong exitos = new AtomicLong(0);
    private static final AtomicLong errores = new AtomicLong(0);
    private static final ConcurrentLinkedQueue<Long> latenciasNanos = new ConcurrentLinkedQueue<>();
    private static final AtomicInteger relojGlobalClientes = new AtomicInteger(0);

    public static void main(String[] args) throws InterruptedException {
        int numClientes = args.length >= 1 ? Integer.parseInt(args[0]) : 50;
        int duracionSeg = args.length >= 2 ? Integer.parseInt(args[1]) : 60;
        String nodoADerribar = args.length >= 3 ? args[2] : null;
        int segHastaDerribo = args.length >= 4 ? Integer.parseInt(args[3]) : duracionSeg / 2;

        System.setProperty("javax.net.ssl.trustStore", "keystore.jks");
        System.setProperty("javax.net.ssl.trustStorePassword", "password");

        System.out.println("=== Generador de carga ===");
        System.out.println("Clientes concurrentes: " + numClientes);
        System.out.println("Duración: " + duracionSeg + " s");
        System.out.println("Nodos destino (puertos): " + java.util.Arrays.toString(PUERTOS_NODOS));
        if (nodoADerribar != null) {
            System.out.println("Falla inducida: se derribará " + nodoADerribar + " a los " + segHastaDerribo + " s");
        }
        System.out.println("==========================\n");

        long instanteFin = System.currentTimeMillis() + duracionSeg * 1000L;
        CountDownLatch listo = new CountDownLatch(numClientes);

        List<Thread> hilos = new ArrayList<>();
        for (int i = 0; i < numClientes; i++) {
            final int idCliente = i;
            Thread t = new Thread(() -> ejecutarCliente(idCliente, instanteFin, listo), "carga-" + i);
            hilos.add(t);
            t.start();
        }

        if (nodoADerribar != null) {
            programarFallaInducida(nodoADerribar, segHastaDerribo);
        }

        for (Thread t : hilos) {
            t.join();
        }

        imprimirMetricas(duracionSeg);
    }

    private static void ejecutarCliente(int idCliente, long instanteFin, CountDownLatch listo) {
        listo.countDown();
        while (System.currentTimeMillis() < instanteFin) {
            int operacion = ThreadLocalRandom.current().nextInt(100);
            int puerto = PUERTOS_NODOS[ThreadLocalRandom.current().nextInt(PUERTOS_NODOS.length)];
            long t0 = System.nanoTime();
            try {
                if (operacion < 50) {
                    // EDITAR el archivo compartido -> recurso bajo exclusión mutua
                    editar(puerto, "Aporte del cliente " + idCliente + "\n");
                } else if (operacion < 80) {
                    subir(puerto, idCliente);
                } else {
                    descargar(puerto);
                }
                latenciasNanos.add(System.nanoTime() - t0);
                exitos.incrementAndGet();
            } catch (Exception e) {
                errores.incrementAndGet();
            }
        }
    }

    private static SSLSocketFactory fabrica() {
        return (SSLSocketFactory) SSLSocketFactory.getDefault();
    }

    private static void editar(int puerto, String texto) throws Exception {
        byte[] bytes = texto.getBytes();
        try (Socket socket = fabrica().createSocket(HOST, puerto);
             ObjectOutputStream oos = new ObjectOutputStream(socket.getOutputStream());
             ObjectInputStream ois = new ObjectInputStream(socket.getInputStream())) {

            socket.setSoTimeout(30000);
            PeticionArchivo peticion = new PeticionArchivo(
                    PeticionArchivo.Operacion.EDITAR, ARCHIVO_COMPARTIDO, bytes.length,
                    relojGlobalClientes.incrementAndGet(), "carga");
            peticion.setChecksum(Utils.calcularChecksum(bytes));
            oos.writeObject(peticion);
            oos.flush();
            escribirPayload(oos, bytes);

            String resp = ois.readUTF();
            if (resp.startsWith("ERROR")) {
                throw new java.io.IOException(resp);
            }
            // Consumimos el estado del documento devuelto por el servidor.
            leerPayload(ois);
        }
    }

    private static void subir(int puerto, int idCliente) throws Exception {
        String contenido = "Archivo de carga del cliente " + idCliente + " ts=" + System.nanoTime();
        byte[] bytes = contenido.getBytes();
        String nombre = "carga_cliente_" + idCliente + ".txt";
        try (Socket socket = fabrica().createSocket(HOST, puerto);
             ObjectOutputStream oos = new ObjectOutputStream(socket.getOutputStream());
             ObjectInputStream ois = new ObjectInputStream(socket.getInputStream())) {

            socket.setSoTimeout(30000);
            PeticionArchivo peticion = new PeticionArchivo(
                    PeticionArchivo.Operacion.SUBIR, nombre, bytes.length,
                    relojGlobalClientes.incrementAndGet(), "carga");
            peticion.setChecksum(Utils.calcularChecksum(bytes));
            oos.writeObject(peticion);
            oos.flush();
            escribirPayload(oos, bytes);

            String resp = ois.readUTF();
            if (resp.startsWith("ERROR")) {
                throw new java.io.IOException(resp);
            }
        }
    }

    private static void descargar(int puerto) throws Exception {
        try (Socket socket = fabrica().createSocket(HOST, puerto);
             ObjectOutputStream oos = new ObjectOutputStream(socket.getOutputStream());
             ObjectInputStream ois = new ObjectInputStream(socket.getInputStream())) {

            socket.setSoTimeout(30000);
            PeticionArchivo peticion = new PeticionArchivo(
                    PeticionArchivo.Operacion.DESCARGAR, ARCHIVO_COMPARTIDO, 0,
                    relojGlobalClientes.incrementAndGet(), "carga");
            oos.writeObject(peticion);
            oos.flush();

            String resp = ois.readUTF();
            if (resp.startsWith("ERROR")) {
                // Archivo aún no creado: no se cuenta como fallo del sistema.
                return;
            }
            leerPayload(ois);
        }
    }

    private static void escribirPayload(ObjectOutputStream oos, byte[] datos) throws java.io.IOException {
        oos.writeInt(datos.length);
        oos.write(datos);
        oos.flush();
    }

    private static byte[] leerPayload(ObjectInputStream ois) throws java.io.IOException {
        int longitud = ois.readInt();
        byte[] datos = new byte[longitud];
        ois.readFully(datos);
        return datos;
    }

    private static void programarFallaInducida(String nodoADerribar, int segHastaDerribo) {
        Thread asesino = new Thread(() -> {
            try {
                Thread.sleep(segHastaDerribo * 1000L);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
            System.out.println("\n>>> [FALLA INDUCIDA] Derribando " + nodoADerribar + " en t=" + segHastaDerribo + "s\n");
            boolean ok = CanalCoordinacion.enviar(nodoADerribar,
                    new MensajeCoordinacion(MensajeCoordinacion.Tipo.DERRIBAR, 0, "generador", null));
            if (!ok) {
                System.err.println(">>> No se pudo contactar a " + nodoADerribar + " para derribarlo.");
            }
        }, "falla-inducida");
        asesino.setDaemon(true);
        asesino.start();
    }

    private static void imprimirMetricas(int duracionSeg) {
        long totalExitos = exitos.get();
        long totalErrores = errores.get();
        long totalPeticiones = totalExitos + totalErrores;

        List<Long> latencias = new ArrayList<>(latenciasNanos);
        Collections.sort(latencias);

        double throughput = totalExitos / (double) duracionSeg;
        double promedioMs = 0;
        double p95Ms = 0;
        if (!latencias.isEmpty()) {
            long suma = 0;
            for (long l : latencias) {
                suma += l;
            }
            promedioMs = (suma / (double) latencias.size()) / 1_000_000.0;
            int indiceP95 = (int) Math.ceil(latencias.size() * 0.95) - 1;
            indiceP95 = Math.max(0, Math.min(indiceP95, latencias.size() - 1));
            p95Ms = latencias.get(indiceP95) / 1_000_000.0;
        }
        double tasaError = totalPeticiones == 0 ? 0 : (totalErrores * 100.0 / totalPeticiones);

        System.out.println("\n===== MÉTRICAS DE LA PRUEBA DE CARGA =====");
        System.out.printf("Peticiones totales : %d (éxitos=%d, errores=%d)%n",
                totalPeticiones, totalExitos, totalErrores);
        System.out.printf("Throughput         : %.2f peticiones/s%n", throughput);
        System.out.printf("Latencia promedio  : %.2f ms%n", promedioMs);
        System.out.printf("Latencia p95       : %.2f ms%n", p95Ms);
        System.out.printf("Tasa de error      : %.2f %%%n", tasaError);
        System.out.println("(Los mensajes de coordinación quedan registrados en los logs de cada nodo)");
        System.out.println("==========================================");
    }
}
