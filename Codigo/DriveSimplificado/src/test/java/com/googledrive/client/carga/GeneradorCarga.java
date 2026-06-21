package com.googledrive.client.carga;

import com.googledrive.core.models.PeticionArchivo;
import com.googledrive.core.utils.Utils;

import javax.imageio.ImageIO;
import javax.net.ssl.SSLSocketFactory;
import java.awt.BasicStroke;
import java.awt.Color;
import java.awt.Font;
import java.awt.Graphics2D;
import java.awt.RenderingHints;
import java.awt.image.BufferedImage;
import java.io.*;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Generador de carga para la prueba de tráfico de la Sección 3 de la rúbrica.
 *
 * Lanza N hilos concurrentes (>= 50 por defecto) durante D segundos (>= 60 por
 * defecto) que ejercitan las dos funciones principales del sistema —almacenamiento
 * (SUBIR/DESCARGAR) y sincronización colaborativa (EDITAR sobre un archivo
 * compartido, que es el recurso protegido por exclusión mutua distribuida)—
 * repartiendo la carga sobre los tres nodos.
 *
 * Recolecta throughput, latencia promedio y percentil 95 (p95), tasa de error y
 * la evolución del throughput por segundo (útil para visualizar la falla inducida).
 * Genera un archivo CSV, un resumen de texto y un gráfico PNG.
 *
 * Uso: GeneradorCarga [numHilos] [duracionSegundos]
 */
public class GeneradorCarga {

    private static final String HOST = "127.0.0.1";
    private static final int[] PUERTOS_NODOS = {9000, 9001, 9002};
    private static final String ARCHIVO_COMPARTIDO = "documento_compartido.txt";
    private static final String DIR_SALIDA = "resultados_carga";

    private static final AtomicLong totalOps = new AtomicLong(0);
    private static final AtomicLong totalErrores = new AtomicLong(0);
    private static final List<Long> latenciasMs = Collections.synchronizedList(new ArrayList<>());
    private static long[] opsPorSegundo;
    private static long[] erroresPorSegundo;

    private static volatile long inicioGlobalMs;
    private static volatile boolean enEjecucion = true;

    public static void main(String[] args) throws Exception {
        int numHilos = args.length >= 1 ? Integer.parseInt(args[0]) : 50;
        int duracionSeg = args.length >= 2 ? Integer.parseInt(args[1]) : 60;

        System.setProperty("javax.net.ssl.trustStore", "keystore.jks");
        System.setProperty("javax.net.ssl.trustStorePassword", "password");

        opsPorSegundo = new long[duracionSeg + 2];
        erroresPorSegundo = new long[duracionSeg + 2];

        System.out.println("=== PRUEBA DE TRÁFICO (CARGA) ===");
        System.out.println("Hilos concurrentes: " + numHilos + " | Duración: " + duracionSeg + " s");
        System.out.println("Funciones ejercitadas: SUBIR, DESCARGAR y EDITAR (recurso crítico compartido).");
        System.out.println("Sugerencia: derribe un nodo a mitad de la prueba para evaluar la falla inducida.\n");

        long finMs = System.currentTimeMillis() + duracionSeg * 1000L;
        inicioGlobalMs = System.currentTimeMillis();

        CountDownLatch listos = new CountDownLatch(numHilos);
        List<Thread> hilos = new ArrayList<>();
        for (int i = 0; i < numHilos; i++) {
            final int idHilo = i;
            Thread t = new Thread(() -> {
                listos.countDown();
                java.util.Random rnd = new java.util.Random(idHilo);
                while (System.currentTimeMillis() < finMs) {
                    int dado = rnd.nextInt(100);
                    int idxNodo = rnd.nextInt(PUERTOS_NODOS.length);
                    boolean ok = false;
                    // Failover en el cliente: si el nodo elegido no responde (caído), se reintenta
                    // en otro nodo. Esto evidencia la recuperación: tras la falla los errores se
                    // reducen al redirigir el tráfico hacia los nodos vivos.
                    for (int intento = 0; intento < PUERTOS_NODOS.length && !ok; intento++) {
                        int puerto = PUERTOS_NODOS[(idxNodo + intento) % PUERTOS_NODOS.length];
                        try {
                            if (dado < 60) {
                                ejecutarEditar(puerto, idHilo);      // 60% recurso crítico compartido
                            } else if (dado < 85) {
                                ejecutarSubir(puerto, idHilo);       // 25% almacenamiento
                            } else {
                                ejecutarDescargar(puerto, idHilo);   // 15% descarga
                            }
                            ok = true;
                        } catch (Exception e) {
                            // se intenta con el siguiente nodo
                        }
                    }
                    if (!ok) registrarError();
                }
            }, "carga-" + i);
            t.start();
            hilos.add(t);
        }

        // Reporte de progreso por consola cada 5 s.
        Thread progreso = new Thread(() -> {
            while (enEjecucion) {
                try { Thread.sleep(5000); } catch (InterruptedException e) { return; }
                long transcurridoS = (System.currentTimeMillis() - inicioGlobalMs) / 1000;
                System.out.println("  [t=" + transcurridoS + "s] ops=" + totalOps.get()
                        + " errores=" + totalErrores.get());
            }
        });
        progreso.setDaemon(true);
        progreso.start();

        for (Thread t : hilos) t.join();
        enEjecucion = false;

        long duracionRealMs = System.currentTimeMillis() - inicioGlobalMs;
        generarReporte(numHilos, duracionSeg, duracionRealMs);
    }

    // ---------- Operaciones ----------

    private static void ejecutarSubir(int puerto, int idHilo) throws IOException {
        String nombre = "carga_" + idHilo + ".dat";
        byte[] datos = ("Datos del hilo " + idHilo + " - " + System.nanoTime()).getBytes();
        long t0 = System.currentTimeMillis();
        SSLSocketFactory ssf = (SSLSocketFactory) SSLSocketFactory.getDefault();
        try (Socket socket = ssf.createSocket(HOST, puerto);
             OutputStream out = socket.getOutputStream();
             InputStream in = socket.getInputStream();
             ObjectOutputStream oos = new ObjectOutputStream(out);
             ObjectInputStream ois = new ObjectInputStream(in)) {
            PeticionArchivo p = new PeticionArchivo(PeticionArchivo.Operacion.SUBIR, nombre, datos.length);
            p.setChecksum(Utils.calcularChecksum(datos));
            p.setContenido(datos);
            oos.writeObject(p);
            oos.flush();
            String resp = ois.readUTF();
            registrarLatencia(t0);
            if (resp == null || !resp.startsWith("ÉXITO")) registrarError();
        }
    }

    private static void ejecutarEditar(int puerto, int idHilo) throws IOException {
        byte[] datos = ("Hilo " + idHilo + " edita @ " + System.nanoTime() + "\n").getBytes();
        long t0 = System.currentTimeMillis();
        SSLSocketFactory ssf = (SSLSocketFactory) SSLSocketFactory.getDefault();
        try (Socket socket = ssf.createSocket(HOST, puerto);
             OutputStream out = socket.getOutputStream();
             InputStream in = socket.getInputStream();
             ObjectOutputStream oos = new ObjectOutputStream(out);
             ObjectInputStream ois = new ObjectInputStream(in)) {
            PeticionArchivo p = new PeticionArchivo(PeticionArchivo.Operacion.EDITAR, ARCHIVO_COMPARTIDO, datos.length);
            p.setChecksum(Utils.calcularChecksum(datos));
            p.setContenido(datos);
            oos.writeObject(p);
            oos.flush();
            String resp = ois.readUTF();
            registrarLatencia(t0); // latencia = tiempo hasta confirmar la edición en la sección crítica
            if (resp == null || !resp.startsWith("ÉXITO")) registrarError();
        }
    }

    private static void ejecutarDescargar(int puerto, int idHilo) throws IOException {
        long t0 = System.currentTimeMillis();
        SSLSocketFactory ssf = (SSLSocketFactory) SSLSocketFactory.getDefault();
        try (Socket socket = ssf.createSocket(HOST, puerto);
             OutputStream out = socket.getOutputStream();
             InputStream in = socket.getInputStream();
             ObjectOutputStream oos = new ObjectOutputStream(out)) {
            PeticionArchivo p = new PeticionArchivo(PeticionArchivo.Operacion.DESCARGAR, ARCHIVO_COMPARTIDO, 0);
            oos.writeObject(p);
            oos.flush();
            byte[] buf = new byte[8192];
            long total = 0;
            int leidos;
            while ((leidos = in.read(buf)) != -1) total += leidos;
            registrarLatencia(t0);
            if (total < 0) registrarError();
        }
    }

    // ---------- Métricas ----------

    private static void registrarLatencia(long t0) {
        long lat = System.currentTimeMillis() - t0;
        latenciasMs.add(lat);
        totalOps.incrementAndGet();
        int seg = (int) ((System.currentTimeMillis() - inicioGlobalMs) / 1000);
        if (seg >= 0 && seg < opsPorSegundo.length) opsPorSegundo[seg]++;
    }

    private static void registrarError() {
        totalErrores.incrementAndGet();
        int seg = (int) ((System.currentTimeMillis() - inicioGlobalMs) / 1000);
        if (seg >= 0 && seg < erroresPorSegundo.length) erroresPorSegundo[seg]++;
    }

    private static void generarReporte(int numHilos, int duracionSeg, long duracionRealMs) throws IOException {
        new File(DIR_SALIDA).mkdirs();

        List<Long> copia = new ArrayList<>(latenciasMs);
        Collections.sort(copia);
        long n = copia.size();
        double prom = copia.stream().mapToLong(Long::longValue).average().orElse(0);
        long p95 = copia.isEmpty() ? 0 : copia.get((int) Math.min(n - 1, Math.floor(0.95 * n)));
        long p50 = copia.isEmpty() ? 0 : copia.get((int) (n / 2));
        long max = copia.isEmpty() ? 0 : copia.get((int) (n - 1));
        double throughput = totalOps.get() / (duracionRealMs / 1000.0);
        long errores = totalErrores.get();
        double tasaError = (totalOps.get() + errores) == 0 ? 0
                : 100.0 * errores / (totalOps.get() + errores);

        StringBuilder sb = new StringBuilder();
        sb.append("==================== RESULTADOS DE LA PRUEBA DE TRÁFICO ====================\n");
        sb.append(String.format("Hilos concurrentes:        %d%n", numHilos));
        sb.append(String.format("Duración objetivo:         %d s (real: %.1f s)%n", duracionSeg, duracionRealMs / 1000.0));
        sb.append(String.format("Operaciones exitosas:      %d%n", totalOps.get()));
        sb.append(String.format("Operaciones con error:     %d%n", errores));
        sb.append(String.format("Throughput:                %.2f ops/seg%n", throughput));
        sb.append(String.format("Latencia promedio:         %.2f ms%n", prom));
        sb.append(String.format("Latencia mediana (p50):    %d ms%n", p50));
        sb.append(String.format("Latencia p95:              %d ms%n", p95));
        sb.append(String.format("Latencia máxima:           %d ms%n", max));
        sb.append(String.format("Tasa de error:             %.2f %%%n", tasaError));
        sb.append("Nota: la cantidad de mensajes de coordinación (Ricart-Agrawala) y de\n");
        sb.append("heartbeats se obtiene de las líneas [Métricas] / [Métricas-FINAL] de cada nodo.\n");
        sb.append("===========================================================================\n");
        String resumen = sb.toString();
        System.out.println("\n" + resumen);

        try (PrintWriter pw = new PrintWriter(new FileWriter(DIR_SALIDA + "/resumen_metricas.txt"))) {
            pw.print(resumen);
        }

        try (PrintWriter pw = new PrintWriter(new FileWriter(DIR_SALIDA + "/throughput_por_segundo.csv"))) {
            pw.println("segundo,operaciones,errores");
            for (int s = 0; s < duracionSeg && s < opsPorSegundo.length; s++) {
                pw.println(s + "," + opsPorSegundo[s] + "," + erroresPorSegundo[s]);
            }
        }

        generarGrafico(duracionSeg, p95, prom, throughput, tasaError);
        System.out.println("Evidencia escrita en: " + new File(DIR_SALIDA).getAbsolutePath());
    }

    private static void generarGrafico(int duracionSeg, long p95, double prom, double throughput, double tasaError) {
        int ancho = 900, alto = 480;
        int margenIzq = 70, margenInf = 70, margenSup = 70, margenDer = 30;
        int areaW = ancho - margenIzq - margenDer;
        int areaH = alto - margenSup - margenInf;

        BufferedImage img = new BufferedImage(ancho, alto, BufferedImage.TYPE_INT_RGB);
        Graphics2D g = img.createGraphics();
        g.setRenderingHint(RenderingHints.KEY_ANTIALIASING, RenderingHints.VALUE_ANTIALIAS_ON);
        g.setColor(Color.WHITE);
        g.fillRect(0, 0, ancho, alto);

        long maxOps = 1;
        for (int s = 0; s < duracionSeg && s < opsPorSegundo.length; s++) {
            maxOps = Math.max(maxOps, opsPorSegundo[s]);
        }

        g.setColor(new Color(40, 40, 40));
        g.setFont(new Font("SansSerif", Font.BOLD, 16));
        g.drawString("Prueba de tráfico: throughput por segundo", margenIzq, 30);
        g.setFont(new Font("SansSerif", Font.PLAIN, 12));
        g.drawString(String.format("Throughput medio %.1f ops/s | latencia prom %.0f ms | p95 %d ms | error %.2f%%",
                throughput, prom, p95, tasaError), margenIzq, 50);

        // Ejes
        g.setColor(Color.GRAY);
        g.drawLine(margenIzq, margenSup, margenIzq, margenSup + areaH);
        g.drawLine(margenIzq, margenSup + areaH, margenIzq + areaW, margenSup + areaH);
        g.setColor(new Color(120, 120, 120));
        g.drawString("ops/s", 10, margenSup + areaH / 2);
        g.drawString("segundo", margenIzq + areaW / 2 - 20, alto - 20);

        // Marcas eje Y
        g.setColor(new Color(220, 220, 220));
        for (int i = 0; i <= 5; i++) {
            int y = margenSup + areaH - (int) (areaH * i / 5.0);
            g.drawLine(margenIzq, y, margenIzq + areaW, y);
            g.setColor(new Color(120, 120, 120));
            g.drawString(String.valueOf(maxOps * i / 5), margenIzq - 40, y + 4);
            g.setColor(new Color(220, 220, 220));
        }

        // Línea de throughput
        g.setColor(new Color(30, 110, 200));
        g.setStroke(new BasicStroke(2f));
        int puntos = Math.min(duracionSeg, opsPorSegundo.length);
        int prevX = -1, prevY = -1;
        for (int s = 0; s < puntos; s++) {
            int x = margenIzq + (int) (areaW * (s / (double) Math.max(1, puntos - 1)));
            int y = margenSup + areaH - (int) (areaH * (opsPorSegundo[s] / (double) maxOps));
            if (prevX >= 0) g.drawLine(prevX, prevY, x, y);
            prevX = x;
            prevY = y;
        }

        g.dispose();
        try {
            ImageIO.write(img, "png", new File(DIR_SALIDA + "/grafico_throughput.png"));
        } catch (IOException e) {
            System.err.println("No se pudo escribir el gráfico: " + e.getMessage());
        }
    }
}
