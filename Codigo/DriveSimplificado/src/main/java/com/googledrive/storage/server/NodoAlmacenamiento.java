package com.googledrive.storage.server;

import java.io.IOException;
import java.net.Socket;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import javax.net.ssl.SSLServerSocket;
import javax.net.ssl.SSLServerSocketFactory;
import com.googledrive.core.utils.RelojLamport;

public class NodoAlmacenamiento {
    
    private int puertoClientes;
    private String idNodoLocal;
    
    private ServicioExclusionMutua servicioCoordinacion;
    private RelojLamport relojLogico;
    
    private static final int HILOS_MAXIMOS = 50;
    private boolean servidorEncendido = true;

    public NodoAlmacenamiento(String idNodoLocal, int puertoClientes) {
        this.idNodoLocal = idNodoLocal;
        this.puertoClientes = puertoClientes;
        this.relojLogico = new RelojLamport();
        this.servicioCoordinacion = new ServicioExclusionMutua(idNodoLocal, relojLogico);
    }

    public void iniciarServicio() {
        System.setProperty("javax.net.ssl.keyStore", "keystore.jks");
        System.setProperty("javax.net.ssl.keyStorePassword", "password");

        // Levantar servicio para coordinación entre nodos
        servicioCoordinacion.iniciarEscuchaCoordinacion();

        // Reporte periódico de métricas de coordinación (evidencia para la prueba de tráfico).
        iniciarReporteMetricas();

        ExecutorService piscinaDeHilos = Executors.newFixedThreadPool(HILOS_MAXIMOS);
        
        try {
            SSLServerSocketFactory fabricaCriptografica = (SSLServerSocketFactory) SSLServerSocketFactory.getDefault();
            try (SSLServerSocket socketSeguro = (SSLServerSocket) fabricaCriptografica.createServerSocket(puertoClientes)) {
                System.out.println("-> " + idNodoLocal.toUpperCase() + " iniciado (Puerto clientes: " + puertoClientes + ")");
                
                while (servidorEncendido) {
                    Socket clienteExterno = socketSeguro.accept();
                    clienteExterno.setSoTimeout(30000); 
                    
                    piscinaDeHilos.execute(new WorkerCliente(clienteExterno, servicioCoordinacion, relojLogico));
                }
            }
        } catch (IOException e) {
            System.err.println("Error iniciando NodoAlmacenamiento: " + e.getMessage());
        } finally {
            piscinaDeHilos.shutdown();
        }
    }

    private void iniciarReporteMetricas() {
        Thread reportero = new Thread(() -> {
            while (servidorEncendido) {
                try {
                    Thread.sleep(15000);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                }
                System.out.println("[Métricas] " + idNodoLocal
                        + " | mensajes coordinación (Ricart-Agrawala) = " + servicioCoordinacion.getMensajesCoordinacion()
                        + " | heartbeats enviados = " + servicioCoordinacion.getDetectorFallos().getHeartbeatsEnviados()
                        + " | reloj Lamport = " + relojLogico.obtenerTiempo()
                        + " | nodos vivos = " + servicioCoordinacion.getDetectorFallos().nodosVivos());
            }
        }, "metricas-" + idNodoLocal);
        reportero.setDaemon(true);
        reportero.start();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> System.out.println(
                "[Métricas-FINAL] " + idNodoLocal
                        + " | mensajes coordinación = " + servicioCoordinacion.getMensajesCoordinacion()
                        + " | heartbeats = " + servicioCoordinacion.getDetectorFallos().getHeartbeatsEnviados()
                        + " | reloj Lamport = " + relojLogico.obtenerTiempo())));
    }

    public static void main(String[] args) {
        String idNodo = "nodo1";
        int puerto = 9000;

        if (args.length >= 2) {
            idNodo = args[0];
            puerto = Integer.parseInt(args[1]);
        } else {
            System.out.println("Usando valores por defecto (nodo1 / 9000)");
        }

        new NodoAlmacenamiento(idNodo, puerto).iniciarServicio();
    }
}