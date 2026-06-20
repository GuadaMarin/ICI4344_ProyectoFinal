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
    private ServicioMembresia servicioMembresia;
    private RelojLamport relojLogico;
    
    private static final int HILOS_MAXIMOS = 50;
    private boolean servidorEncendido = true;

    public NodoAlmacenamiento(String idNodoLocal, int puertoClientes) {
        this.idNodoLocal = idNodoLocal;
        this.puertoClientes = puertoClientes;
        this.relojLogico = new RelojLamport();
        this.servicioMembresia = new ServicioMembresia(idNodoLocal);
        this.servicioCoordinacion = new ServicioExclusionMutua(idNodoLocal, relojLogico, servicioMembresia);
    }

    public void iniciarServicio() {
        System.setProperty("javax.net.ssl.keyStore", "keystore.jks");
        System.setProperty("javax.net.ssl.keyStorePassword", "password");

        // Listener de coordinación entre nodos (exclusión mutua + heartbeats + falla inducida)
        servicioCoordinacion.iniciarEscuchaCoordinacion();
        // Detección de caídas por heartbeats y membresía dinámica
        servicioMembresia.iniciar();

        // Al apagar el nodo dejamos registradas las métricas de coordinación.
        Runtime.getRuntime().addShutdownHook(new Thread(() ->
                System.out.println("\n[Métricas " + idNodoLocal + "] Mensajes de coordinación -> enviados="
                        + servicioCoordinacion.getMensajesEnviados()
                        + " recibidos=" + servicioCoordinacion.getMensajesRecibidos())));

        ExecutorService piscinaDeHilos = Executors.newFixedThreadPool(HILOS_MAXIMOS);
        
        try {
            SSLServerSocketFactory fabricaCriptografica = (SSLServerSocketFactory) SSLServerSocketFactory.getDefault();
            try (SSLServerSocket socketSeguro = (SSLServerSocket) fabricaCriptografica.createServerSocket(puertoClientes)) {
                System.out.println("-> " + idNodoLocal.toUpperCase() + " iniciado (Puerto clientes: " + puertoClientes + ")");
                
                while (servidorEncendido) {
                    Socket clienteExterno = socketSeguro.accept();
                    clienteExterno.setSoTimeout(30000); 
                    
                    piscinaDeHilos.execute(new WorkerCliente(clienteExterno, servicioCoordinacion, relojLogico, idNodoLocal));
                }
            }
        } catch (IOException e) {
            System.err.println("Error iniciando NodoAlmacenamiento: " + e.getMessage());
        } finally {
            piscinaDeHilos.shutdown();
        }
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
