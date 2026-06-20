package com.googledrive.storage.server;
import java.io.IOException;
import java.net.Socket;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import javax.net.ssl.SSLServerSocket;
import javax.net.ssl.SSLServerSocketFactory;

public class StorageServer {
    private static final int PUERTO = 9000;
    private static final int MAX_HILOS = 50; // Pool de hilos para alta concurrencia
    private boolean activo = true;

    public void iniciar() {
        // Configuramos las propiedades de seguridad para TLS
        System.setProperty("javax.net.ssl.keyStore", "keystore.jks");
        System.setProperty("javax.net.ssl.keyStorePassword", "password");

        ExecutorService poolHilos = Executors.newFixedThreadPool(MAX_HILOS);
        
        try {
            SSLServerSocketFactory ssf = (SSLServerSocketFactory) SSLServerSocketFactory.getDefault();
            try (SSLServerSocket serverSocket = (SSLServerSocket) ssf.createServerSocket(PUERTO)) {
                System.out.println("Nodo de Almacenamiento (TLS) iniciado en puerto " + PUERTO);
                
                while (activo) {
                    // Acepta conexiones entrantes de clientes
                    Socket socketCliente = serverSocket.accept();
                    // Configuramos un timeout (ej: 30 segundos) para mitigar conexiones colgadas o ataques lentos
                    socketCliente.setSoTimeout(30000); 
                    System.out.println("Nueva conexión segura desde: " + socketCliente.getInetAddress().getHostAddress());
                    
                    // Asigna la conexión a un hilo independiente
                    poolHilos.execute(new FtpWorker(socketCliente));
                }
            }
        } catch (IOException e) {
            System.err.println("Fallo crítico en el servidor de almacenamiento: " + e.getMessage());
        } finally {
            poolHilos.shutdown();
        }
    }

    public static void main(String[] args) {
        new StorageServer().iniciar();
    }
}