package com.googledrive.storage.server;

import java.io.*;
import java.security.MessageDigest;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class GestorArchivosLocal {
    private static final String DIRECTORIO_BASE = "./storage_data/";
    private static final int BUFFER_SIZE = 8192;
    
    // mapa para guardar los locks de cada archivo y que no se pisen al escribir
    private static final ConcurrentHashMap<String, ReentrantReadWriteLock> fileLocks = new ConcurrentHashMap<>();

    public GestorArchivosLocal() {
        File dir = new File(DIRECTORIO_BASE);
        if (!dir.exists()) {
            dir.mkdirs();
        }
    }

    private ReentrantReadWriteLock obtenerLock(String nombreArchivo) {
        return fileLocks.computeIfAbsent(nombreArchivo, k -> new ReentrantReadWriteLock());
    }

    private String bytesToHex(byte[] bytes) {
        StringBuilder sb = new StringBuilder();
        for (byte b : bytes) {
            sb.append(String.format("%02x", b));
        }
        return sb.toString();
    }

    public String guardarArchivo(String nombreArchivo, InputStream redIn, long tamano) throws IOException {
        ReentrantReadWriteLock lock = obtenerLock(nombreArchivo);
        lock.writeLock().lock(); // bloqueamos para que nadie mas escriba aca (region critica)
        
        try (FileOutputStream fos = new FileOutputStream(DIRECTORIO_BASE + nombreArchivo);
             BufferedOutputStream bos = new BufferedOutputStream(fos)) {
            
            MessageDigest md = MessageDigest.getInstance("MD5");
            byte[] buffer = new byte[BUFFER_SIZE];
            int bytesLeidos;
            long totalLeido = 0;
            
            // pasamos los bytes directo y calculamos checksum
            while (totalLeido < tamano && (bytesLeidos = redIn.read(buffer)) != -1) {
                bos.write(buffer, 0, bytesLeidos);
                md.update(buffer, 0, bytesLeidos);
                totalLeido += bytesLeidos;
            }
            bos.flush();
            return bytesToHex(md.digest());
        } catch (Exception e) {
            throw new IOException("Fallo en guardado de archivo", e);
        } finally {
            lock.writeLock().unlock(); // soltamos el lock
        }
    }

    // Variante que recibe el contenido ya deserializado (marshalling vía objeto), evitando
    // mezclar ObjectInputStream con lecturas crudas del socket.
    public String guardarArchivo(String nombreArchivo, byte[] datos) throws IOException {
        try (InputStream entrada = new ByteArrayInputStream(datos)) {
            return guardarArchivo(nombreArchivo, entrada, datos.length);
        }
    }

    public String editarArchivo(String nombreArchivo, byte[] datos) throws IOException {
        try (InputStream entrada = new ByteArrayInputStream(datos)) {
            return editarArchivo(nombreArchivo, entrada, datos.length);
        }
    }

    public String enviarArchivo(String nombreArchivo, OutputStream redOut) throws IOException {
        ReentrantReadWriteLock lock = obtenerLock(nombreArchivo);

        // Leemos el archivo a memoria SOLO mientras se mantiene el lock de lectura (operación local y rápida);
        // el envío por red, que puede ser lento, se realiza FUERA del lock para no bloquear a los escritores.
        byte[] datos;
        lock.readLock().lock();
        try {
            File archivo = new File(DIRECTORIO_BASE + nombreArchivo);
            if (!archivo.exists()) {
                throw new FileNotFoundException("El archivo solicitado no existe en este nodo.");
            }
            datos = java.nio.file.Files.readAllBytes(archivo.toPath());
        } finally {
            lock.readLock().unlock();
        }

        try {
            MessageDigest md = MessageDigest.getInstance("MD5");
            redOut.write(datos);
            redOut.flush();
            md.update(datos);
            return bytesToHex(md.digest());
        } catch (Exception e) {
            throw new IOException("Fallo al enviar archivo", e);
        }
    }

    public String editarArchivo(String nombreArchivo, InputStream redIn, long tamano) throws IOException {
        ReentrantReadWriteLock lock = obtenerLock(nombreArchivo);
        lock.writeLock().lock(); // bloqueamos para escribir (region critica)
        
        // le ponemos true para que escriba al final del archivo y no borre lo que ya estaba
        try (FileOutputStream fos = new FileOutputStream(DIRECTORIO_BASE + nombreArchivo, true);
             BufferedOutputStream bos = new BufferedOutputStream(fos)) {
            
            MessageDigest md = MessageDigest.getInstance("MD5");
            
            // le mandamos la hora del server apenas entra al lock
            // asi queda guardado el orden real en el que entraron las ediciones
            String timestamp = "[" + java.time.LocalTime.now().format(java.time.format.DateTimeFormatter.ofPattern("HH:mm:ss.SSS")) + "] ";
            bos.write(timestamp.getBytes());

            byte[] buffer = new byte[BUFFER_SIZE];
            int bytesLeidos;
            long totalLeido = 0;
            
            while (totalLeido < tamano && (bytesLeidos = redIn.read(buffer)) != -1) {
                bos.write(buffer, 0, bytesLeidos);
                md.update(buffer, 0, bytesLeidos);
                totalLeido += bytesLeidos;
            }
            // un enter para que no quede todo en la misma linea
            byte[] newline = "\n".getBytes();
            bos.write(newline);
            bos.flush();
            
            return bytesToHex(md.digest());
        } catch (Exception e) {
            throw new IOException("Fallo en edicion de archivo", e);
        } finally {
            lock.writeLock().unlock(); // soltamos el lock
        }
    }
}