package com.googledrive.storage.server;

import java.io.*;
import java.nio.file.Files;
import java.security.MessageDigest;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class GestorArchivosLocal {
    private static final String DIRECTORIO_BASE = "./storage_data/";

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

    private String md5(byte[] datos) {
        try {
            return bytesToHex(MessageDigest.getInstance("MD5").digest(datos));
        } catch (Exception e) {
            throw new RuntimeException("Error al calcular MD5", e);
        }
    }

    public String guardarArchivo(String nombreArchivo, byte[] datos) throws IOException {
        ReentrantReadWriteLock lock = obtenerLock(nombreArchivo);
        lock.writeLock().lock(); // bloqueamos para que nadie mas escriba aca (region critica)
        try (FileOutputStream fos = new FileOutputStream(DIRECTORIO_BASE + nombreArchivo)) {
            fos.write(datos);
            fos.flush();
            return md5(datos);
        } finally {
            lock.writeLock().unlock();
        }
    }

    public byte[] leerArchivo(String nombreArchivo) throws IOException {
        ReentrantReadWriteLock lock = obtenerLock(nombreArchivo);
        lock.readLock().lock(); // lock de lectura para que varios puedan leer a la vez
        try {
            File archivo = new File(DIRECTORIO_BASE + nombreArchivo);
            if (!archivo.exists()) {
                throw new FileNotFoundException("El archivo solicitado no existe en este nodo.");
            }
            return Files.readAllBytes(archivo.toPath());
        } finally {
            lock.readLock().unlock();
        }
    }

    public String editarArchivo(String nombreArchivo, byte[] datos, String marcaLogica) throws IOException {
        ReentrantReadWriteLock lock = obtenerLock(nombreArchivo);
        lock.writeLock().lock(); // bloqueamos para escribir (region critica)
        // append (true) para no borrar lo que ya estaba
        try (FileOutputStream fos = new FileOutputStream(DIRECTORIO_BASE + nombreArchivo, true)) {
            // Escribimos la marca lógica de Lamport asignada al entrar a la sección
            // crítica. El orden de las ediciones queda definido por el reloj lógico
            // (no por la hora física), demostrando ordenamiento causal sin reloj global.
            fos.write((marcaLogica + " ").getBytes());
            fos.write(datos);
            fos.write("\n".getBytes());
            fos.flush();
            return md5(datos);
        } finally {
            lock.writeLock().unlock();
        }
    }
}
