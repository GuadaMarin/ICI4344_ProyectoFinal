package com.googledrive.storage.server;

import java.io.*;
import java.net.Socket;
import java.net.SocketTimeoutException;
import com.googledrive.core.utils.RelojLamport;
import com.googledrive.core.models.PeticionArchivo;

public class WorkerCliente implements Runnable {
    private Socket socketCliente;
    private ServicioExclusionMutua servicioExclusionMutua;
    private RelojLamport relojLogico;

    public WorkerCliente(Socket socketCliente, ServicioExclusionMutua servicioExclusionMutua, RelojLamport relojLogico) {
        this.socketCliente = socketCliente;
        this.servicioExclusionMutua = servicioExclusionMutua;
        this.relojLogico = relojLogico;
    }

    @Override
    public void run() {
        try (
                InputStream in = socketCliente.getInputStream();
                OutputStream out = socketCliente.getOutputStream();
                ObjectInputStream ois = new ObjectInputStream(in);
                ObjectOutputStream oos = new ObjectOutputStream(out)) {
            
            PeticionArchivo peticion = (PeticionArchivo) ois.readObject();
            relojLogico.registrarRecepcion(peticion.getTimestampLamport());
            GestorArchivosLocal gestorDelDiscoLocal = new GestorArchivosLocal();

            if (peticion.getTipoOperacion() == PeticionArchivo.Operacion.SUBIR) {
                
                // Pedimos acceso a la red antes de modificar el disco
                servicioExclusionMutua.solicitarAccesoCritico(peticion.getNombreArchivo());
                try {
                    byte[] datos = peticion.getContenido() != null ? peticion.getContenido() : new byte[0];
                    String hashCalculado = gestorDelDiscoLocal.guardarArchivo(peticion.getNombreArchivo(), datos);
                    if (peticion.getChecksum() != null && !peticion.getChecksum().equals(hashCalculado)) {
                        oos.writeUTF("ERROR: Archivo corrupto tras transmisión. MD5 no coincide.");
                    } else {
                        oos.writeUTF("ÉXITO: Archivo almacenado correctamente.");
                    }
                } finally {
                    // Liberamos el acceso sin importar si hubo error
                    servicioExclusionMutua.liberarAccesoCritico(peticion.getNombreArchivo());
                }
                
            } else if (peticion.getTipoOperacion() == PeticionArchivo.Operacion.DESCARGAR) {
                // Descarga no requiere Ricart-Agrawala, solo ReadLock local
                gestorDelDiscoLocal.enviarArchivo(peticion.getNombreArchivo(), out);
                
            } else if (peticion.getTipoOperacion() == PeticionArchivo.Operacion.EDITAR) {
                
                servicioExclusionMutua.solicitarAccesoCritico(peticion.getNombreArchivo());
                try {
                    byte[] datos = peticion.getContenido() != null ? peticion.getContenido() : new byte[0];
                    String hashCalculado = gestorDelDiscoLocal.editarArchivo(peticion.getNombreArchivo(), datos);
                    if (peticion.getChecksum() != null && !peticion.getChecksum().equals(hashCalculado)) {
                        oos.writeUTF("ERROR: Edición corrupta (MD5 fail).");
                    } else {
                        oos.writeUTF("ÉXITO: Archivo editado.");
                    }
                } finally {
                    servicioExclusionMutua.liberarAccesoCritico(peticion.getNombreArchivo());
                }
            }
            oos.flush();

        } catch (SocketTimeoutException e) {
            System.err.println("Timeout: Conexión con cliente expiró.");
        } catch (EOFException | java.net.SocketException e) {
            System.err.println("El cliente se desconectó abruptamente.");
        } catch (IOException | ClassNotFoundException e) {
            System.err.println("Error en WorkerCliente: " + e.getMessage());
        } finally {
            try {
                if (!socketCliente.isClosed()) socketCliente.close();
            } catch (IOException e) {}
        }
    }
}