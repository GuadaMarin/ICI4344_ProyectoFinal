package com.googledrive.storage.server;

import java.io.*;
import java.net.Socket;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import com.googledrive.core.utils.RelojLamport;
import com.googledrive.core.models.PeticionArchivo;

public class WorkerCliente implements Runnable {
    private Socket socketCliente;
    private ServicioExclusionMutua servicioExclusionMutua;
    private RelojLamport relojLogico;
    private String idNodoLocal;

    public WorkerCliente(Socket socketCliente, ServicioExclusionMutua servicioExclusionMutua,
                         RelojLamport relojLogico, String idNodoLocal) {
        this.socketCliente = socketCliente;
        this.servicioExclusionMutua = servicioExclusionMutua;
        this.relojLogico = relojLogico;
        this.idNodoLocal = idNodoLocal;
    }

    @Override
    public void run() {
        // Todo el protocolo viaja por los Object streams (metadato + payload con
        // longitud prefijada). No se mezcla lectura cruda del socket, evitando que
        // el ObjectInputStream sobre-lea bytes del payload bajo concurrencia.
        try (ObjectInputStream ois = new ObjectInputStream(socketCliente.getInputStream());
             ObjectOutputStream oos = new ObjectOutputStream(socketCliente.getOutputStream())) {

            PeticionArchivo peticion = (PeticionArchivo) ois.readObject();
            relojLogico.registrarRecepcion(peticion.getTimestampLamport());
            GestorArchivosLocal gestor = new GestorArchivosLocal();

            switch (peticion.getTipoOperacion()) {
                case SUBIR:
                    procesarSubida(ois, oos, peticion, gestor);
                    break;
                case EDITAR:
                    procesarEdicion(ois, oos, peticion, gestor);
                    break;
                case DESCARGAR:
                    procesarDescarga(oos, peticion, gestor);
                    break;
                default:
                    oos.writeUTF("ERROR: operación desconocida.");
            }
            oos.flush();

        } catch (SocketTimeoutException e) {
            System.err.println("Timeout: la conexión con el cliente expiró.");
        } catch (EOFException | SocketException e) {
            System.err.println("El cliente se desconectó abruptamente: " + e.getMessage());
        } catch (IOException | ClassNotFoundException e) {
            System.err.println("Error en WorkerCliente: " + e.getMessage());
        } finally {
            try {
                if (!socketCliente.isClosed()) socketCliente.close();
            } catch (IOException ignored) {
            }
        }
    }

    private void procesarSubida(ObjectInputStream ois, ObjectOutputStream oos,
                                PeticionArchivo peticion, GestorArchivosLocal gestor) throws IOException {
        byte[] datos = leerPayload(ois);
        // Exclusión mutua distribuida (Ricart-Agrawala) antes de tocar el disco.
        servicioExclusionMutua.solicitarAccesoCritico(peticion.getNombreArchivo());
        try {
            String hash = gestor.guardarArchivo(peticion.getNombreArchivo(), datos);
            oos.writeUTF(validarChecksum(peticion, hash, "almacenado"));
        } finally {
            servicioExclusionMutua.liberarAccesoCritico(peticion.getNombreArchivo());
        }
    }

    private void procesarEdicion(ObjectInputStream ois, ObjectOutputStream oos,
                                 PeticionArchivo peticion, GestorArchivosLocal gestor) throws IOException {
        byte[] datos = leerPayload(ois);
        servicioExclusionMutua.solicitarAccesoCritico(peticion.getNombreArchivo());
        try {
            // Marca lógica de Lamport del momento de entrada a la sección crítica:
            // define el orden causal del documento sin reloj global.
            int marcaLamport = relojLogico.registrarEventoLocal();
            String marcaLogica = "[Lamport=" + marcaLamport + " | nodo=" + idNodoLocal
                    + " | autor=" + peticion.getIdNodoOrigen() + "]";
            String hash = gestor.editarArchivo(peticion.getNombreArchivo(), datos, marcaLogica);
            oos.writeUTF(validarChecksum(peticion, hash, "editado"));
            // Devolvemos el estado completo del documento tras la edición.
            escribirPayload(oos, gestor.leerArchivo(peticion.getNombreArchivo()));
        } finally {
            servicioExclusionMutua.liberarAccesoCritico(peticion.getNombreArchivo());
        }
    }

    private void procesarDescarga(ObjectOutputStream oos, PeticionArchivo peticion,
                                  GestorArchivosLocal gestor) throws IOException {
        // La descarga no toma exclusión mutua distribuida: usa el ReadLock local.
        try {
            byte[] datos = gestor.leerArchivo(peticion.getNombreArchivo());
            oos.writeUTF("ÉXITO");
            escribirPayload(oos, datos);
        } catch (FileNotFoundException e) {
            oos.writeUTF("ERROR: " + e.getMessage());
        }
    }

    private String validarChecksum(PeticionArchivo peticion, String hashCalculado, String accion) {
        if (peticion.getChecksum() != null && !peticion.getChecksum().equals(hashCalculado)) {
            return "ERROR: integridad fallida (MD5 no coincide).";
        }
        return "ÉXITO: archivo " + accion + " correctamente.";
    }

    private static byte[] leerPayload(ObjectInputStream ois) throws IOException {
        int longitud = ois.readInt();
        byte[] datos = new byte[longitud];
        ois.readFully(datos);
        return datos;
    }

    private static void escribirPayload(ObjectOutputStream oos, byte[] datos) throws IOException {
        oos.writeInt(datos.length);
        oos.write(datos);
        oos.flush();
    }
}
